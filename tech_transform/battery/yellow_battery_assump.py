import pandas as pd
import os
import hashlib
from datetime import datetime
import numpy as np

# Main Transformation Function
def transform_battery_assump(df, df_pre, file_name, table_name):
    try:
        # Apply initial transformations
        df = battery_BA11(df, 'B_A_1_1')
        df = df.dropna(axis=1, how='all')
        df = battery_estimator_project_manager_transformation(df)
        df = battery_assumption_transformation(df, file_name)

        # If pre-estimation data is available, correlate it with assumption data
        if df_pre is not None:
            pre_uid_letter_dict = battery_correlate_preestimation_uid_with_letter(df_pre)
            assump_uid_letter_dict = battery_correlate_assumption_uid_with_letter(df)
            transformed_df = map_assumption_to_preestimation(df, df_pre, assump_uid_letter_dict, pre_uid_letter_dict)
        else:
            df['Preestimation_UID'] = np.nan
            transformed_df = df
        
        # Pivot and flatten the transformed DataFrame
        df_pivot = assumption_pivot_and_flatten(transformed_df)

        # Load the pivoted data to the database
        # load_df_to_db(df_pivot, table_name)

        # Return both the transformed and pivoted DataFrames
        return transformed_df, df_pivot
    except Exception as e:
        print(f"Error processing {file_name}: {e}")
        return None, None
   
def battery_assumption_transformation(df, file_name):
    """
    Perform all transformations on the DataFrame for a given Excel file.
    - Column UID Transformation
    - Main Transformation (including NaN handling, renaming, melting, and time assignments)
    - Filename and Hash Transformation
    """
    
    def find_column_with_exact_string(df, search_string):
            for column in df.columns:
                if df[column].eq(search_string).any():
                    return column, df[column].count()

    column_name, column_length = find_column_with_exact_string(df, 'B_A_1')
    df = df.iloc[:column_length,:]
    
    # Drop columns with >35% NaN values
    df = df.drop(columns=df.columns[df.isna().sum() / len(df) * 100 > 35])
    
    # Column UID Transformation
    df = column_uid_transformation(df)
    
    # Rename Columns
    new_columns = ['Variable_UID'] + ['Variable_Name'] +  list(df.columns[2:])
    df.columns = new_columns

    # Handle duplicate columns 
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique(): 
        dup_indices = cols[cols == dup].index.values
        for i, index in enumerate(dup_indices):
            if i != 0:
                cols[index] = f"{dup}_{i}"
    df.columns = cols

    # Melting the DataFrame
    df = df.melt(id_vars=['Variable_UID', 'Variable_Name'], var_name='Column_UID', value_name='Variable_Value')

    # Time assignments
    df['Upload_Time'] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M'))
    df['Rotation_Date'] = df['Upload_Time'] + pd.DateOffset(years=1)


    # Filename and Hash Transformation
    desired_string = os.path.splitext(os.path.basename(file_name))[0]
    hash_string = hashlib.sha256(desired_string.encode('utf-8')).hexdigest()

    # Adding Filename and Hash to DataFrame
    df['File_Name'] = desired_string
    df['File_Hash'] = hash_string

    # Create Assumption_UID with additional context 
    def generate_uid(value, context):
        combined_value = f"{value}_{context}"
        return hashlib.md5(combined_value.encode('utf-8')).hexdigest()[:32]

    # Use the file name as context
    context = desired_string
    df['Assumption_UID'] = df['Column_UID'].apply(lambda x: generate_uid(x, context))
    
    # Reorder columns
    new_column_order = ['File_Name', 'File_Hash', 'Column_UID', 'Assumption_UID', 'Variable_UID', 'Variable_Name', 'Variable_Value', 'Upload_Time', 'Rotation_Date']
    df = df.reindex(columns=new_column_order)

    return df

# Battery Estimator Project Manager Transformation
def battery_estimator_project_manager_transformation(df):
    # Function to find row and column index with exact string
    def find_row_with_exact_string(df, string):
        row_index, col_index = None, None
        for col in df.columns:
            mask = df[col].astype(str).str.contains(string, case=False, na=False)
            if mask.any():
                row_index = mask[mask].index[0]
                col_index = df.columns.get_loc(col)
                break
        return row_index, col_index
    
    # Grab rows above that row 
    row_index, col_index = find_row_with_exact_string(df, 'Estimate Date')
    
    # Make a copy of the original DataFrame to avoid modifying it directly
    adjusted_df = df.copy()
    
    # Grab the two rows of interest
    rows_of_interest = adjusted_df.iloc[row_index-2:row_index, col_index]
    
    # Fill all rows within the specified range with the values from the rows of interest
    for col in range(col_index+1, len(adjusted_df.columns)):
        adjusted_df.iloc[1:3, col] = rows_of_interest.values
    
    # Replace the two rows above the string with 'Estimator' and 'Project Manager'
    adjusted_df.iloc[row_index-2, col_index] = 'Estimator'
    adjusted_df.iloc[row_index-1, col_index] = 'Project Manager'
    
    return adjusted_df

# Helper Functions
def battery_BA11(df, new_row_label):
    """
    Processes the DataFrame to:
    1. Add a new dynamic row with a specified label.
    2. Replace 'B_A_1' with 'B_A_1_1' in the DataFrame.
    
    Parameters:
    - df: DataFrame to process.
    - new_row_label: The label for the new dynamic row.
    
    Returns:
    - df: The processed DataFrame.
    """
    
    # Add a new dynamic row
    new_row = {col: new_row_label if col == df.columns[0] else df[col][0] for col in df.columns}
    new_row_df = pd.DataFrame([new_row])
    df = pd.concat([new_row_df, df], ignore_index=True)
    
    # Check if "S_A_1" exists in the DataFrame and replace it with "S_A_1_1"
    for column in df.columns:
        if df[column].eq("B_A_1").any():
            index = df[df[column] == "B_A_1"].index[0]
            df.loc[index, column] = "B_A_1_1"
    
    df.iloc[0, 5] = ''
    
    return df

def column_uid_transformation(df): 
    # Columns needed: Project Name, Project P-Num, Proposal ID, Scenario ID
    column_needed = df.iloc[:, 1].to_list()
    
    # Initialize indices with None
    name_row_idx = None
    project_row_idx = None
    proposal_row_idx = None
    scenario_row_idx = None
    
    try:
        name_row_idx = column_needed.index('Project Name')
    except ValueError:
        pass

    try:
        project_row_idx = column_needed.index('Project Number')
    except ValueError:
        pass
    
    try:
        proposal_row_idx = column_needed.index('Proposal ID')
    except ValueError:
        pass

    try:
        scenario_row_idx = column_needed.index('Scenario ID')
    except ValueError:
        pass

    # Create a new list to hold column names
    new_columns = []

    # Iterate over the columns in the dataframe
    for col_idx, col_name in enumerate(df.columns):
        # Get the corresponding values from the specific rows
        key_val = ''
        if name_row_idx is not None:
            key_val = df.iloc[name_row_idx, col_idx]

        project_val = ''
        if project_row_idx is not None:
            project_val = df.iloc[project_row_idx, col_idx]
        
        proposal_val = ''
        if proposal_row_idx is not None:
            proposal_val = df.iloc[proposal_row_idx, col_idx]
        
        scenario_val = ''
        if scenario_row_idx is not None:
            scenario_val = df.iloc[scenario_row_idx, col_idx]

        # Create the new column name by concatenating the values
        new_col_name = f'{key_val} {proposal_val} {project_val} {scenario_val}'
        new_columns.append(new_col_name)

    # Update the dataframe with the new column names
    df.columns = new_columns

    return df

def get_object_type(obj):
    if isinstance(obj, int):
        return 'int'
    elif isinstance(obj, str) and obj:
        return 'str'
    elif obj == '' or obj is None:
        return None
    elif isinstance(obj, float):
        return 'numeric'
    elif isinstance(obj, datetime):
        return 'timestamp'
    else:
        return 'unknown'
    
def assumption_pivot_and_flatten(df):
    """
    Pivot a DataFrame and flatten the resulting multi-index columns.

    Args:
        df (pandas.DataFrame): The input DataFrame.

    Returns:
        pandas.DataFrame: The pivoted and flattened DataFrame.
    """
    # Check if the DataFrame is not None and not empty
    if df is not None and not df.empty:
        index_cols = ['File_Name', 'File_Hash', 'Column_UID', 'Upload_Time', 'Rotation_Date', 'Preestimation_UID', 'Assumption_UID']
        pivot_col = 'Variable_UID'
        value_col = 'Variable_Value'

        df = df.dropna(subset=pivot_col)

        df = df.drop_duplicates(subset=['File_Hash', 'Column_UID', 'Variable_UID'], keep='last')

        pivot_df = df.pivot(index=index_cols, columns=pivot_col, values=value_col)
        pivot_df.reset_index(inplace=True)
        
        pivot_df.columns.name = None
   
        return pivot_df
    else:
        return None
    
def battery_correlate_preestimation_uid_with_letter(df):
    """
    Create a dictionary linking each Preestimation_UID to its corresponding letter
    based on rows where Variable_UID is 'B_ER_11'.
    
    Args:
    df (pd.DataFrame): The dataframe containing the data.
    
    Returns:
    dict: A dictionary linking Preestimation_UID to the corresponding letter.
    """
    # Filter rows where Variable_UID is 'B_ER_11'
    filtered_df = df[df['Variable_UID'] == 'B_ER_11']
    
    # Create the dictionary
    uid_letter_dict = dict(zip(filtered_df['Preestimation_UID'], filtered_df['Variable_Value']))
    
    return uid_letter_dict

def battery_correlate_assumption_uid_with_letter(df):
    """
    Create a dictionary linking each Assumption_UID to its corresponding letter
    based on rows where Variable_UID is 'B_A_1_1'.
    
    Args:
    df (pd.DataFrame): The dataframe containing the data.
    
    Returns:
    dict: A dictionary linking Assumption_UID to the corresponding letter.
    """
    # Filter rows where Variable_UID is 'B_A_1_1'
    filtered_df = df[df['Variable_UID'] == 'B_A_1_1']
    
    # Create the dictionary
    uid_letter_dict = dict(zip(filtered_df['Assumption_UID'], filtered_df['Variable_Value']))
    
    return uid_letter_dict

def map_assumption_to_preestimation(df_assumption, df_preestimation, assump_uid_letter_dict, pre_uid_letter_dict):
    """
    Add a new column to the assumption DataFrame mapping Assumption_UID to Preestimation_UID
    based on the common letters and ensuring they are from the same File_Name.
    
    Args:
    df_assumption (pd.DataFrame): The assumption dataframe.
    df_preestimation (pd.DataFrame): The preestimation dataframe.
    assump_uid_letter_dict (dict): Dictionary linking Assumption_UID to letters.
    pre_uid_letter_dict (dict): Dictionary linking Preestimation_UID to letters.
    
    Returns:
    pd.DataFrame: The updated assumption dataframe with the new mapping column.
    """
    # Create a dictionary for File_Name based mappings
    file_name_to_pre_uid_dict = {}
    for pre_uid, letter in pre_uid_letter_dict.items():
        file_name = df_preestimation[df_preestimation['Preestimation_UID'] == pre_uid]['File_Name'].values[0]
        if file_name not in file_name_to_pre_uid_dict:
            file_name_to_pre_uid_dict[file_name] = {}
        file_name_to_pre_uid_dict[file_name][letter] = pre_uid
    
    # Function to map Assumption_UID to Preestimation_UID
    def map_uid(assumption_uid, file_name):
        letter = assump_uid_letter_dict.get(assumption_uid, None)
        if letter and file_name in file_name_to_pre_uid_dict:
            return file_name_to_pre_uid_dict[file_name].get(letter, None)
        return None
    
    # Apply the mapping function to create a new column
    df_assumption['Preestimation_UID'] = df_assumption.apply(lambda row: map_uid(row['Assumption_UID'], row['File_Name']), axis=1)
    
    return df_assumption
