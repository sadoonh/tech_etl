import pandas as pd
import hashlib
import os
from datetime import datetime

# Main Transformation Function
def transform_battery_pre(df, file_name, table_name):
    try:
        # Drop columns with all NaN values
        df = df.dropna(axis=1, how='all')
        
        # Apply solar-specific transformations
        transformed_df = battery_preestimation_transformation(df, file_name)
        
        # Pivot and flatten the transformed dataframe
        df_pivot = pre_pivot_and_flatten(transformed_df)
        
        # Load the pivoted data to the database if table_name is provided 
        # load_df_to_db(df_pivot, table_name)
        
        # Return the transformed and pivoted dataframes
        return transformed_df, df_pivot
    except Exception as e:
        # Log a more detailed error message
        print(f"Error processing {file_name}: {e}")
        return None, None

def battery_preestimation_transformation(df, file_name):
    """
    Process the dataframe by applying various transformations.

    Args:
    df (pd.DataFrame): The dataframe to process.
    file_name (str): The name of the file for data type determination and hash calculation.
    data_types (dict): Not directly used in this snippet but can be integrated as needed.

    Returns:
    pd.DataFrame: The processed dataframe.
    """
    # Search for 'UID' and determine starting row and column
    row_index, column_index = None, None
    for col in df.columns:
        mask = df[col].astype(str).str.contains('UID', case=False, na=False)
        if mask.any():
            row_index = mask[mask].index[0]
            column_index = df.columns.get_loc(col)
            break
    
    df = df.iloc[(row_index + 1):, (column_index + 1):]
    df = df.drop(columns=df.columns[df.isna().mean() > 0.80])    
    # Drop column with 'Req'
    for col in df.columns:
        if df[col].astype(str).str.contains('Req', case=False, na=False).any():
            df = df.drop(columns=col)
            break
    
    # Column UID Transformation
    df = column_uid_transformation(df)
    
    # Rename columns and melt
    df = df.rename(columns={df.columns[0]: 'Variable_UID', df.columns[1]: 'Variable_Name'})

    # Handle duplicate columns
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique(): 
        dup_indices = cols[cols == dup].index.values
        for i, index in enumerate(dup_indices):
            if i != 0:
                cols[index] = f"{dup}_{i}"
    df.columns = cols
    
    df = df.melt(id_vars=['Variable_UID', 'Variable_Name'], var_name='Column_UID', value_name='Variable_Value')
    
    # Add timestamps and rotation date
    df['Upload_Time'] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M'))
    df['Rotation_Date'] = df['Upload_Time'] + pd.DateOffset(years=1)
    
    
    # Compute file hash
    desired_string = os.path.splitext(os.path.basename(file_name))[0]
    hash_string = hashlib.sha256(desired_string.encode('utf-8')).hexdigest()
    
    df['File_Name'] = desired_string
    df['File_Hash'] = hash_string
    
    # Create Preestimation_UID with additional context
    def generate_uid(value, context):
        combined_value = f"{value}_{context}"
        return hashlib.md5(combined_value.encode('utf-8')).hexdigest()[:32]

    # Use the file name as context
    context = desired_string
    df['Preestimation_UID'] = df['Column_UID'].apply(lambda x: generate_uid(x, context))
    
    # Reorder columns
    new_column_order = ['File_Name', 'File_Hash', 'Column_UID', 'Preestimation_UID', 'Variable_UID', 'Variable_Name', 'Variable_Value', 'Upload_Time', 'Rotation_Date']
    df = df.reindex(columns=new_column_order)
    
    # Drop nan values in Variable_UID column
    df = df.dropna(subset=['Variable_UID'])

    return df

# Helper functions
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
    
def pre_pivot_and_flatten(df):
    """
    Pivot a DataFrame and flatten the resulting multi-index columns.

    Args:
        df (pandas.DataFrame): The input DataFrame.

    Returns:
        pandas.DataFrame: The pivoted and flattened DataFrame.
    """
    # Check if the DataFrame is not None and not empty
    if df is not None and not df.empty:
        index_cols = ['File_Name', 'File_Hash', 'Column_UID', 'Upload_Time', 'Rotation_Date', 'Preestimation_UID']
        pivot_col = 'Variable_UID'
        value_col = 'Variable_Value'

        # Ensure there are no missing values in the pivot column
        df = df.dropna(subset=[pivot_col])

        # Remove duplicate entries
        df = df.drop_duplicates(subset=['File_Hash', 'Column_UID', pivot_col], keep='last')

        # Perform the pivot operation
        pivot_df = df.pivot(index=index_cols, columns=pivot_col, values=value_col)
        
        # Reset the index to flatten the DataFrame
        pivot_df.reset_index(inplace=True)

        pivot_df.columns.name = None

         
        return pivot_df
    else:
        return None