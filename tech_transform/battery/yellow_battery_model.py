import pandas as pd
import os
import hashlib
from datetime import datetime
import uuid
from load import load_df_to_db

# Main Transformation Function
def transform_battery_model(df, file_path, df_assump, table_name):
    try:
        # Apply solar model transformation
        transformed_df = battery_model_transformation(df, file_path, 'B_M_1')

        # Correlate UID letters between model and assumption
        assump_uid_letter_dict = battery_correlate_assumption_uid_with_letter_for_model(df_assump)
        model_uid_letter_dict = battery_correlate_model_uid_with_letter(transformed_df)

        # Map model to assumption using the UID letters
        transformed_df = map_model_to_assumption(transformed_df, df_assump, model_uid_letter_dict, assump_uid_letter_dict)
        
        # load the transformed DataFrame to a database 
        load_df_to_db(transformed_df, table_name)

        # Define the relative path for the log CSV file
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))  # This gets the project root
        log_csv_path = os.path.join(project_root, 'loaded_files', 'yellow_battery_model_files.csv')
        
        file_name = transformed_df['File_Name'].iloc[0]
        
        # Log the successfully processed file name to the CSV file
        log_processed_filename(file_name, log_csv_path)

        # Return the transformed DataFrame
        return transformed_df

    except ValueError as e:
        print(f"Value error processing {file_path}: {e}")
        return None
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None

def battery_model_transformation(df, file_name, string):
    # Find the index of the column that matches the exact string provided.
    column_index = find_column_index_with_exact_string(df, string)
    
    # Slice the DataFrame from the found column index onwards.
    df = df.iloc[:, column_index:]
    
    # Drop the first 13 columns after the matched column, as they are not needed.
    columns_to_drop = df.columns[1:14]
    df = df.drop(columns=columns_to_drop).T.reset_index(drop=True)
    
    # Set the first row as the DataFrame header.
    df.columns = df.iloc[0]
    df = df.iloc[1:, :].reset_index(drop=True)
    
    # Drop columns where all values are NA.
    df = df.dropna(axis=1, how='all')
    
    # Calculate the percentage of missing values per row.
    missing_percentage = df.isnull().mean(axis=1)
    
    # Drop rows where more than 90% of values are missing.
    rows_to_drop = df[missing_percentage > 0.9]
    df = df.drop(rows_to_drop.index).reset_index(drop=True)
    
    # Drop columns with any NA values.
    df = df[df.columns.dropna()]
    
    # Identify rows that contain the specified string and are not NA.
    key_rows = df[string].dropna().index
    
    # Split the DataFrame into smaller DataFrames based on the key rows identified.
    df_list = []
    for i in range(len(key_rows)):
        if i != len(key_rows) - 1:
            df_small = df.iloc[key_rows[i]:key_rows[i+1]]
        else:
            df_small = df.iloc[key_rows[i]:(key_rows[i]+6)]
        df_small = df_small.copy()
          # Generate a unique Model UID using UUID
        model_uid = str(uuid.uuid4())
        df_small['Model_UID'] = model_uid
        df_list.append(df_small)
    
    # Apply column transformations to each smaller DataFrame and store them in global variables.
    for i, df_small in enumerate(df_list, start=1):
        globals()[f'df{i}'] = df_small
    
    # Apply a transformation to each DataFrame in the list and concatenate the results.
    df_transformed_list = []
    for df_small in df_list:
        model_uid = df_small['Model_UID'].iloc[0]
        df_small = df_small.drop(columns=['Model_UID'])
        df_transformed = column_transform_test(df_small)
        df_transformed['Model_UID'] = model_uid  # Preserving Model_UID after transformation
        df_transformed_list.append(df_transformed)
    
    df = pd.concat(df_transformed_list, ignore_index=True)
    
    # Reshape the DataFrame with melt to organize variable IDs and values.
    df = df.melt(id_vars=['Model_UID'], var_name='Variable_UID', value_name='Variable_Value')
    
    # Extract the file name without extension and generate a SHA256 hash.
    desired_string = os.path.splitext(os.path.basename(file_name))[0]
    hash_string = hashlib.sha256(desired_string.encode('utf-8')).hexdigest()
    
    # Add columns for file name, hash, and upload time.
    df = df.assign(File_Name=desired_string, File_Hash=hash_string)
    df['Upload_Time'] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M'))
    
    # Calculate the rotation date, one year from the upload time.
    df['Rotation_Date'] = df['Upload_Time'] + pd.DateOffset(years=1)
    
    # Reorder the columns in the desired sequence.
    new_column_order = ['File_Name', 'File_Hash', 'Model_UID', 'Variable_UID', 'Variable_Value', 'Upload_Time', 'Rotation_Date']
    df = df[new_column_order]
    
    return df

# helper functions
def find_column_index_with_exact_string(df, search_string):
        for column in df.columns:
            if df[column].eq(search_string).any():
                return df.columns.get_loc(column)

def column_transform_test(df):
  # Initialize an empty list to store new_col_name values
    new_col_names_list = []

    # Extract the values after the second underscore for all columns
    values_after_second_underscore = df.columns.str.split('_').str[2]

    for col_index, col_name in enumerate(df.columns):
        for row_index in range(len(df)):
            # Create the new column name using the extracted value
            new_col_name = f"{col_name[0:3]}_{values_after_second_underscore[col_index]}_{row_index + 1}"
            new_col_names_list.append(new_col_name)

    # Use set_axis to rename the columns
    transformed_df = df.unstack().reset_index(drop=True).to_frame().T.set_axis(new_col_names_list, axis=1)
    return transformed_df

def battery_correlate_assumption_uid_with_letter_for_model(df):
    """
    Create a dictionary linking each Assumption_UID to its corresponding letter 
    based on rows where Variable_UID is 'B_A_6'.
    
    Args:
    df (pd.DataFrame): The dataframe containing the data.
    
    Returns:
    dict: A dictionary linking Assumption_UID to the corresponding letter.
    """
    # Filter rows where Variable_UID is 'B_A_6'
    filtered_df = df[df['Variable_UID'] == 'B_A_6']
    
    # Create the dictionary
    uid_letter_dict = dict(zip(filtered_df['Assumption_UID'], filtered_df['Variable_Value']))
    
    return uid_letter_dict

def battery_correlate_model_uid_with_letter(df):
    """
    Create a dictionary linking each Model_UID to its corresponding letter
    based on rows where Variable_UID is 'B_M_2_4'.
    
    Args:
    df (pd.DataFrame): The dataframe containing the data.
    
    Returns:
    dict: A dictionary linking Model_UID to the corresponding letter.
    """
    # Filter rows where Variable_UID is 'B_M_2_4'
    filtered_df = df[df['Variable_UID'] == 'B_M_2_4']
    
    # Create the dictionary
    uid_letter_dict = dict(zip(filtered_df['Model_UID'], filtered_df['Variable_Value']))
    
    return uid_letter_dict

def map_model_to_assumption(df_model, df_assumption, model_uid_letter_dict, assump_uid_letter_dict):
    """
    Add a new column to the assumption DataFrame mapping Assumption_UID to Preestimation_UID
    based on the common letters and ensuring they are from the same File_Name.
    
    Args:
    df_assumption (pd.DataFrame): The assumption dataframe.
    df_model (pd.DataFrame): The preestimation dataframe.
    assump_uid_letter_dict (dict): Dictionary linking Assumption_UID to letters.
    model_uid_letter_dict (dict): Dictionary linking Model_UID to letters.
    
    Returns:
    pd.DataFrame: The updated assumption dataframe with the new mapping column.
    """
    # Create a dictionary for File_Name based mappings
    file_name_to_assump_uid_dict = {}
    for assump_uid, letter in assump_uid_letter_dict.items():
        file_name = df_assumption[df_assumption['Assumption_UID'] == assump_uid]['File_Name'].values[0]
        if file_name not in file_name_to_assump_uid_dict:
            file_name_to_assump_uid_dict[file_name] = {}
        file_name_to_assump_uid_dict[file_name][letter] = assump_uid
    
    # Function to map Model_UID to Assumption_UID
    def map_uid(model_uid, file_name):
        letter = model_uid_letter_dict.get(model_uid, None)
        if letter and file_name in file_name_to_assump_uid_dict:
            return file_name_to_assump_uid_dict[file_name].get(letter, None)
        return None
    
    # Apply the mapping function to create a new column
    df_model['Assumption_UID'] = df_model.apply(lambda row: map_uid(row['Model_UID'], row['File_Name']), axis=1)
    
    return df_model

def log_processed_filename(file_name, log_csv_path):
    """
    Logs the processed file name to a CSV file, adding '.xlsx' to the filename.

    Args:
        file_name (str): The file name to log.
        log_csv_path (str): The path to the CSV file where the file name will be logged.
    """
    try:
        # Step 1: Strip the extension (e.g., .csv)
        base_file_name = os.path.splitext(file_name)[0]

        # Step 2: Remove '_Assump' or '_Assumption' from the base file name
        base_file_name = base_file_name.replace('_Model', '').replace('_Model-sheet', '')

        # Step 3: Add '.xlsx' to the cleaned file name
        processed_file_name = base_file_name + '.xlsx'

        # Check if the log CSV file already exists
        if os.path.exists(log_csv_path):
            # Load the existing CSV file
            log_df = pd.read_csv(log_csv_path)
            # If the file name is already logged, do nothing
            if processed_file_name in log_df['File_Name'].values:
                print(f"{processed_file_name} is already logged.")
                return
        else:
            # If the log file doesn't exist, create a new DataFrame
            log_df = pd.DataFrame(columns=['File_Name'])

        # Create a new DataFrame with the new file name
        new_file_df = pd.DataFrame({'File_Name': [processed_file_name]})

        # Append the new file name using pd.concat
        log_df = pd.concat([log_df, new_file_df], ignore_index=True)

        # Save the updated DataFrame back to the CSV file
        log_df.to_csv(log_csv_path, index=False)
        print(f"Appended {processed_file_name} to log CSV.")

    except Exception as e:
        print(f"Error logging file name {file_name}: {e}")