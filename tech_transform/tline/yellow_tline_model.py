import pandas as pd
import os
import hashlib
from datetime import datetime
import uuid

# Tline Model Transformation 
def transform_tline_model(df, file_name, string):
    # Select columns starting from the first column to the column where 'Project Name:' is found, inclusive.
    selected_columns = [0] + list(range(find_column_index_with_exact_string(df, 'Project Name:'), df.shape[1]))
    
    # Transform the DataFrame by selecting the specified columns and then transpose it.
    df = df.iloc[:, selected_columns].T.reset_index(drop=True)
    
    # Set the first row as column headers.
    df.columns = df.iloc[0]
    # Remove the first row after setting it as header and reset index.
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

    # Identify the rows that contain the specified string and are not NA, for segmentation.
    key_rows = df[string].dropna().index
    # Split the DataFrame based on key rows into smaller DataFrames.
    df_list = []
    for i in range(len(key_rows)):
        if i != len(key_rows) - 1:
            df_small = df.iloc[key_rows[i]:key_rows[i+1]]
        else:
            df_small = df.iloc[key_rows[i]:]
        df_list.append(df_small)

    # Optionally, store each smaller DataFrame in the global namespace. (Consider alternatives for production code.)
    for i, df_small in enumerate(df_list, start=1):
        globals()[f'df{i}'] = df_small

    # Apply a transformation function to each small DataFrame, then concatenate them.
    df = [column_transform(df) for df in df_list]
    df = pd.concat(df)

    # Reshape the DataFrame to organize variable IDs and values.
    df = df.melt(var_name='Variable_UID', value_name='Variable_Value')

    # Extract the base file name, hash it, and add as new columns.
    desired_string = os.path.splitext(os.path.basename(file_name))[0]
    hash_string = hashlib.sha256(desired_string.encode('utf-8')).hexdigest()
    df = df.assign(File_Name=desired_string, File_Hash=hash_string)

    # Add a timestamp for the upload and calculate a rotation date one year ahead.
    df['Upload_Time'] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M'))
    df['Rotation_Date'] = df['Upload_Time'] + pd.DateOffset(years=1)

    # Reorder the columns to the specified order.
    new_column_order = ['File_Name', 'File_Hash', 'Variable_UID', 'Variable_Value', 'Upload_Time', 'Rotation_Date']
    df = df[new_column_order]

    return df

# helper functions
def find_column_index_with_exact_string(df, search_string):
        for column in df.columns:
            if df[column].eq(search_string).any():
                return df.columns.get_loc(column)

def column_transform(df):
    # Initialize an empty list to store new_col_name values
    new_col_names_list = []

    for col_index, col_name in enumerate(df.columns):
        for row_index in range(len(df)):
            new_col_name = f"{col_name[0:3]}_{col_index + 1}_{row_index + 1}"
            new_col_names_list.append(new_col_name)

    # Use set_axis to rename the columns
    transformed_df = df.unstack().reset_index(drop=True).to_frame().T.set_axis(new_col_names_list, axis=1)
    return transformed_df