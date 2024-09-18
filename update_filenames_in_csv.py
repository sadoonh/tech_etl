import os
import pandas as pd
from load import load_table_filenames_from_db
from sqlalchemy.exc import SQLAlchemyError

def update_filenames_in_csv(table_name: str, csv_path: str):
    """
    Appends distinct filenames from the database to a CSV file if they do not already exist in the CSV.

    Args:
        table_name (str): The name of the table to load filenames from.
        csv_path (str): The path to the CSV file that contains existing filenames.
    """
    try:
        # Step 1: Load distinct filenames from the database
        db_filenames_df = load_table_filenames_from_db(table_name)
        
        if db_filenames_df.empty:
            print(f"No filenames found in table '{table_name}'.")
            return

        # Step 2: Load existing filenames from the CSV file (if the file exists)
        if os.path.exists(csv_path):
            existing_filenames_df = pd.read_csv(csv_path)
            existing_filenames = set(existing_filenames_df['File_Name'].tolist())
        else:
            # If CSV file doesn't exist, start with an empty set
            existing_filenames = set()

        # Step 3: Find new filenames that are in the database but not in the CSV file
        new_filenames = db_filenames_df[~db_filenames_df['File_Name'].isin(existing_filenames)]

        if new_filenames.empty:
            print("No new filenames to append.")
            return

        # Step 4: Append new filenames to the CSV file
        new_filenames.to_csv(csv_path, mode='a', header=not os.path.exists(csv_path), index=False)
        print(f"Appended {len(new_filenames)} new filenames to '{csv_path}'.")

    except SQLAlchemyError as e:
        print(f"Error loading filenames from table: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")