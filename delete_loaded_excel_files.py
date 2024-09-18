import os
import pandas as pd

def delete_loaded_excel_files():
    """
    Checks if .xlsx files in the raw_excel directory exist in any of the CSV files
    in the loaded_files directory. If found, deletes the .xlsx file from raw_excel and
    prints the file name and the CSV file where it was found.
    """
    # Define the directories
    project_root = os.path.dirname(os.path.abspath(__file__))
    raw_excel_dir = os.path.join(project_root, 'raw_excel')
    loaded_files_dir = os.path.join(project_root, 'loaded_files')

    # Dictionary to hold the loaded file names and the CSV file they were found in
    loaded_file_names = {}

    # Iterate over all CSV files in loaded_files directory
    for csv_file in os.listdir(loaded_files_dir):
        if csv_file.endswith('.csv'):
            csv_path = os.path.join(loaded_files_dir, csv_file)
            try:
                # Load the CSV as a DataFrame
                df = pd.read_csv(csv_path)

                # Add all file names to the dictionary, with the CSV file they were found in
                for file_name in df['File_Name'].astype(str).tolist():
                    loaded_file_names[file_name] = csv_file
            except Exception as e:
                print(f"Error reading {csv_file}: {e}")

    # Iterate over all .xlsx files in raw_excel directory
    for excel_file in os.listdir(raw_excel_dir):
        if excel_file.endswith('.xlsx'):
            file_name_without_ext = os.path.splitext(excel_file)[0] + '.xlsx'

            # Check if the .xlsx file is found in any of the CSV files
            if file_name_without_ext in loaded_file_names:
                # Get the CSV file where the .xlsx file was found
                found_in_csv = loaded_file_names[file_name_without_ext]

                # Delete the .xlsx file from raw_excel
                file_path = os.path.join(raw_excel_dir, excel_file)
                try:
                    os.remove(file_path)
                    print(f"Deleted {excel_file} found in {found_in_csv}")
                except Exception as e:
                    print(f"Error deleting {excel_file}: {e}")

