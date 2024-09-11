import pandas as pd
import os
import shutil
import csv
from xls2csv_package import Xlsx2csv

def convert_xlsx_files_in_directory(source_directory, archive_directory, processed_directory):
    # Create the archive and processed directories if they don't exist
    if not os.path.exists(archive_directory):
        os.makedirs(archive_directory)
    
    if not os.path.exists(processed_directory):
        os.makedirs(processed_directory)
    
    # Lists of sheet names for each category
    pre_sheets = ["Pre-Estimation", "Preestimation"]
    assump_sheets = ["Assump", "Assumption"]
    model_sheets = ["Model", "Model-sheet"]
    
    # Get the list of .xlsx files in the source directory
    xlsx_files = [f for f in os.listdir(source_directory) if f.endswith(".xlsx")]
    total_files = len(xlsx_files)
    
    # Loop through all .xlsx files in the source directory
    for index, filename in enumerate(xlsx_files, 1):
        xlsx_file_path = os.path.join(source_directory, filename)
        base_file_name = os.path.splitext(filename)[0]
        sheet_found = False

        # Check for and convert Pre-Estimation sheets
        for sheet_name in pre_sheets:
            if convert_excel_to_csv(xlsx_file_path, sheet_name, processed_directory):
                sheet_found = True

        # Check for and convert Assumption sheets
        for sheet_name in assump_sheets:
            if convert_excel_to_csv(xlsx_file_path, sheet_name, processed_directory):
                sheet_found = True

        # Check for and convert Model sheets
        for sheet_name in model_sheets:
            if convert_excel_to_csv(xlsx_file_path, sheet_name, processed_directory):
                sheet_found = True

        # If any sheet was processed, move the file to the archive directory
        if sheet_found:
            shutil.move(xlsx_file_path, os.path.join(archive_directory, filename))
            print(f"({index}/{total_files}) Processed {filename} and moved to archive.")

def convert_excel_to_csv(xlsx_file_path, sheet_name, processed_directory):
    # Get the base name of the Excel file without the extension
    base_file_name = os.path.splitext(os.path.basename(xlsx_file_path))[0]

    # Concatenate the base file name with the sheet name for the output CSV file
    csv_file_path = os.path.join(processed_directory, f"{base_file_name}_{sheet_name}.csv")

    # Ensure the file exists before processing
    if not os.path.exists(xlsx_file_path):
        return None

    # Create an instance of Xlsx2csv with the provided Excel file
    try:
        xlsx_to_csv_converter = Xlsx2csv(xlsx_file_path, outputencoding="utf-8")
    except Exception as e:
        return None

    # Get the sheet ID from the sheet name
    sheet_id = xlsx_to_csv_converter.getSheetIdByName(sheet_name)

    # If the sheet is not found, return None
    if not sheet_id:
        return None

    try:
        # Convert the specified sheet to a CSV file
        xlsx_to_csv_converter.convert(csv_file_path, sheetid=sheet_id)
    except Exception as e:
        return None

    return csv_file_path

def move_csv_files_to_final_destination(processed_directory, final_destination):
    # Subfolders inside the final destination
    solar_dir = os.path.join(final_destination, 'solar')
    battery_dir = os.path.join(final_destination, 'battery')
    wind_dir = os.path.join(final_destination, 'wind')
    tline_dir = os.path.join(final_destination, 'tline')

    # List all CSV files in the processed directory
    csv_files = [f for f in os.listdir(processed_directory) if f.endswith(".csv")]

    # Define the patterns for each type (searching in file contents)
    solar_patterns = ["S_ER_1", "S_A_1", "S_M_1"]
    battery_patterns = ["B_ER_1", "B_A_1", "B_M_1"]
    wind_patterns = ["W_ER_1", "W_A_1", "W_M_1"]
    tline_patterns = ["T_ER_1", "T_A_1", "T_M_1"]

    # Function to check if any of the patterns are found in the CSV file contents
    def file_contains_any_pattern(file_path, patterns):
        try:
            with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                for row in reader:
                    # Check if any pattern exists in any of the row's cells
                    if any(pattern in cell for cell in row for pattern in patterns):
                        return True
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
        return False

    # Iterate through each CSV file and move it to the correct folder
    for csv_file in csv_files:
        csv_file_path = os.path.join(processed_directory, csv_file)

        # Check if the CSV file content matches any of the solar patterns
        if file_contains_any_pattern(csv_file_path, solar_patterns):
            shutil.move(csv_file_path, os.path.join(solar_dir, csv_file))
            print(f"Moved {csv_file} to solar folder.")
        
        # Check if the CSV file content matches any of the battery patterns
        elif file_contains_any_pattern(csv_file_path, battery_patterns):
            shutil.move(csv_file_path, os.path.join(battery_dir, csv_file))
            print(f"Moved {csv_file} to battery folder.")
        
        # Check if the CSV file content matches any of the wind patterns
        elif file_contains_any_pattern(csv_file_path, wind_patterns):
            shutil.move(csv_file_path, os.path.join(wind_dir, csv_file))
            print(f"Moved {csv_file} to wind folder.")
        
        # Check if the CSV file content matches any of the tline patterns
        elif file_contains_any_pattern(csv_file_path, tline_patterns):
            shutil.move(csv_file_path, os.path.join(tline_dir, csv_file))
            print(f"Moved {csv_file} to tline folder.")
        
        else:
            print(f"File {csv_file} does not match any pattern.")