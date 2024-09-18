import os
import shutil

def archive_csv_files():
    """
    Moves all CSV files from tech_csv's subfolders (solar, battery, wind, tline)
    to a single archive_csv folder in the Lambda project root, without preserving subfolder structure.
    """
    # Get the directory of the current file (archive_csv_files.py) and set project root (Lambda)
    project_root = os.path.dirname(os.path.abspath(__file__))
    tech_csv_dir = os.path.join(project_root, 'tech_csv')
    archive_csv_dir = os.path.join(project_root, 'archive_csv')

    try:
        # Create archive folder if it doesn't exist
        if not os.path.exists(archive_csv_dir):
            os.makedirs(archive_csv_dir)

        # Walk through all subfolders in tech_csv and find CSV files
        for root, dirs, files in os.walk(tech_csv_dir):
            for file_name in files:
                if file_name.endswith('.csv'):
                    source_file = os.path.join(root, file_name)
                    destination_file = os.path.join(archive_csv_dir, file_name)

                    # Handle cases where files with the same name might exist in different subfolders
                    if os.path.exists(destination_file):
                        base, extension = os.path.splitext(file_name)
                        new_file_name = f"{base}_{len(os.listdir(archive_csv_dir))}{extension}"
                        destination_file = os.path.join(archive_csv_dir, new_file_name)

                    # Move the CSV file to archive_csv
                    shutil.move(source_file, destination_file)

        print("All CSV files have been archived.")
    except Exception as e:
        print(f"Error occurred during archiving: {e}")
