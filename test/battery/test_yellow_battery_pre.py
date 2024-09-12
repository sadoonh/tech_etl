import sys
from pathlib import Path
import pandas as pd
import importlib

# Add the parent directory of 'tech_transform' to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from extract import convert_xlsx_files_in_directory

def process_technology(tech: str):
    """
    Function to process files for the given technology in the current working directory.
    
    Args:
        tech (str): The technology type ('battery', 'solar', 'wind', 'tline').
    """
    folder = Path.cwd()  # Always use the current working directory

    # Dynamically import the transformation functions based on the tech parameter
    transform_pre = getattr(importlib.import_module(f'tech_transform.{tech}.yellow_{tech}_pre'), f'transform_{tech}_pre')

    # Find all .xlsx files in the folder
    xlsx_files = list(folder.glob("*.xlsx"))

    # Print the list of found files and convert them
    print(f"Excel files found for {tech}:")
    for file in xlsx_files:
        convert_xlsx_files_in_directory(folder, folder, folder)

    # Find all .csv files in the folder after conversion
    csv_files = list(folder.glob("*.csv"))

    # Initialize df_pre to None to ensure it's available before using it
    df_pre = None

    # Iterate over the .csv files to process Pre-Estimation files first
    for file in csv_files:
        if "Pre-Estimation" in file.stem or "Preestimation" in file.stem:
            df_pre = pd.read_csv(file, low_memory=False, header=None)
            df_pre, _ = transform_pre(df_pre, file.name, f"yellow_{tech}_pre")
            if df_pre is not None:
                print(f"Processed Pre-Estimation: {file.name}")
                df_pre.to_excel(file.stem + '_processed.xlsx', index=False)
            else:
                print(f"Failed to process Pre-Estimation: {file.name}")



if __name__ == "__main__":
    tech = "battery"  
    process_technology(tech)