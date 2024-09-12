import sys
from pathlib import Path
import pandas as pd
import importlib
import warnings
import re
warnings.simplefilter(action='ignore', category=FutureWarning)

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
    transform_assump = getattr(importlib.import_module(f'tech_transform.{tech}.yellow_{tech}_assump'), f'transform_{tech}_assump')
    transform_model = getattr(importlib.import_module(f'tech_transform.{tech}.yellow_{tech}_model'), f'transform_{tech}_model')

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
            else:
                print(f"Failed to process Pre-Estimation: {file.name}")

    # Ensure df_pre is assigned before processing Assumption files
    if df_pre is not None:
        for file in csv_files:
            if "Assump" in file.stem or "Assumption" in file.stem:
                df_assump = pd.read_csv(file, low_memory=False, header=None)
                df_assump, df_pivot = transform_assump(df_assump, df_pre, file.name, f"yellow_{tech}_assump")
                
                if df_assump is not None:
                    print(f"Processed Assumption: {file.name}")
                else:
                    print(f"Failed to process Assumption: {file.name}")
    else:
        print(f"No Pre-Estimation file was found for {tech}.")

    # Ensure df_assump is assigned before processing Model files
    if df_assump is not None:
        for file in csv_files:
            if "Model" in file.stem or "Model-sheet" in file.stem:
                df_model = pd.read_csv(file, low_memory=False, header=None)
                df_model = transform_model(df_model, file.name, df_assump, f"yellow_{tech}_model")
                
                count_greater_than_6 = df_model['Variable_UID'].apply(
                            lambda x: int(re.search(r'_(\d+)$', x).group(1)) > 6 if re.search(r'_(\d+)$', x) else False).sum()
                print(f"Number of variables with UID ending in a number greater than 6: {count_greater_than_6}")
                
                unique_model_uid_count = df_model['Model_UID'].nunique()
                print(f"Number of unique model_UID values: {unique_model_uid_count}")

                if df_model is not None:
                    print(f"Processed Model: {file.name}")
                    df_model.to_excel(file.stem + '_processed.xlsx', index=False)
                else:
                    print(f"Failed to process Model: {file.name}")



if __name__ == "__main__":
    tech = "wind"  
    process_technology(tech)