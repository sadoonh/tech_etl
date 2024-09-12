import os
import pandas as pd

# Import the specific transformations from each subdirectory
from tech_transform.solar.yellow_solar_pre import transform_solar_pre
from tech_transform.solar.yellow_solar_assump import transform_solar_assump
from tech_transform.solar.yellow_solar_model import transform_solar_model

from tech_transform.battery.yellow_battery_pre import transform_battery_pre
from tech_transform.battery.yellow_battery_assump import transform_battery_assump
from tech_transform.battery.yellow_battery_model import transform_battery_model

from tech_transform.wind.yellow_wind_pre import transform_wind_pre
from tech_transform.wind.yellow_wind_assump import transform_wind_assump
from tech_transform.wind.yellow_wind_model import transform_wind_model

from tech_transform.tline.yellow_tline_pre import transform_tline_pre
from tech_transform.tline.yellow_tline_assump import transform_tline_assump
from tech_transform.tline.yellow_tline_model import transform_tline_model

# Main function to process CSVs in each directory
def process_csv_files_in_order(directory, tech_type):
    pre_file = None
    assump_file = None
    model_file = None

    # Find the relevant CSV files in the directory
    for csv_file in os.listdir(directory):
        if 'Pre' in csv_file:
            pre_file = csv_file
        elif 'Assump' in csv_file:
            assump_file = csv_file
        elif 'Model' in csv_file:
            model_file = csv_file

    if not pre_file or not assump_file or not model_file:
        print(f"Missing one or more of Pre, Assump, or Model files in the {tech_type} directory.")
        return None, None, None

    # File paths
    pre_file_path = os.path.join(directory, pre_file)
    assump_file_path = os.path.join(directory, assump_file)
    model_file_path = os.path.join(directory, model_file)

    # Read CSV files into DataFrames
    pre_df = pd.read_csv(pre_file_path, low_memory=False, header=None)
    assump_df = pd.read_csv(assump_file_path, low_memory=False, header=None)
    model_df = pd.read_csv(model_file_path, low_memory=False, header=None)

    # Apply transformations based on technology type
    if tech_type == 'solar':
        pre_transformed, _ = transform_solar_pre(pre_df, pre_file, 'yellow_solar_pre')
        assump_transformed, _ = transform_solar_assump(assump_df, pre_transformed, assump_file, 'yellow_solar_assump')
        model_transformed = transform_solar_model(model_df, assump_transformed, model_file, 'yellow_solar_model')

    elif tech_type == 'battery':
        pre_transformed, _ = transform_battery_pre(pre_df, pre_file, 'yellow_battery_pre')
        assump_transformed, _ = transform_battery_assump(assump_df, pre_transformed, assump_file, 'yellow_battery_assump')
        model_transformed = transform_battery_model(model_df, assump_transformed, model_file, 'yellow_battery_model')

    elif tech_type == 'wind':
        pre_transformed, _ = transform_wind_pre(pre_df, pre_file, 'yellow_wind_pre')
        assump_transformed, _ = transform_wind_assump(assump_df, pre_transformed, assump_file, 'yellow_wind_assump')
        model_transformed = transform_wind_model(model_df, assump_transformed, model_file, 'yellow_wind_model')

    elif tech_type == 'tline':
        pre_transformed, _ = transform_tline_pre(pre_df, pre_file, 'yellow_tline_pre')
        assump_transformed, _ = transform_tline_assump(assump_df, pre_transformed, assump_file, 'yellow_tline_assump')
        model_transformed = transform_tline_model(model_df, assump_transformed, model_file, 'yellow_tline_model')

    # Return the transformed DataFrames
    return pre_transformed, assump_transformed, model_transformed

# Function to process all directories for each technology and return results
def process_all_directories_in_order(final_destination):
    solar_dir = os.path.join(final_destination, 'solar')
    battery_dir = os.path.join(final_destination, 'battery')
    wind_dir = os.path.join(final_destination, 'wind')
    tline_dir = os.path.join(final_destination, 'tline')

    # Process each technology type and store the results in a dictionary
    results = {
        'solar': process_csv_files_in_order(solar_dir, 'solar'),
        'battery': process_csv_files_in_order(battery_dir, 'battery'),
        'wind': process_csv_files_in_order(wind_dir, 'wind'),
        'tline': process_csv_files_in_order(tline_dir, 'tline')
    }

    return results

    
