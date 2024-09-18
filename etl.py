import pandas as pd
import os
import shutil
import csv
from xls2csv_package import Xlsx2csv
from extract import *
from transform import *
from tech_transform.solar.yellow_solar_assump import *
from tech_transform.wind.yellow_wind_assump import *
from load import load_df_to_db, load_table_filenames_from_db
from archive_csv_files import archive_csv_files
from delete_loaded_excel_files import delete_loaded_excel_files
from update_filenames_in_csv import update_filenames_in_csv

RAW_EXCEL = 'C:/Users/sadoo/OneDrive/Desktop/Lambda/raw_excel'
RAW_CSV = 'C:/Users/sadoo/OneDrive/Desktop/Lambda/raw_csv'
ARCHIVE_EXCEL = 'C:/Users/sadoo/OneDrive/Desktop/Lambda/archive_excel'
TECH_CSV = 'C:/Users/sadoo/OneDrive/Desktop/Lambda/tech_csv'

delete_loaded_excel_files()
convert_xlsx_files_in_directory(RAW_EXCEL, ARCHIVE_EXCEL, RAW_CSV)
move_csv_files_to_final_destination(RAW_CSV, TECH_CSV)
process_all_directories_in_order(TECH_CSV)
archive_csv_files()
