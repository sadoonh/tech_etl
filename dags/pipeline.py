# Import Airflow libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from extract import convert_xlsx_files_in_directory, move_csv_files_to_final_destination
from transform import process_all_directories_in_order

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 11),  
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'etl_pipeline',  # DAG ID
    default_args=default_args,
    description='An ETL pipeline for tech-specific transformations',
    schedule_interval=timedelta(days=1),  # Adjust the schedule as needed
    catchup=False,
)

# Define the source and destination directories
RAW_EXCEL = 'C:/Users/sadoo/OneDrive/Desktop/Lambda/raw_excel'
RAW_CSV = 'C:/Users/sadoo/OneDrive/Desktop/Lambda/raw_csv'
ARCHIVE_EXCEL = 'C:/Users/sadoo/OneDrive/Desktop/Lambda/archive_excel'
TECH_CSV = 'C:/Users/sadoo/OneDrive/Desktop/Lambda/tech_csv'

# 1. Extract Task: Convert Excel files to CSV and move CSVs to their final destination
def extract_data():
    # Convert Excel to CSV
    convert_xlsx_files_in_directory(RAW_EXCEL, ARCHIVE_EXCEL, RAW_CSV)

    # Move the CSVs to their respective final destination folders
    move_csv_files_to_final_destination(RAW_CSV, TECH_CSV)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

# 2. Transform Task: Apply the necessary transformations for solar, wind, battery
def transform_data():
    process_all_directories_in_order(TECH_CSV)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

# Set task dependencies (Extract -> Transform)
extract_task >> transform_task