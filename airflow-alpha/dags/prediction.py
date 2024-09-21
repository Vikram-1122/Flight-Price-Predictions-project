import os
import requests
import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Path to track processed files
processed_files_tracker = '/opt/airflow/processed_files_tracker.txt'

def get_processed_files():
    """Read the list of processed files from the tracker."""
    if os.path.exists(processed_files_tracker):
        with open(processed_files_tracker, 'r') as f:
            processed_files = f.read().splitlines()
    else:
        processed_files = []
    return processed_files

def update_processed_files(new_file):
    """Update the tracker with the newly processed file."""
    with open(processed_files_tracker, 'a') as f:
        f.write(f"{new_file}\n")

@dag(default_args=default_args, schedule_interval='*/2 * * * *', start_date=days_ago(1), catchup=False, description='Scheduled Prediction DAG')
def prediction_dag():
    @task
    def check_for_new_data():
        good_data_folder = '/opt/airflow/good_data'  
        all_files = [os.path.join(good_data_folder, f) for f in os.listdir(good_data_folder) if f.endswith('.csv')]
        processed_files = get_processed_files()
        
    
        new_files = [file for file in all_files if file not in processed_files]
        
        if not new_files:
            raise AirflowSkipException("No new files to process. Skipping DAG run.")
        
        
        return new_files[0]

    @task
    def make_predictions(file_path: str):
        model_api_url = 'http://localhost:8000/predict'  
        
        try:
            files = {'file': open(file_path, 'rb')}
            response = requests.post(model_api_url, files=files)
            response.raise_for_status()
            predictions = response.json()
            
            print(f"Predictions for {file_path}: {predictions}")
        except requests.exceptions.RequestException as e:
            print(f"Failed to get predictions for {file_path}: {e}")

        update_processed_files(file_path)

  
    new_file = check_for_new_data()
    make_predictions(new_file)

dag_instance = prediction_dag()
