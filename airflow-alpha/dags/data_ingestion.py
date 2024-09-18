import logging
import pandas as pd
import os
import json
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.http.operators.http import SimpleHttpOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'email_on_failure': False,
    'email_on_retry': False,
}

@dag(
    dag_id='data_ingestion_dag',
    description='A DAG for ingesting, validating, and processing data',
    schedule_interval='0 */1 * * *',  # every one hour
    start_date=days_ago(1),
    max_active_runs=1,
    default_args=default_args,
    tags=['data_ingestion'],
)
def my_data_ingestion_dag():

    @task
    def read_data() -> str:
        raw_data_folder = '/opt/airflow/raw_data'

        # List the raw_data files
        files = [f for f in os.listdir(raw_data_folder) if f.endswith('.csv') and not f.startswith('.ipynb_checkpoints')]
        logging.info(f"Files found in raw data folder: {files}")

        if files:
            file_path = os.path.join(raw_data_folder, files[0])
            logging.info(f"Reading file: {file_path}")
            return file_path
        
        logging.warning("No files found in raw data folder")
        return ""

    @task
    def validate_data(file_path: str) -> str:
        if not file_path:
            logging.error("No file provided for validation")
            return "Failed"
        
        data = pd.read_csv(file_path)
        
        # Critical column datatypes
        critical_columns = {
            'airline': 'object',
            'flight': 'object',
            'source_city': 'object',
            'destination_city': 'object',
            'travel_class': 'object',
            'duration': 'float64',
            'price': 'int64',
            'stops': 'object',
            'days_left': 'int64',
            'departure_time': 'object',
            'arrival_time': 'object'
        }
        
        # Check for missing values in critical columns
        if data[list(critical_columns.keys())].isnull().values.any():
            logging.error("Critical columns contain missing values!")
            return "Failed"
        
        # Check data types of critical columns
        for column, expected_type in critical_columns.items():
            if data[column].dtype != expected_type:
                logging.error(f"Column '{column}' should be of type {expected_type}, but found type {data[column].dtype}.")
                return "Failed"
        
        # Validate 'stops' column
        valid_stops = ['zero', 'one', 'two_or_more', 'three', 'four', 'five', 'two']
        invalid_stops = data[~data['stops'].isin(valid_stops)]['stops'].unique()
        if len(invalid_stops) > 0:
            logging.error(f"Invalid stops values found: {invalid_stops}")
            return "Failed"
        
        # Validate time columns
        valid_times = ['Morning', 'Afternoon', 'Evening', 'Night', 'Early_Morning']
        invalid_departure_times = data[~data['departure_time'].isin(valid_times)]['departure_time'].unique()
        invalid_arrival_times = data[~data['arrival_time'].isin(valid_times)]['arrival_time'].unique()
        if len(invalid_departure_times) > 0:
            logging.error(f"Invalid departure times found: {invalid_departure_times}")
            return "Failed"
        if len(invalid_arrival_times) > 0:
            logging.error(f"Invalid arrival times found: {invalid_arrival_times}")
            return "Failed"
        
        logging.info("Data validation passed.")
        return "Success"

    @task
    def split_and_save_data(file_path: str, status: str) -> str:
        if status == "Failed":
            logging.error("Skipping data split due to validation failure.")
            return "Failed"
        
        data = pd.read_csv(file_path)
        
        # Attempt to convert 'duration' and 'price' columns
        data['duration'] = pd.to_numeric(data['duration'], errors='coerce')
        data['price'] = pd.to_numeric(data['price'], errors='coerce')
        
        # Define conditions for good and bad data
        good_data_condition = (
            data['airline'].notnull() &
            data['flight'].notnull() &
            data['source_city'].notnull() &
            data['destination_city'].notnull() &
            data['travel_class'].notnull() &
            data['duration'].between(0.5, 50) &
            (data['price'] > 0) &
            (data['days_left'] >= 0) &
            data['stops'].isin(['zero', 'one', 'two_or_more', 'two', 'three', 'four', 'five']) &
            data['departure_time'].isin(['Morning', 'Afternoon', 'Evening', 'Night', 'Early_Morning']) &
            data['arrival_time'].isin(['Morning', 'Afternoon', 'Evening', 'Night', 'Early_Morning'])
        )
        
        good_data_folder = '/opt/airflow/good_data'
        bad_data_folder = '/opt/airflow/bad_data'
        os.makedirs(good_data_folder, exist_ok=True)
        os.makedirs(bad_data_folder, exist_ok=True)
        
        good_data = data[good_data_condition]
        bad_data = data[~good_data_condition]
        
        good_data_file = os.path.join(good_data_folder, os.path.basename(file_path))
        bad_data_file = os.path.join(bad_data_folder, os.path.basename(file_path))
        
        # Save good data
        if not good_data.empty:
            good_data.to_csv(good_data_file, index=False)
            logging.info(f"Good data saved to {good_data_file}.")
        
        # Save bad data only if errors are found
        if not bad_data.empty:
            bad_data.to_csv(bad_data_file, index=False)
            logging.info(f"Bad data saved to {bad_data_file}.")
        
        # Remove the original file from raw data folder
        os.remove(file_path)
        logging.info(f"Original file removed from {file_path}")

        return "Success" if not bad_data.empty else "Success-NoBadData"

    @task
    def send_alert(file_path: str, status: str):
        filename = os.path.basename(file_path) if file_path else "Unknown"
        if status == "Success" or status == "Success-NoBadData":
            message = f"File ingestion successful! File: {filename}"
        else:
            message = f"File ingestion failed for {filename}"

        # Send alert to Microsoft Teams
        return SimpleHttpOperator(
            task_id='send_alert_task',
            method='POST',
            http_conn_id='msteams_webhook',
            endpoint='',
            headers={"Content-Type": "application/json"},
            data=json.dumps({"text": message}),
        ).execute({})

    # Define task dependencies
    file_path = read_data()
    validation_status = validate_data(file_path)

    # Parallel execution of these tasks
    split_data = split_and_save_data(file_path, validation_status)
    alert_task = send_alert(file_path, split_data)

    # Set up task dependencies
    validation_status >> split_data >> alert_task

data_ingestion_dag = my_data_ingestion_dag()
