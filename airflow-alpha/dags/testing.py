import logging
import pandas as pd
import os
import json
import time
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
    dag_id='data_ingestion_testing',
    description='A DAG for ingesting, validating, and processing data',
    schedule_interval='0 */2 * * *',  # every 2 hours
    start_date=days_ago(1),
    max_active_runs=1,
    default_args=default_args,
    tags=['data_ingestion'],
)
def my_data_ingestion_dag():

    @task
    def read_data() -> str:
        raw_data_folder = '/opt/airflow/raw_data'
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
            return "Failed due to no file"

        try:
            data = pd.read_csv(file_path)
        except Exception as e:
            logging.error(f"Error reading file: {e}")
            return "Failed"

        # Define the expected values for certain columns
        valid_airlines = ["AirAsia", "SpiceJet", "Vistara", "GO_FIRST", "Indigo", "Air_India"]
        valid_source_cities = ["Mumbai", "Delhi", "Hyderabad", "Bangalore", "Kolkata", "Chennai"]
        valid_departure_times = ["Early_Morning", "Morning", "Afternoon", "Evening", "Night", "Late_Night"]
        valid_stops = ["zero", "one", "two", "three", "four", "two_or_more", "five"]
        valid_arrival_times = ["Morning", "Early_Morning", "Afternoon", "Evening", "Night", "Late_Night"]
        valid_destination_cities = ["Delhi", "Hyderabad", "Bangalore", "Kolkata", "Chennai", "Mumbai"]
        valid_travel_classes = ["Economy", "Business", "First"]

        # Check for missing values
        if data.isnull().values.any():
            logging.error("Dataset contains missing values!")
            return "Failed"

        # Validate specific columns
        if not data['airline'].isin(valid_airlines).all():
            logging.error("Invalid airline values found!")
            return "Failed"

        if not data['source_city'].isin(valid_source_cities).all():
            logging.error("Invalid source city values found!")
            return "Failed"

        if not data['departure_time'].isin(valid_departure_times).all():
            logging.error("Invalid departure time values found!")
            return "Failed"

        if not data['stops'].isin(valid_stops).all():
            logging.error("Invalid stops values found!")
            return "Failed"

        if not data['arrival_time'].isin(valid_arrival_times).all():
            logging.error("Invalid arrival time values found!")
            return "Failed"

        if not data['destination_city'].isin(valid_destination_cities).all():
            logging.error("Invalid destination city values found!")
            return "Failed"

        if not data['travel_class'].isin(valid_travel_classes).all():
            logging.error("Invalid travel class values found!")
            return "Failed"

        # Validate numerical columns
        if (data['duration'] < 0).any():
            logging.error("Invalid duration values found! Duration should be greater than or equal to 0.0 hours.")
            return "Failed"

        if (data['days_left'] < 0).any():
            logging.error("Invalid days_left values found! Days left should be greater than or equal to 0.")
            return "Failed"

        if (data['price'] < 0).any():
            logging.error("Invalid price values found! Price should be greater than or equal to 0.")
            return "Failed"

        # If all validations pass
        logging.info("Data validation passed.")
        return "Success"

    @task
    def split_and_save_data(file_path: str, validation_status: str) -> str:
        if validation_status == "Failed" or not file_path:
            logging.error(f"Validation failed or no file provided for splitting: {file_path}")
            return "Failed"

        try:
            data = pd.read_csv(file_path)
        except Exception as e:
            logging.error(f"Error reading file for splitting: {e}")
            return "Failed"

        good_data_folder = '/opt/airflow/good_data'
        bad_data_folder = '/opt/airflow/bad_data'
        
        good_data_condition = (
            data['airline'].notnull() &
            data['flight'].notnull() &
            data['source_city'].notnull() &
            data['destination_city'].notnull() &
            data['travel_class'].notnull() &
            data['duration'].between(0, 60) &
            (data['price'] > 0) &
            (data['days_left'] >= 0) &
            data['stops'].isin(['zero', 'one', 'two_or_more', 'two', 'three', 'four', 'five']) &
            data['departure_time'].isin(['Morning', 'Afternoon', 'Evening', 'Night', 'Early_Morning', 'Late_Night']) &
            data['arrival_time'].isin(['Morning', 'Afternoon', 'Evening', 'Night', 'Early_Morning', 'Late_Night'])
        )
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
        
        # Attempt to remove the original file with retry logic
        retry_attempts = 3
        for attempt in range(retry_attempts):
            try:
                os.remove(file_path)
                logging.info(f"Original file removed from {file_path}")
                break
            except PermissionError as e:
                logging.error(f"Attempt {attempt + 1}: PermissionError: {e}")
                time.sleep(5)  # Wait before retrying

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

