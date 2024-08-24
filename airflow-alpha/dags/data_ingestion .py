import logging
import pandas as pd
import os
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

@dag(
    dag_id='data_ingestion_dag',
    description='A DAG for data ingestion and validation',
    tags=['data_ingestion'],
    schedule=timedelta(days=1),  # Set to appropriate schedule
    start_date=days_ago(1),  # Start date for the DAG
    max_active_runs=1  # Ensure only one active run at a time
)
def data_ingestion_dag():

    @task
    def read_data() -> str:
        raw_data_folder = '/path/to/raw-data'  # Update to actual path
        files = os.listdir(raw_data_folder)
        if files:
            file_path = os.path.join(raw_data_folder, files[0])
            logging.info(f'Read file: {file_path}')
            return file_path
        else:
            raise FileNotFoundError("No files found in the raw-data folder.")

    @task
    def validate_data(file_path: str) -> dict:
        data = pd.read_csv(file_path)
        
        # Step 1: Identify rows with missing values in important columns
        missing_values_condition = data[['airline', 'flight', 'source_city', 'destination_city', 'travel_class', 'price']].notnull().all(axis=1)

        # Step 2: Identify rows with datatype mismatches
        expected_dtypes = {
            'airline': str,
            'flight': str,
            'source_city': str,
            'destination_city': str,
            'travel_class': str,
            'price': float  # Adjust if necessary
        }

        def check_dtypes(row):
            for col, dtype in expected_dtypes.items():
                if not isinstance(row[col], dtype):
                    return False
            return True

        # Apply the datatype check
        datatype_mismatch_condition = data.apply(check_dtypes, axis=1)

        # Combine both conditions: rows with no missing values and correct data types
        good_data_condition = missing_values_condition & datatype_mismatch_condition
        validation_results = {
            'file_path': file_path,
            'good_data_count': good_data_condition.sum(),
            'bad_data_count': (~good_data_condition).sum(),
            'criticality': 'low'  # Adjust based on actual checks
        }
        logging.info(f'Validation results: {validation_results}')
        return validation_results

    @task
    def save_statistics(validation_results: dict):
        file_path = validation_results['file_path']
        # Implement actual database saving logic here
        logging.info(f'Saving statistics for {file_path} with results: {validation_results}')

    @task
    def send_alerts(validation_results: dict):
        criticality = validation_results['criticality']
        # Implement actual alert sending logic here
        logging.info(f'Sending alert with criticality: {criticality}')

    @task
    def split_and_save_data(file_path: str, validation_results: dict):
        data = pd.read_csv(file_path)
        
        # Reuse the validation conditions
        missing_values_condition = data[['airline', 'flight', 'source_city', 'destination_city', 'travel_class', 'price']].notnull().all(axis=1)
        
        expected_dtypes = {
            'airline': str,
            'flight': str,
            'source_city': str,
            'destination_city': str,
            'travel_class': str,
            'price': float
        }

        def check_dtypes(row):
            for col, dtype in expected_dtypes.items():
                if not isinstance(row[col], dtype):
                    return False
            return True

        datatype_mismatch_condition = data.apply(check_dtypes, axis=1)
        good_data_condition = missing_values_condition & datatype_mismatch_condition

        good_data = data[good_data_condition]
        bad_data = data[~good_data_condition]

        # Save good and bad data
        good_data_folder = '/path/to/good_data'  # Update to actual path
        bad_data_folder = '/path/to/bad_data'    # Update to actual path
        good_data.to_csv(os.path.join(good_data_folder, os.path.basename(file_path)), index=False)
        bad_data.to_csv(os.path.join(bad_data_folder, os.path.basename(file_path)), index=False)
        logging.info(f'Split and saved data from {file_path}')

    # Task relationships
    file_path = read_data()
    validation_results = validate_data(file_path)
    
    save_statistics(validation_results)
    send_alerts(validation_results)
    split_and_save_data(file_path, validation_results)

# Instantiate the DAG
data_ingestion_dag_instance = data_ingestion_dag()
