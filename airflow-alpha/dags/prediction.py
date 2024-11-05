import os
import requests
import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.utils.dates import days_ago
from datetime import timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Paths
GOOD_DATA_PATH = '/opt/airflow/good_data/'  # Directory for data files
PROCESSED_FILES_TRACKER = '/opt/airflow/processed_files_tracker.txt'  # File to track processed files
PREDICTION_API_URL = 'http://localhost:8000/predict'  # API endpoint for predictions

# Utility functions for tracking processed files
def get_processed_files():
    """Read the list of processed files from the tracker."""
    if os.path.exists(PROCESSED_FILES_TRACKER):
        with open(PROCESSED_FILES_TRACKER, 'r') as f:
            processed_files = f.read().splitlines()
    else:
        processed_files = []
    return processed_files

def update_processed_files(new_file):
    """Update the tracker with the newly processed file."""
    with open(PROCESSED_FILES_TRACKER, 'a') as f:
        f.write(f"{new_file}\n")

@dag(
    default_args=default_args,
    schedule_interval='*/2 * * * *',  # Every 2 minutes
    start_date=days_ago(1),
    catchup=False,
    description='Scheduled Prediction DAG'
)
def prediction_dag():
    @task
    def check_for_new_data():
        """Check the directory for new data files."""
        all_files = [f for f in os.listdir(GOOD_DATA_PATH) if f.endswith('.csv')]
        processed_files = get_processed_files()

        # Identify unprocessed files
        new_files = [f for f in all_files if f not in processed_files]

        if not new_files:
            raise AirflowSkipException("No new files to process. Skipping DAG run.")

        return new_files

    @task
    def make_predictions(files: list):
        """Send files to the prediction API and log the results."""
        for file in files:
            file_path = os.path.join(GOOD_DATA_PATH, file)
            data = pd.read_csv(file_path)

            # Replace NaN with None for JSON compatibility
            data = data.where(pd.notnull(data), None)
            data_json = {'data': data.to_dict(orient='records')}

            try:
                response = requests.post(PREDICTION_API_URL, json=data_json)
                response.raise_for_status()
                predictions = response.json().get('predictions', [])

                # Add predictions to the DataFrame
                data['Prediction'] = predictions
                print(f"Predictions for {file}: {data.head()}")

                # Save or log the predicted DataFrame if needed
                # For example, save it back to GOOD_DATA_PATH with a new name
                predicted_file_path = os.path.join(GOOD_DATA_PATH, f"predicted_{file}")
                data.to_csv(predicted_file_path, index=False)
                
                update_processed_files(file)

            except requests.exceptions.RequestException as e:
                print(f"Failed to get predictions for {file}: {e}")
            except Exception as err:
                print(f"An error occurred during prediction for {file}: {err}")

    # DAG task sequence
    new_files = check_for_new_data()
    make_predictions(new_files)

# Instantiate the DAG
dag_instance = prediction_dag()
