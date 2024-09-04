import pandas as pd
import os
import shutil
import random
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.microsoft.teams.operators.teams import TeamsWebhookOperator

# Define default arguments
default_args = {
    'owner': 'airflow',
    'retries': 1
}

@dag(
    dag_id='data_ingestion_dag',
    description='DAG for data ingestion and validation',
    tags=['data_ingestion'],
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    max_active_runs=1,
    default_args=default_args,
)
def data_ingestion_dag():

    @task
    def read_data() -> str:
        raw_data_folder = '/opt/airflow/raw_data'
        files = [f for f in os.listdir(raw_data_folder) if os.path.isfile(os.path.join(raw_data_folder, f))]
        if not files:
            raise ValueError("No files found in the raw_data folder.")
        selected_file = random.choice(files)
        file_path = os.path.join(raw_data_folder, selected_file)
        return file_path

    @task
    def validate_data(file_path: str) -> (str, list):
        errors = []
        try:
            data = pd.read_csv(file_path)
            
            if 'duration' not in data.columns:
                errors.append("Missing 'duration' column.")
            else:
                # Check if 'duration' contains numeric values
                if not pd.to_numeric(data['duration'], errors='coerce').notna().all():
                    errors.append("Invalid values in 'duration' column.")
                
            # Example validation: Check for empty rows
            if data.isnull().values.any():
                errors.append("Null values found in the data.")

        except Exception as e:
            errors.append(str(e))
        
        is_valid = len(errors) == 0
        return file_path, errors, is_valid

    @task
    def save_statistics(file_path: str, errors: list, is_valid: bool):
        # Implement statistics saving logic here
        stats_path = '/opt/airflow/data_statistics.csv'
        if not os.path.isfile(stats_path):
            # Create header if file does not exist
            with open(stats_path, 'w') as f:
                f.write("file_path,is_valid,error_count,errors\n")
        with open(stats_path, 'a') as f:
            f.write(f"{file_path},{is_valid},{len(errors)},{'|'.join(errors)}\n")

    @task
    def send_alerts(file_path: str, errors: list):
        # Generate report
        report = f"Data validation report for file: {file_path}\n"
        if errors:
            report += "Errors:\n" + "\n".join(errors)
        else:
            report += "No errors found."

        # Send alert using Teams
        teams_message = TeamsWebhookOperator(
            task_id='send_teams_alert',
            http_conn_id='teams_webhook',
            message=report
        )
        teams_message.execute(context={})

    @task
    def split_and_save_data(file_path: str, is_valid: bool):
        if is_valid:
            destination_folder = '/opt/airflow/good_data'
        else:
            destination_folder = '/opt/airflow/bad_data'
        
        os.makedirs(destination_folder, exist_ok=True)
        shutil.move(file_path, os.path.join(destination_folder, os.path.basename(file_path)))

    # Define task flow
    file_path = read_data()
    file_path, errors, is_valid = validate_data(file_path)
    save_statistics(file_path, errors, is_valid)
    send_alerts(file_path, errors)
    split_and_save_data(file_path, is_valid)

data_ingestion_dag = data_ingestion_dag()
