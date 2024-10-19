import os
import random
import logging
import json
import pandas as pd
from datetime import datetime
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
import great_expectations as ge

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'email_on_failure': False,
    'email_on_retry': False,
}

@dag(
    dag_id='data_ingestion_with_validation',
    description='A DAG for data ingestion, validation with Great Expectations, and parallel processing',
    schedule_interval='@hourly',
    start_date=days_ago(1),
    max_active_runs=1,
    default_args=default_args,
    tags=['data_ingestion'],
)
def data_ingestion_dag():

    @task
    def read_data() -> str:
        raw_data_folder = '/opt/airflow/raw_data'
        files = [f for f in os.listdir(raw_data_folder) if f.endswith('.csv')]
        
        if not files:
            logging.warning("No files found in the raw data folder")
            return ""
        
        # Randomly select a file
        file_path = os.path.join(raw_data_folder, random.choice(files))
        logging.info(f"Reading file: {file_path}")
        return file_path

    @task
    def validate_data(file_path: str) -> dict:
        if not file_path:
            logging.error("No file provided for validation")
            return {"status": "Failed", "errors": []}

        # Load data using Great Expectations
        df = pd.read_csv(file_path)
        ge_df = ge.from_pandas(df)

        # Create an expectation suite
        expectation_suite = ge_df.expect_column_values_to_not_be_null("airline")
        expectation_suite = ge_df.expect_column_values_to_be_in_set(
            "source_city", ["Mumbai", "Delhi", "Hyderabad", "Bangalore", "Kolkata", "Chennai"]
        )

        # Validate data
        validation_results = ge_df.validate()
        
        errors = []
        criticality = "low"

        if not validation_results["success"]:
            for result in validation_results["results"]:
                if not result["success"]:
                    errors.append(result["expectation_config"]["kwargs"])
                    criticality = "high" if result["result"]["unexpected_percent"] > 50 else "medium"
        
        validation_report = validation_results.to_json_dict()
        logging.info(f"Validation report: {validation_report}")

        return {"status": "Success" if validation_results["success"] else "Failed", "criticality": criticality, "errors": errors}

    @task
    def save_statistics(file_path: str, validation_report: dict):
        engine = create_engine('sqlite:////opt/airflow/statistics.db')  # Example for SQLite
        df = pd.read_csv(file_path)
        
        # Gather statistics
        stats = {
            'filename': os.path.basename(file_path),
            'total_rows': len(df),
            'valid_rows': len(df) - len(validation_report['errors']),
            'invalid_rows': len(validation_report['errors']),
            'criticality': validation_report['criticality'],
            'timestamp': datetime.now().isoformat()
        }

        # Save statistics to the database
        stats_df = pd.DataFrame([stats])
        stats_df.to_sql('data_statistics', con=engine, if_exists='append', index=False)

        logging.info(f"Statistics saved: {stats}")

    @task
    def send_alert(validation_report: dict, file_path: str):
        # Create alert message
        filename = os.path.basename(file_path)
        alert_message = {
            "text": f"Data validation {validation_report['status']} for file {filename}\n"
                    f"Criticality: {validation_report['criticality']}\n"
                    f"Errors: {validation_report['errors']}\n"
                    f"Report: /path/to/great_expectations_report.html"
        }

        # Send alert to Microsoft Teams
        SimpleHttpOperator(
            task_id='send_teams_alert',
            method='POST',
            http_conn_id='msteams_webhook',
            endpoint='',
            headers={"Content-Type": "application/json"},
            data=json.dumps(alert_message),
        ).execute({})

        logging.info(f"Alert sent: {alert_message}")

    @task
    def split_and_save_data(file_path: str, validation_report: dict):
        df = pd.read_csv(file_path)

        # Create good and bad data based on validation
        good_data = df[~df.index.isin([error['unexpected_index_list'] for error in validation_report['errors']])]
        bad_data = df[df.index.isin([error['unexpected_index_list'] for error in validation_report['errors']])]

        # Define good and bad data folders
        good_data_folder = '/opt/airflow/good_data'
        bad_data_folder = '/opt/airflow/bad_data'
        os.makedirs(good_data_folder, exist_ok=True)
        os.makedirs(bad_data_folder, exist_ok=True)

        # Save data
        good_data_file = os.path.join(good_data_folder, os.path.basename(file_path))
        bad_data_file = os.path.join(bad_data_folder, os.path.basename(file_path))

        good_data.to_csv(good_data_file, index=False)
        bad_data.to_csv(bad_data_file, index=False)

        # Delete the original raw file
        os.remove(file_path)
        logging.info(f"File {file_path} has been split and saved")

    # DAG Workflow
    file_path = read_data()

    validation_report = validate_data(file_path)

    # Execute in parallel: save statistics, send alerts, and split data
    save_statistics(file_path, validation_report)
    send_alert(validation_report, file_path)
    split_and_save_data(file_path, validation_report)

# Instantiate the DAG
data_ingestion_with_validation = data_ingestion_dag()
