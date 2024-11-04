import logging
import pandas as pd
import os
import json
from datetime import datetime
from sqlalchemy import create_engine
import great_expectations as ge
from great_expectations.data_context import DataContext
from great_expectations.core.batch import BatchRequest
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
    dag_id='data_ingestion_dag_1',
    description='A DAG for data ingestion and validation with Great Expectations and PostgreSQL logging',
    schedule_interval='* * * * *',  # every minute
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
    def validate_data(file_path: str) -> dict:
        if not file_path:
            logging.error("No file provided for validation")
            return {"status": "Failed", "errors": {}}

        # Load data and initialize Great Expectations DataFrame
        data = pd.read_csv(file_path)
        df_ge = ge.from_pandas(data)

        # PostgreSQL Connection
        engine = create_engine('postgresql://postgres:root@localhost:5432/predictions')

        # Define the validation suite
        validation_suite = [
            {"expectation": "expect_column_values_to_be_in_set", "column": "travel_class", "kwargs": {"value_set": ["Economy", "Business"]}},
            {"expectation": "expect_column_values_to_be_in_set", "column": "airline", "kwargs": {"value_set": ["AirAsia", "SpiceJet", "Vistara", "GO_FIRST", "Indigo", "Air_India"]}},
            {"expectation": "expect_column_values_to_be_in_set", "column": "source_city", "kwargs": {"value_set": ["Mumbai", "Delhi", "Hyderabad", "Bangalore", "Kolkata", "Chennai"]}},
            {"expectation": "expect_column_values_to_be_between", "column": "price", "kwargs": {"min_value": 1, "max_value": 100000}},
            {"expectation": "expect_column_values_to_be_of_type", "column": "price", "kwargs": {"type_": "float"}},
            {"expectation": "expect_column_values_to_be_of_type", "column": "travel_class", "kwargs": {"type_": "string"}},
            {"expectation": "expect_column_to_exist", "column": "days_left", "kwargs": {}},
            {"expectation": "expect_column_values_to_not_be_null", "column": "price", "kwargs": {}},
            {"expectation": "expect_column_value_lengths_to_be_between", "column": "duration", "kwargs": {"min_value": 4, "max_value": 8}},
            {"expectation": "expect_column_values_to_match_regex", "column": "duration", "kwargs": {"regex": r"^\d{1,2}h \d{1,2}m$"}}
        ]

        # Error logging dictionary
        error_log = {
            "error_type": [],
            "column": [],
            "row_index": [],
            "timestamp": []
        }

        # Run validations and log failures
        for validation in validation_suite:
            expectation = validation["expectation"]
            column = validation["column"]
            kwargs = validation["kwargs"]

            # Run the expectation
            result = getattr(df_ge, expectation)(column=column, **kwargs)

            # Check for failures and log errors for each failed row
            if not result.success:
                for unexpected_index in result.result.get("unexpected_index_list", []):
                    error_log["error_type"].append(expectation)
                    error_log["column"].append(column)
                    error_log["row_index"].append(unexpected_index)
                    error_log["timestamp"].append(datetime.now())

        # Convert error log to DataFrame for easy insertion into PostgreSQL
        if error_log["error_type"]:  # Only save if there are errors
            error_log_df = pd.DataFrame(error_log)
            error_log_df.to_sql("stats", engine, if_exists="append", index=False)
            logging.info("Error log saved to PostgreSQL")

        # Prepare validation results summary
        validation_status = "Success" if not error_log["error_type"] else "Failed"
        return {"status": validation_status, "errors": error_log}

    @task
    def save_statistics(file_path: str, validation_results: dict):
        if validation_results["status"] == "Success":
            logging.info("Validation passed; skipping statistics logging.")
            return

        # Prepare stats data
        stats_data = {
            "file_name": os.path.basename(file_path),
            "validation_status": validation_results["status"],
            "error_count": len(validation_results["errors"]["error_type"]),
            "error_details": json.dumps(validation_results["errors"]),
            "timestamp": datetime.now()
        }

        # Save to PostgreSQL
        engine = create_engine("postgresql://postgres:root@localhost:5433/predictions")
        with engine.connect() as conn:
            conn.execute(
                """
                INSERT INTO stats (file_name, validation_status, error_count, error_details, timestamp)
                VALUES (%(file_name)s, %(validation_status)s, %(error_count)s, %(error_details)s, %(timestamp)s)
                """,
                stats_data
            )
        logging.info("Statistics saved to PostgreSQL")

    @task
    def send_alert(file_path: str, validation_results: dict):
        status = validation_results["status"]
        filename = os.path.basename(file_path)
        error_summary = f"File ingestion {status}! File: {filename}\nErrors: {json.dumps(validation_results['errors'])}"

        alert = SimpleHttpOperator(
            task_id='send_alert',
            method='POST',
            http_conn_id='msteams_webhook',
            endpoint='',
            headers={"Content-Type": "application/json"},
            data=json.dumps({"text": error_summary}),
        )
        alert.execute({})  # Execute the alert task

    @task
    def split_and_save_data(file_path: str, validation_results: dict):
        data = pd.read_csv(file_path)
        # Data splitting code here ...
        # Move or split the data into good_data and bad_data folders as per validation

    # Define the workflow
    file_path = read_data()
    validation_results = validate_data(file_path)
    save_statistics_task = save_statistics(file_path, validation_results)
    send_alert_task = send_alert(file_path, validation_results)
    split_and_save_data_task = split_and_save_data(file_path, validation_results)

    # Define task dependencies
    file_path >> validation_results >> save_statistics_task >> send_alert_task

data_ingestion_dag = my_data_ingestion_dag()
