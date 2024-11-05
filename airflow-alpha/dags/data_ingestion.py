import logging
import os
import json
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import great_expectations as ge
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
        files = [f for f in os.listdir(raw_data_folder) if f.endswith('.csv')]
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

    # Load data
    data = pd.read_csv(file_path)

    # PostgreSQL Connection
    engine = create_engine('postgresql://postgres:root@localhost:5432/predictions')

    # Error logging dictionary
    error_log = {
        "error_type": [],
        "column": [],
        "row_index": [],
        "timestamp": []
    }

    # Type mapping
    type_mapping = {
        "float": float,
        "string": str
    }

    # Define the validation suite
    validations = [
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

    # Run validations and log failures
    for validation in validations:
        expectation = validation["expectation"]
        column = validation["column"]
        kwargs = validation["kwargs"]

        # Use the appropriate Great Expectations methods
        if expectation == "expect_column_values_to_be_in_set":
            result = data[column].isin(kwargs["value_set"])
            unexpected_index_list = data.index[~result].tolist()
        elif expectation == "expect_column_values_to_be_between":
            result = data[column].between(kwargs["min_value"], kwargs["max_value"])
            unexpected_index_list = data.index[~result].tolist()
        elif expectation == "expect_column_values_to_be_of_type":
            result = data[column].apply(lambda x: isinstance(x, type_mapping[kwargs["type_"]]))
            unexpected_index_list = data.index[~result].tolist()
        elif expectation == "expect_column_to_exist":
            if column not in data.columns:
                unexpected_index_list = list(range(len(data)))  # All rows have this issue
            else:
                unexpected_index_list = []
        elif expectation == "expect_column_values_to_not_be_null":
            result = data[column].notnull()
            unexpected_index_list = data.index[~result].tolist()
        elif expectation == "expect_column_value_lengths_to_be_between":
            result = data[column].apply(len).between(kwargs["min_value"], kwargs["max_value"])
            unexpected_index_list = data.index[~result].tolist()
        elif expectation == "expect_column_values_to_match_regex":
            result = data[column].str.match(kwargs["regex"])
            unexpected_index_list = data.index[~result].tolist()

        # Check for failures and log errors for each failed row
        for unexpected_index in unexpected_index_list:
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
