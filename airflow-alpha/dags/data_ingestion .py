import logging
import pandas as pd
import os
import shutil
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.email import EmailOperator

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['dspskyprix@gmail.com'],  # Email addresses to receive alerts
}

# Define DAG using @dag decorator
@dag(
    dag_id='data_ingestion_dag',
    description='A DAG for ingesting, validating, and processing data',
    tags=['data_ingestion'],
    schedule_interval=timedelta(minutes=5),  # Run every 5 minutes
    start_date=days_ago(1),  # Start date set to 1 day ago
    max_active_runs=1,  # Ensure only one active run at a time
    default_args=default_args,
)
def my_data_ingestion_dag():
    
    @task
    def read_data() -> str:
        raw_data_folder = '/opt/airflow/raw_data'  # Use absolute path inside container
        files = [f for f in os.listdir(raw_data_folder) if f.endswith('.csv') and not f.startswith('.ipynb_checkpoints')]
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
            raise ValueError("No file path provided")
        
        data = pd.read_csv(file_path)
        critical_columns = ['airline', 'flight', 'source_city', 'destination_city', 'travel_class', 'duration', 'price']
        if data[critical_columns].isnull().values.any():
            raise ValueError("Critical columns contain missing values!")
            
        valid_stops = ['zero', 'one', 'two_or_more', 'three', 'four', 'five', 'two']
        invalid_stops = data[~data['stops'].isin(valid_stops)]['stops'].unique()
        if len(invalid_stops) > 0:
            logging.error(f"Invalid stops values found: {invalid_stops}")
            raise ValueError("Stops column contains invalid values.")
        logging.info("Data validation passed.")
        return file_path

    @task
    def save_statistics(file_path: str):
        data = pd.read_csv(file_path)
        statistics = data.describe(include='all')
        statistics_file_path = '/opt/airflow/data_statistics.csv'  # Use absolute path inside container
        statistics.to_csv(statistics_file_path)
        logging.info(f"Statistics saved to {statistics_file_path}.")

    @task
    def split_and_save_data(file_path: str):
        data = pd.read_csv(file_path)
        
        # Convert 'duration' column to numeric, setting errors='coerce' to handle non-numeric values
        data['duration'] = pd.to_numeric(data['duration'], errors='coerce')
        
        # Log data types and some example values for debugging
        logging.info(f"Data types:\n{data.dtypes}")
        logging.info(f"Sample data:\n{data.head()}")
        
        # Define good data condition
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
        
        # Determine destination folder based on the good data condition
        destination_folder = '/opt/airflow/good_data' if good_data_condition.all() else '/opt/airflow/bad_data'
        filename = os.path.basename(file_path)
        destination_path = os.path.join(destination_folder, filename)
        shutil.move(file_path, destination_path)
        logging.info(f"File has been moved to {destination_path}.")

    # Define email alert task
    def create_alert_email_task(subject, html_content):
        return EmailOperator(
            task_id='send_alert',
            to=['dspskyprix@gmail.com'],
            subject=subject,
            html_content=html_content
        )

    # Define tasks
    file_path = read_data()
    validated_file_path = validate_data(file_path)
    save_statistics(validated_file_path)
    split_and_save_data(validated_file_path)

    # Set up email alerts on failure
    validation_failure_alert = create_alert_email_task(
        subject='Data Validation Failure Alert',
        html_content="""<h3>Data validation failed</h3>
                        <p>Please check the logs for details.</p>"""
    )

    # Set alert email task to run on failure of validation
    validated_file_path.on_failure_callback = lambda context: validation_failure_alert.execute(context)

data_ingestion_dag = my_data_ingestion_dag()
