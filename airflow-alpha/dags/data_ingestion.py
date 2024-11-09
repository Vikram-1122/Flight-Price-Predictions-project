from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowSkipException
import great_expectations as ge
import os
import shutil
import requests
import pandas as pd
from datetime import datetime, timedelta


RAW_DATA_FOLDER = "/opt/airflow/raw_data"
GOOD_DATA_FOLDER = "/opt/airflow/good_data"
BAD_DATA_FOLDER = "/opt/airflow/bad_data"

required_columns = [
    'airline', 'flight', 'source_city', 'departure_time', 'stops',
    'destination_city', 'travel_class', 'duration', 'days_left', 'price', 'arrival_time'
]

categorical_cols = {
    "airline": ["AirAsia", "SpiceJet", "Vistara", "GO_FIRST", "Indigo", "Air_India"],
    "source_city": ["Mumbai", "Delhi", "Hyderabad", "Bangalore", "Kolkata", "Chennai"],
    "departure_time": ["Early_Morning", "Morning", "Afternoon", "Evening", "Night"],
    "stops": ["zero", "one", "two", "three", "four"],
    "arrival_time": ["Morning", "Early_Morning", "Afternoon", "Evening", "Night"],
    "destination_city": ["Delhi", "Hyderabad", "Bangalore", "Kolkata", "Chennai", "Mumbai"],
    "travel_class": ["Economy", "Business", "First"]
}


def read_data():
    files = [f for f in os.listdir(RAW_DATA_FOLDER) if f.endswith('.csv')]
    if not files:
        raise AirflowSkipException("No files found in the raw_data folder, skipping task.")
    file_name = files[0]
    file_path = os.path.join(RAW_DATA_FOLDER, file_name)
    return file_name, file_path


def validate_data(file_name, file_path):
    df = pd.read_csv(file_path)
    validation_results = {}
    validation_success = True  

    
    missing_columns = [col for col in required_columns if col not in df.columns]
    validation_results['missing_columns_count'] = len(missing_columns)
    if validation_results['missing_columns_count'] > 0:
        validation_success = False

    for col in required_columns:
        validation_results[f"missing_rows_{col}"] = df[col].isnull().sum() if col in df.columns else 0

   
    for col, valid_values in categorical_cols.items():
        if col in df.columns:
            try:
                ge_df = ge.dataset.PandasDataset(df)
                result = ge_df.expect_column_values_to_be_in_set(col, valid_values)
                validation_results[f"data_mismatch_{col}"] = result.result["unexpected_count"]
            except Exception as e:
                validation_results[f"data_mismatch_{col}"] = 0
                print(f"Error validating categorical values for column {col}: {str(e)}")

    
    timestamp = datetime.now()
    validation_results['date'] = timestamp.strftime('%Y-%m-%d')
    validation_results['time'] = timestamp.strftime('%H:%M:%S')

    return validation_results, validation_success


def split_and_save(file_name, file_path, validation_success):
    destination_folder = GOOD_DATA_FOLDER if validation_success else BAD_DATA_FOLDER
    shutil.move(file_path, os.path.join(destination_folder, file_name))


def send_alert(file_name, validation_success):
    status = "Successful" if validation_success else "Failed"
    message = {"text": f"Validation status for {file_name}: {status}. Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"}
    msteams_webhook = BaseHook.get_connection('msteams_webhook').host
    response = requests.post(msteams_webhook, json=message)
    if response.status_code != 200:
        print(f"Failed to send alert for {file_name}. Status code: {response.status_code}")


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'Data_Ingestion_Dag',
    default_args=default_args,
    schedule_interval=timedelta(minutes=2),
    catchup=False,
    max_active_runs=1,
) as dag:

    
    read_data_task = PythonOperator(
        task_id='read_data',
        python_callable=read_data,
    )

    
    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        op_args=["{{ task_instance.xcom_pull(task_ids='read_data')[0] }}", "{{ task_instance.xcom_pull(task_ids='read_data')[1] }}"],
    )

    
    save_statistics_task = PostgresOperator(
        task_id='save_statistics',
        postgres_conn_id='postgres_conn',
        sql="""
        INSERT INTO validation_statistics (
            file_name, date, time, missing_columns_count,
            missing_rows_airline, missing_rows_flight, missing_rows_source_city,
            missing_rows_departure_time, missing_rows_stops, missing_rows_destination_city,
            missing_rows_travel_class, missing_rows_duration, missing_rows_days_left,
            missing_rows_price, missing_rows_arrival_time,
            data_mismatch_airline, data_mismatch_source_city, data_mismatch_departure_time,
            data_mismatch_stops, data_mismatch_arrival_time, data_mismatch_destination_city,
            data_mismatch_travel_class
        ) VALUES (
            '{{ task_instance.xcom_pull(task_ids="read_data")[0] }}',
            '{{ task_instance.xcom_pull(task_ids="validate_data")[0]["date"] }}',
            '{{ task_instance.xcom_pull(task_ids="validate_data")[0]["time"] }}',
            {{ task_instance.xcom_pull(task_ids="validate_data")[0]["missing_columns_count"] }},
            {{ task_instance.xcom_pull(task_ids="validate_data")[0]["missing_rows_airline"] }},
            {{ task_instance.xcom_pull(task_ids="validate_data")[0]["missing_rows_flight"] }},
            {{ task_instance.xcom_pull(task_ids="validate_data")[0]["missing_rows_source_city"] }},
            {{ task_instance.xcom_pull(task_ids="validate_data")[0]["missing_rows_departure_time"] }},
            {{ task_instance.xcom_pull(task_ids="validate_data")[0]["missing_rows_stops"] }},
            {{ task_instance.xcom_pull(task_ids="validate_data")[0]["missing_rows_destination_city"] }},
            {{ task_instance.xcom_pull(task_ids="validate_data")[0]["missing_rows_travel_class"] }},
            {{ task_instance.xcom_pull(task_ids="validate_data")[0]["missing_rows_duration"] }},
            {{ task_instance.xcom_pull(task_ids="validate_data")[0]["missing_rows_days_left"] }},
            {{ task_instance.xcom_pull(task_ids="validate_data")[0]["missing_rows_price"] }},
            {{ task_instance.xcom_pull(task_ids="validate_data")[0]["missing_rows_arrival_time"] }},
            {{ task_instance.xcom_pull(task_ids="validate_data")[0]["data_mismatch_airline"] }},
            {{ task_instance.xcom_pull(task_ids="validate_data")[0]["data_mismatch_source_city"] }},
            {{ task_instance.xcom_pull(task_ids="validate_data")[0]["data_mismatch_departure_time"] }},
            {{ task_instance.xcom_pull(task_ids="validate_data")[0]["data_mismatch_stops"] }},
            {{ task_instance.xcom_pull(task_ids="validate_data")[0]["data_mismatch_arrival_time"] }},
            {{ task_instance.xcom_pull(task_ids="validate_data")[0]["data_mismatch_destination_city"] }},
            {{ task_instance.xcom_pull(task_ids="validate_data")[0]["data_mismatch_travel_class"] }}
        );
        """
    )

    
    split_and_save_task = PythonOperator(
        task_id='split_and_save',
        python_callable=split_and_save,
        op_args=["{{ task_instance.xcom_pull(task_ids='read_data')[0] }}", "{{ task_instance.xcom_pull(task_ids='read_data')[1] }}", "{{ task_instance.xcom_pull(task_ids='validate_data')[1] }}"]
    )

    
    send_alert_task = PythonOperator(
        task_id='send_alert',
        python_callable=send_alert,
        op_args=["{{ task_instance.xcom_pull(task_ids='read_data')[0] }}", "{{ task_instance.xcom_pull(task_ids='validate_data')[1] }}"]
    )

    read_data_task >> validate_data_task >> [save_statistics_task , split_and_save_task , send_alert_task]
