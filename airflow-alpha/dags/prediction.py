import sys
import pickle
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os

sys.path.append('..')
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 1),
    'retries': 1
}

def check_for_new_data():
    good_data_folder = '..\dsp-skyprix\good_data'  # Replace with actual path
    files = os.listdir(good_data_folder)
    if files:
        return files
    return []

def make_predictions(file_paths):
    model_path = '../price_prediction_model.pkl'
    with open(model_path, 'rb') as model_file:
        model = pickle.load(model_file)
    
    for file_path in file_paths:
        data = pd.read_csv(file_path)
        predictions = model.predict(data[['source_city', 'destination_city']])  # Replace with actual feature names
        data['predictions'] = predictions
        data.to_csv(file_path.replace('good_data', 'predicted_data'), index=False)

with DAG('prediction_dag', default_args=default_args, schedule_interval='@hourly') as dag:
    check_for_new_data_task = PythonOperator(task_id='check_for_new_data', python_callable=check_for_new_data)
    make_predictions_task = PythonOperator(task_id='make_predictions', python_callable=make_predictions)

    check_for_new_data_task >> make_predictions_task
