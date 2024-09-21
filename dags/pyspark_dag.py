from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
import shutil

from setup.pyspark import process_stock_data

STOCK_DATA_DIR = 'dags/stock_yearly_data'


def process_file(file_path):
    directory_name, file_name = os.path.split(file_path)
    new_directory_name = directory_name + "/archive/"
    print(file_path,new_directory_name)
    process_stock_data(pd.read_csv(file_path))
    shutil.move(file_path,new_directory_name)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'process_stock_data_dag',
    default_args=default_args,
    description='DAG to process yearly stock CSV files using PySpark',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    def process_files(**kwargs):
        files = [f for f in os.listdir(STOCK_DATA_DIR) if f.endswith('.csv')]
        for file_name in files:
            file_path = os.path.join(STOCK_DATA_DIR, file_name)
            process_file(file_path)

    process_files_task = PythonOperator(
        task_id='process_files',
        python_callable=process_files,
        provide_context=True,
    )

    process_files_task
