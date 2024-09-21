from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
import shutil

from setup.pyspark import process_stock_data

stock_data_dir = 'dags/stock_yearly_data'
consolidated_stock_data_file_path = 'dags/consolidated_stock_data/consolidated_stock_data.csv'

def preprocess_stock_data(file_path):
    df = pd.read_csv(file_path)
    required_columns = ['date', 'open', 'high', 'low', 'close', 'adj_close', 'volume', 'stock_symbol']
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Column {col} is missing from the input data.")

    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df.dropna(subset=['date', 'stock_symbol'], inplace=True)

    numeric_columns = ['open', 'high', 'low', 'close', 'adj_close', 'volume']
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')
    df[numeric_columns] = df[numeric_columns].fillna(method='ffill')

    df.sort_values(by=['stock_symbol', 'date'], inplace=True)
    df.to_csv(file_path, index=False, mode='w', header=True)

def append_to_consolidated_csv(annual_csv_file):
    df_annual = pd.read_csv(annual_csv_file)

    if not os.path.exists(consolidated_stock_data_file_path):
        #with header
        df_annual.to_csv(consolidated_stock_data_file_path, mode='w', index=False, header=True)
    else:
        # without header
        df_annual.to_csv(consolidated_stock_data_file_path, mode='a', index=False, header=False)


def process_file(file_path):
    directory_name, file_name = os.path.split(file_path)
    new_directory_name = directory_name + "/archive/"
    print(file_path,new_directory_name)
    preprocess_stock_data(file_path)
    append_to_consolidated_csv(file_path)
    # process_stock_data(pd.read_csv(file_path))
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
        files = [f for f in os.listdir(stock_data_dir) if f.endswith('.csv')]
        for file_name in files:
            file_path = os.path.join(stock_data_dir, file_name)
            process_file(file_path)

    process_files_task = PythonOperator(
        task_id='process_files',
        python_callable=process_files,
        provide_context=True,
    )

    process_files_task
