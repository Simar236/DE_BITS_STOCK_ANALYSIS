from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

import functools
from datetime import datetime
from util_functions.load_file import load_csv_files
from util_functions.HW_util import insert_HW_value,get_HW_value


with DAG('setup', start_date=datetime(2024, 1, 1), 
    schedule_interval=None , catchup=False) as dag:

    create_table= PostgresOperator(
        task_id="create_table",
        postgres_conn_id='postgres',
        sql='''
CREATE TABLE IF NOT EXISTS STOCK_DATA_INIT (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    adj_close DOUBLE PRECISION NOT NULL,
    volume BIGINT NOT NULL,
    file_name VARCHAR(255) NOT NULL
);
'''
    )


    create_HW_table= PostgresOperator(
            task_id="create_table_HW",
            postgres_conn_id='postgres',
            sql='''
                    CREATE TABLE IF NOT EXISTS STOCK_DATA_HW (
                        id SERIAL PRIMARY KEY,
                        date DATE NOT NULL
                    );
                '''
        )

    # insert_HW_value = PythonOperator(
    #     task_id='insert_HW_value',
    #     python_callable=functools.partial(insert_HW_value, table_name="STOCK_DATA_HW", value='01-01-1972' ),  
        
    #     provide_context=True,
    # )
    
    get_HW_value = PythonOperator(
        task_id='get_HW_value',
        python_callable=get_HW_value,
        # op_kwargs={'table_name': 'STOCK_DATA_HW'},  # Pass the table name here
        provide_context=True,  # Needed to pass `kwargs` to the callable
    )
    

    load_csv_files = PythonOperator(
        task_id='load_files_task',
        python_callable=functools.partial(load_csv_files, path='dags/data', table_name="stock_data_init"), 
        
        provide_context=True,
    )
 
    # download_a = BashOperator(
    #     task_id='download_a',
    #     bash_command='sleep 10'
    # )
    # get_HW_value
    # create_HW_table >> insert_HW_value >> get_HW_value
    # create_table >> create_HW_table >> load_csv_files

    