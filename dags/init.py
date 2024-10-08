from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

import functools
from datetime import datetime
from setup.util_functions.load_file import load_csv_files
from setup.util_functions.HW_util import insert_HW_value,get_HW_value


with DAG('setup', start_date=datetime(2024, 1, 1), 
    schedule_interval=None , catchup=False) as dag:

    # create_table= PostgresOperator(
    #     task_id="create_table",
    #     postgres_conn_id='postgres',
    #     sql='''
    #         CREATE TABLE IF NOT EXISTS STOCK_DATA_INIT (
    #             id SERIAL PRIMARY KEY,
    #             date DATE NOT NULL,
    #             open DOUBLE PRECISION NOT NULL,
    #             high DOUBLE PRECISION NOT NULL,
    #             low DOUBLE PRECISION NOT NULL,
    #             close DOUBLE PRECISION NOT NULL,
    #             adj_close DOUBLE PRECISION NOT NULL,
    #             volume BIGINT NOT NULL,
    #             file_name VARCHAR(255) NOT NULL
    #         );
    #         '''
    # )


    # create_HW_table= PostgresOperator(
    #         task_id="create_table_HW",
    #         postgres_conn_id='postgres',
    #         sql='''
    #                 CREATE TABLE IF NOT EXISTS STOCK_DATA_HW (
    #                     id SERIAL PRIMARY KEY,
    #                     date DATE NOT NULL
    #                 );
    #             '''
    #     )

    

    create_stock_insight_table= PostgresOperator(
            task_id="create_stocks_insight_table",
            postgres_conn_id='postgres',
            sql='''
                    CREATE TABLE stock_annual_insights (
                    stock_symbol VARCHAR(10) NOT NULL,
                    year INTEGER NOT NULL,
                    annual_avg_price NUMERIC(10, 4),
                    annual_volatility NUMERIC(10, 6),
                    annual_max_price NUMERIC(10, 4),
                    annual_min_price NUMERIC(10, 4),
                    price_range NUMERIC(10, 4),
                    avg_volume BIGINT,
                    days_price_up INTEGER,
                    days_price_down INTEGER,
                    annual_cumulative_return NUMERIC(10, 6),
                    annual_end_performance NUMERIC(10, 6),
                    last_traded_date DATE,
                    PRIMARY KEY (stock_symbol, year)
                );

                '''
        )
    
    create_predicted_stock_insight_table= PostgresOperator(
            task_id="stock_annual_predicted_insights_table",
            postgres_conn_id='postgres',
            sql='''
                    CREATE TABLE stock_annual_predicted_insights (
                    id SERIAL PRIMARY KEY,
                    stock_symbol VARCHAR(10) NOT NULL,
                    predicted_year INTEGER NOT NULL,
                    predicted_annual_cumulative_return NUMERIC
                );
                '''
        )
    
     
    # insert_HW_value = PythonOperator(
    #     task_id='insert_HW_value',
    #     python_callable=insert_HW_value,
    #     op_kwargs={'table_name':"STOCK_DATA_HW", 'value':'1972-01-01','update_flag':False},
    #     provide_context=True,
    # )
    

    # load_csv_files = PythonOperator(
    #     task_id='load_files_task',
    #     python_callable=load_csv_files, 
    #     op_kwargs={'path':'dags/data', 'table_name': "stock_data_init"},
    #     provide_context=True,
    # )

    
    get_HW_value = PythonOperator(
        task_id='get_HW_value',
        python_callable=get_HW_value,
        op_kwargs={'table_name': 'STOCK_DATA_HW'},
        provide_context=True,
    )
    
   
 
    
    # create_table >> create_HW_table >> create_stock_insight_table >> [load_csv_files , insert_HW_value] >> get_HW_value  >> create_table_moving_average
    get_HW_value 

    