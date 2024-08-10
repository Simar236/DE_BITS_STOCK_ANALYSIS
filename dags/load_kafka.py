from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
# import functools
 
from datetime import datetime

from util_functions.k.producer import produce_data_kafka
from util_functions.HW_util import get_HW_value,get_end_date


def produce_kafka_data(table_name, **kwargs):
    value=kwargs['ti'].xcom_pull(key='HW_value')
    print(value)
    hook = PostgresHook(postgres_conn_id='postgres')
    connection = hook.get_conn()
    cursor = connection.cursor()
    query = f"SELECT * FROM {table_name} where date between '{value}' and '{get_end_date(value)}';"  
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    produce_data_kafka(result)

    
 
with DAG('produce_kafka', start_date=datetime(2024, 1, 1), 
    schedule_interval=None , catchup=False) as dag:

    
    get_HW_value = PythonOperator(
        task_id='get_HW_value',
        python_callable=get_HW_value,
        op_kwargs={'table_name': 'STOCK_DATA_HW'},
        provide_context=True,
    )

    produce_kafka_data = PythonOperator(
        task_id='produce_kafka_data',
        python_callable=produce_kafka_data,
        op_kwargs={'table_name': 'STOCK_DATA_INIT'},
        provide_context=True,
    )

    get_HW_value >> produce_kafka_data
    
    