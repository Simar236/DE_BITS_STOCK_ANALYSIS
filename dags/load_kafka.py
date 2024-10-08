from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
 
from datetime import datetime

from setup.util_functions.kafka_util.producer import produce_data_kafka
from setup.util_functions.HW_util import get_HW_value,get_end_date, insert_HW_value


def produce_kafka_data(table_name, **kwargs):
    value=kwargs['ti'].xcom_pull(key='HW_value')
    new_HW_value=get_end_date(value)
    print(value,new_HW_value)
    hook = PostgresHook(postgres_conn_id='postgres')
    connection = hook.get_conn()
    cursor = connection.cursor()
    query = f"SELECT * FROM {table_name} where date between '{value}' and '{new_HW_value}' order by date;"  
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    connection.close()
      
    print(f"Number of records fetched: {len(result)}")

    if result:
        produce_data_kafka(result)
        print("Data successfully pushed to Kafka.")

    value=kwargs['ti'].xcom_push(key='new_HW_value', value=new_HW_value)

    
#   schedule_interval='0 */2 * * *'
with DAG('produce_kafka', start_date=datetime(2024, 1, 1), 
    schedule_interval='* * * * *', catchup=False) as dag:

    
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

    update_HW_value = PythonOperator(
        task_id='update_HW_value',
        python_callable=insert_HW_value,
        op_kwargs={'table_name':"STOCK_DATA_HW", 
                   'value': '{{ task_instance.xcom_pull(task_ids="produce_kafka_data", key="new_HW_value") }}',
                   'update_flag':True},
        provide_context=True,
    )

    get_HW_value >> produce_kafka_data >> update_HW_value
    
    