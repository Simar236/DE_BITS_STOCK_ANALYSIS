from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta
from kafka import KafkaConsumer
import os

STOCK_DATA_DIR = 'dags/stock_yearly_data_all'
def consume_and_save_to_file():
    topic = 'test_stock_topic'
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['kafka:9092'], 
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest', 
        group_id='kafka_consumer_group'
    )

    
    for message in consumer:
        stock_data = message.value
        date_str = stock_data.get('date')  
        if date_str:
            
            year = datetime.strptime(date_str, '%Y-%m-%d').year
            file_name = f"stock_data_{year}.csv"
            # file_name = f"consolidated_stock_data.csv"
            file_path = os.path.join(STOCK_DATA_DIR, file_name)
            file_exists = os.path.isfile(file_path)
            with open(file_path, 'a') as f:
                if not file_exists:
                    # write header
                    header = ','.join(stock_data.keys()) + '\n'
                    f.write(header)
                #write stock data
                data_line = ','.join(str(value) for value in stock_data.values()) + '\n'
                f.write(data_line)

        else:
            print("No valid date found in the message.")
    print("done!")
    consumer.close()


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
    'kafka_stock_consumer_dag',
    default_args=default_args,
    description='DAG to consume Kafka stock data and save to year-wise files',
    schedule_interval=None, 
    catchup=False,
) as dag:

    
    consume_task = PythonOperator(
        task_id='consume_and_save_to_file',
        python_callable=consume_and_save_to_file,
        provide_context=True,
    )

    consume_task
