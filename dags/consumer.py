from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from kafka import KafkaConsumer
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)

def consume_data_kafka():
    topic = 'test_stock_topic'
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['kafka:9092'], 
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest', 
        group_id='kafka_consumer_group'
    )

    logging.info(f"Consuming messages from topic: {topic}")
    
    for message in consumer:
        logging.info(f"Received message: {message.value}")
        
        print(message.value)

    consumer.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 10),
    'retries': 1,
}

with DAG('consumer', start_date=datetime(2024, 1, 1), 
    schedule_interval=None , catchup=False) as dag:

    consume_task = PythonOperator(
        task_id='consume_kafka_data',
        python_callable=consume_data_kafka,
        dag=dag,
    )
    
    consume_task
