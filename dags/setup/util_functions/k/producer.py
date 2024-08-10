import json
from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def produce_data_kafka(data):

    topic = "test_stock_topic"

    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=json_serializer
    )

    for record in data:
        json_record = {
            "date": record[1].isoformat(),  
            "open": record[2],
            "high": record[3],
            "low": record[4],
            "close": record[5],
            "volume": record[6],
            "stock_symbol": record[7]
        }
        producer.send(topic, value=json_record)
        
    producer.flush()
    producer.close()

