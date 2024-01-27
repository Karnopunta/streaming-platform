import time
import json
import random

from datetime import datetime

from kafka import KafkaProducer

def get_json_data():

    stock = {
        'event_time': datetime.now().isoformat(),
        'ticker': random.choice(['AAPL', 'AMZN', 'MSFT', 'INTC', 'TBV']),
        'price': round(random.random() * 100, 2)
    }
    return json.dumps(stock) 

def main():
    producer = KafkaProducer(bootstrap_servers=['localhost:19092'])

    for _ in range(20000):
        json_data = get_json_data()
        producer.send("stock_json_topic_spark", bytes(f'{json_data}','UTF-8'))
        print(f"Data is sent: {json_data}")
        time.sleep(1)


if __name__ == "__main__":
    main()