import csv
import json
from confluent_kafka import Consumer, KafkaException, KafkaError
from config import TOPIC

def consume_and_write_to_csv(csv_file):
    # Configure the Consumer
    c = Consumer({
        'bootstrap.servers': 'localhost:19092',  # Assuming you're running this on the same machine as the compose
        'group.id': 'python-consumer',
        'auto.offset.reset': 'latest'
    })

    # Subscribe to the topic
    c.subscribe([TOPIC])

    # Process messages
    try:
        with open(csv_file, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)

            # Write CSV header
            csv_writer.writerow(['event_time', 'ticker', 'price'])  # Adjust based on your actual column names

            while True:
                msg = c.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        print('%% %s [%d] reached end at offset %d\n' %
                            (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    # Parse JSON message
                    data = json.loads(msg.value().decode('utf-8'))
                    
                    # Write data to CSV
                    csv_writer.writerow([data['event_time'], data['ticker'], data['price']])
                    
                    print('Received message: {}'.format(msg.value().decode('utf-8')))
    except KeyboardInterrupt:
        pass
    finally:
        # Close down consumer to commit final offsets.
        c.close()

def main():
    csv_file = 'output.csv'  # Adjust the filename as needed
    consume_and_write_to_csv(csv_file)

if __name__ == '__main__':
    main()
