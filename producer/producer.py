import json
import os
import time
import csv

from kafka import KafkaProducer

TOPICS = os.environ.get('TOPICS', 'Topic1,Topic2').split(',')
BOOTSTRAP_SERVERS = os.environ.get('BOOTSTRAP_SERVERS', 'localhost:9091,localhost:9092').split(',')
CSV_PATH = os.environ.get('CSV_PATH', './data/Divvy_Trips_2019_Q4.csv')
ENTRIES_LIMIT = int(os.environ.get('ENTRIES_LIMIT', '100'))

print("Starting producer...")
print(f"TOPICS: {TOPICS}")
print(f"BOOTSTRAP_SERVERS: {BOOTSTRAP_SERVERS}")

def get_producer():
    try:
        kafka_producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        return kafka_producer
    except Exception as e:
        print(e)
        return None


producer=None

while producer is None:
    print('brokers are not available yet')
    time.sleep(5)
    producer = get_producer()

print("Sending messages...")

with open(CSV_PATH, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for idx, row in enumerate(reader):
        time.sleep(2)
        if idx >= ENTRIES_LIMIT:
            break
        print(f"Sending message {idx}...")
        for topic in TOPICS:
            producer.send(topic, row)


producer.close()