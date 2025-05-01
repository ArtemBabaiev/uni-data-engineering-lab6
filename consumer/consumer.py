import json
import os
import time
import minio

from kafka import KafkaConsumer

TOPIC = os.environ.get('TOPIC', 'Topic1')
BOOTSTRAP_SERVERS = os.environ.get('BOOTSTRAP_SERVERS', 'localhost:9091,localhost:9092').split(',')

print("Starting consumer...")
print(f"TOPIC: {TOPIC}")
print(f"BOOTSTRAP_SERVERS: {BOOTSTRAP_SERVERS}")

def get_consumer():
    try:
        kafka_consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        return kafka_consumer
    except Exception as e:
        print(e)
        return None


consumer = None

while consumer is None:
    print('brokers are not available yet')
    time.sleep(5)
    consumer = get_consumer()

print("Receiving messages")

for message in consumer:
    data = message.value
    print(data)
