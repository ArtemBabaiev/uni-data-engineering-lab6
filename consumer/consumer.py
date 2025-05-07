import json
import os
import time
from minio import Minio, S3Error
from datetime import datetime, timedelta
import threading

import csv

from kafka import KafkaConsumer

TOPIC = os.environ.get('TOPIC', 'Topic1')
BOOTSTRAP_SERVERS = os.environ.get('BOOTSTRAP_SERVERS', 'localhost:9091,localhost:9092').split(',')

MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'localhost:9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', 'admin')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', 'pass_admin')

BUCKET_NAME = os.environ.get('BUCKET_NAME', 'trips-bucket1')
WORK_DIR = os.environ.get('WORK_DIR', './workdir')

print("Starting consumer...")
print(f"TOPIC: {TOPIC}")
print(f"BOOTSTRAP_SERVERS: {BOOTSTRAP_SERVERS}")

consumer = None

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

current_month_year = None
csv_file = None
csv_writer = None

last_message_time = datetime.now()
idle_timeout = timedelta(minutes=2)


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

def upload_to_minio(csv_file_path):
    print(f"Uploading {csv_file_path}...")
    timestamp = int(time.time())  # UNIX timestamp as integer
    file_name = os.path.splitext(os.path.basename(csv_file_path))[0]
    new_object_name = f"{file_name}_{timestamp}.csv"
    minio_client.fput_object(
        BUCKET_NAME, new_object_name, csv_file_path,
    )
    print(f"Uploaded {csv_file_path} as {new_object_name} to MinIO.")

def open_file(month_year, init_data):
    print(f"Opening {month_year}...")
    path = os.path.join(WORK_DIR, f"{month_year}.csv")
    month_year_file = open(path, "a", newline='', encoding="utf-8")
    dict_writer = csv.DictWriter(month_year_file, fieldnames=init_data.keys())
    dict_writer.writeheader()
    dict_writer.writerow(init_data)
    month_year_file.flush()
    return month_year_file, dict_writer

def monitor_idle_time():
    global last_message_time, csv_file, current_month_year, csv_writer
    print("Monitoring idle time...")
    while True:
        time.sleep(30)  # Check every 30 seconds
        print("Checking idle time...")
        if current_month_year and (datetime.now() - last_message_time > idle_timeout):
            print("Idle timeout exceeded. Uploading file...")
            csv_file_path = os.path.join(WORK_DIR, f"{current_month_year}.csv")
            try:
                if csv_file:
                    csv_file.close()
                    csv_file = None
                upload_to_minio(csv_file_path)
                os.remove(csv_file_path)
                current_month_year = None
                csv_writer = None
            except Exception as e:
                print(f"Error during idle upload: {e}")

while consumer is None:
    print('brokers are not available yet')
    time.sleep(5)
    consumer = get_consumer()

if not minio_client.bucket_exists(BUCKET_NAME):
    minio_client.make_bucket(BUCKET_NAME)

os.makedirs(WORK_DIR, exist_ok=True)

print("Consumer started. Listening for messages...")

monitor_thread = threading.Thread(target=monitor_idle_time, daemon=True)
monitor_thread.start()

try:
    for message in consumer:
        last_message_time = datetime.now()
        data = message.value
        dt = datetime.strptime(data['start_time'], "%Y-%m-%d %H:%M:%S")
        new_month_year = f"{dt.strftime('%B').lower()}_{dt.year}"
        if current_month_year is None:
            current_month_year = new_month_year
            csv_file, csv_writer = open_file(new_month_year, data)
        elif current_month_year == new_month_year:
            csv_writer.writerow(data)
            csv_file.flush()
        else:
            previous_csv = os.path.join(WORK_DIR, f"{current_month_year}.csv")
            current_month_year = new_month_year
            csv_file.close()

            csv_file, csv_writer = open_file(new_month_year, data)

            try:
                upload_to_minio(previous_csv)
                os.remove(previous_csv)
            except Exception as e:
                print(f"Error uploading/deleting file {previous_csv}: {e}")


except KeyboardInterrupt:
    print("Consumer stopped.")

finally:
    if csv_file:
        csv_file.close()