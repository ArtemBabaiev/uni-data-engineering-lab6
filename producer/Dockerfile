FROM python:3.12.7-alpine
RUN pip install kafka-python
COPY . /app
WORKDIR /app
ENV CSV_PATH="/app/data/Divvy_Trips_2019_Q4.csv"
CMD ["python3","-u","/app/producer.py"]