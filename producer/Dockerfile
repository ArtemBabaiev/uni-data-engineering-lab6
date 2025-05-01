FROM python:3.12.7-alpine
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
ENV CSV_PATH="/app/data/Divvy_Trips_2019_Q4.csv"
CMD ["python3","-u","/app/producer.py"]