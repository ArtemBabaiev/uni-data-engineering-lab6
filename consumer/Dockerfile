FROM python:3.12-alpine
RUN pip install kafka-python
COPY . /app
WORKDIR /app
CMD ["python3","-u","/app/consumer.py"]