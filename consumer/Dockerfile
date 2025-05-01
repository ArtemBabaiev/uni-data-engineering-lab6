FROM python:3.12-alpine
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
CMD ["python3","-u","/app/consumer.py"]