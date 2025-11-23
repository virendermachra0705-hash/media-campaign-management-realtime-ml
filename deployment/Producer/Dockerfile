FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/streaming/kafka_producer.py /app/kafka_producer.py
COPY data /app/data

CMD ["python", "kafka_producer.py"]
