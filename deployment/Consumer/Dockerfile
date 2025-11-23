FROM python:3.10-slim

WORKDIR /app

# Install LightGBM dependency
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src /app/src
COPY data /app/data

CMD ["python", "-m", "src.streaming.kafka_consumer"]
