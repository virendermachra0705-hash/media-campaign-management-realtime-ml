import os
import json
import time
import pandas as pd
from confluent_kafka import Producer


# -------------------------------------------------------------------
# Load bootstrap server from environment (used inside Docker)
# -------------------------------------------------------------------
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

producer_conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "client.id": "media-campaign-producer"
}

producer = Producer(producer_conf)


# -------------------------------------------------------------------
# Load dataset
# -------------------------------------------------------------------
df = pd.read_csv("/app/data/processed/features1.csv")


# -------------------------------------------------------------------
# Configurations
# -------------------------------------------------------------------
BATCH_SIZE = 15
SLEEP_TIME = 120  # 2 minutes


# -------------------------------------------------------------------
# Delivery callback
# -------------------------------------------------------------------
def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✔ Delivered to {msg.topic()} [{msg.partition()}]")


print("🚀 Kafka Producer started — streaming in batches of 15 events every 2 minutes...")


# -------------------------------------------------------------------
# Streaming Loop
# -------------------------------------------------------------------
total_events = len(df)
batch_counter = 0

for start_idx in range(0, total_events, BATCH_SIZE):

    batch_counter += 1
    end_idx = min(start_idx + BATCH_SIZE, total_events)

    batch = df.iloc[start_idx:end_idx]

    print(f"\n📦 Sending Batch {batch_counter} ({len(batch)} events)")

    for idx, row in batch.iterrows():

        event = row.to_dict()

        producer.produce(
            topic="ad_events",
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report
        )

        producer.poll(0)  # Process delivery callbacks

    producer.flush()  # Ensure all messages delivered
    print(
        f"✅ Batch {batch_counter} delivered.\n⏳ Waiting 2 minutes before next batch...")

    time.sleep(SLEEP_TIME)

print("🎉 All data batches sent successfully!")
