import json
import joblib
import pandas as pd
import os
import time
from confluent_kafka import Consumer
from src.features.realtime_features import prepare_realtime_features

# Prometheus
from prometheus_client import start_http_server, Counter, Histogram, Gauge

# Load encoders + model registry
enc = joblib.load("src/models/encoders.pkl")
model_registry = enc.get("model_registry", {})

active_model = os.environ.get("CONSUMER_MODEL", "LightGBM")
if active_model not in model_registry:
    active_model = next(iter(model_registry.keys()))

model = joblib.load(f"src/models/{model_registry[active_model]}")

# prepare folders
os.makedirs("logs", exist_ok=True)
os.makedirs("data/bi", exist_ok=True)

log_file = "logs/predictions.log"
bi_file = "data/bi/live_predictions.csv"

# Prometheus metrics (exposed on a separate HTTP port)
# expose metrics on all interfaces so Prometheus can scrape
start_http_server(8001, addr="0.0.0.0")
PRED_COUNTER = Counter("consumer_predictions_total",
                       "Total predictions from consumer", ["model"])
PRED_LATENCY = Histogram("consumer_prediction_latency_seconds",
                         "Consumer prediction latency seconds", ["model"])
PRED_VALUE = Histogram("consumer_predicted_roas",
                       "Predicted ROAS distribution")
LAST_PRED = Gauge("consumer_last_prediction", "Last predicted ROAS", ["model"])
PROCESSED_EVENTS = Counter(
    "consumer_events_processed_total", "Events processed", ["model"])


def log_prediction(event, pred):
    with open(log_file, "a") as f:
        f.write(json.dumps(
            {"ts": time.time(), "event": event, "pred": pred}) + "\n")


# Kafka consumer config
conf = {
    'bootstrap.servers': os.environ.get("KAFKA_BOOT", "kafka:9092"),

    'group.id': os.environ.get("KAFKA_GROUP", "campaign_group"),
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(["ad_events"])

print("Consumer started, waiting for messages...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error:", msg.error())
            continue
        try:
            event = json.loads(msg.value().decode("utf-8"))
        except Exception as e:
            print("Decode error:", e)
            continue

        start = time.time()
        try:
            x = prepare_realtime_features(event)
            pred = float(model.predict(x)[0])
            pred = max(0.0, pred)  # business floor
        except Exception as e:
            print("Model predict error:", e)
            continue

        latency = time.time() - start

        # metrics
        PRED_COUNTER.labels(model=active_model).inc()
        PRED_LATENCY.labels(model=active_model).observe(latency)
        try:
            PRED_VALUE.observe(pred)
        except Exception:
            pass
        LAST_PRED.labels(model=active_model).set(pred)
        PROCESSED_EVENTS.labels(model=active_model).inc()

        # log + BI CSV
        log_prediction(event, pred)
        out = event.copy()
        out["predicted_roas"] = pred
        pd.DataFrame([out]).to_csv(bi_file, mode="a",
                                   header=not os.path.exists(bi_file), index=False)

        print(f"PRED: {pred:.6f} (latency: {latency:.3f}s)")
except KeyboardInterrupt:
    print("Shutting down consumer")
finally:
    consumer.close()
