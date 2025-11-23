from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi import FastAPI, Response

consumer_app = FastAPI(title="Consumer Metrics")

EVENTS_CONSUMED = Counter("events_consumed_total", "Total events consumed")
CONSUME_LATENCY = Histogram(
    "consume_latency_seconds", "Latency for processing events")


@consumer_app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
