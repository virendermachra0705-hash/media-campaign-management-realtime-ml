import joblib
import time
import os
from fastapi import FastAPI, HTTPException, Response
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

from src.features.realtime_features import prepare_realtime_features

app = FastAPI(title="AdTech ROAS Inference API")

# Load encoders + model registry once
enc = joblib.load("src/models/encoders.pkl")
model_registry = enc.get("model_registry", {})

# default model
active_model = os.environ.get("ACTIVE_MODEL", "LightGBM")
if active_model not in model_registry:
    active_model = next(iter(model_registry.keys()))

model = joblib.load(f"src/models/{model_registry[active_model]}")

# Prometheus metrics
PRED_COUNTER = Counter("predictions_total", "Total predictions", ["model"])
PRED_LATENCY = Histogram("prediction_latency_seconds",
                         "Prediction latency seconds", ["model"])
PRED_VALUE = Histogram("predicted_roas_distribution", "Predicted ROAS distribution", buckets=(
    0.0, 0.001, 0.01, 0.1, 0.5, 1, 2, 5, 10, 50))
MODEL_GAUGE = Gauge(
    "active_model", "Active model numeric id (map in logs)", ["model_name"])

MODEL_GAUGE.labels(model_name=active_model).set(1)


@app.get("/health")
def health():
    return {"status": "running", "active_model": active_model}


@app.post("/predict")
def predict(event: dict):
    """
    Accept a generic JSON object (dict), convert to features and predict.
    Using dict avoids pydantic RootModel issues across versions.
    """
    payload = event
    start = time.time()
    try:
        x = prepare_realtime_features(payload)
        pred = float(model.predict(x)[0])
        pred = max(0.0, pred)  # business: don't return negative ROAS
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    latency = time.time() - start

    PRED_COUNTER.labels(model=active_model).inc()
    PRED_LATENCY.labels(model=active_model).observe(latency)
    try:
        PRED_VALUE.observe(pred)
    except Exception:
        pass

    return {"predicted_roas": pred, "model": active_model, "latency_s": latency}


@app.post("/select_model")
def select_model(model_name: str):
    global model, active_model
    if model_name not in model_registry:
        raise HTTPException(
            status_code=400, detail=f"Invalid model. Choose from {list(model_registry.keys())}")
    model = joblib.load(f"src/models/{model_registry[model_name]}")
    active_model = model_name
    MODEL_GAUGE.labels(model_name=active_model).set(1)
    return {"message": f"Switched to model: {model_name}"}


@app.get("/metrics")
def metrics():
    # Prometheus scraping endpoint
    resp = generate_latest()
    return Response(content=resp, media_type=CONTENT_TYPE_LATEST)
