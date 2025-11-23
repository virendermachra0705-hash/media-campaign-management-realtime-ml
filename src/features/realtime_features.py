import pandas as pd
import joblib
import numpy as np


def prepare_realtime_features(event: dict):
    """
    Cleans + encodes + formats a single event for real-time prediction.
    """
    enc = joblib.load("src/models/encoders.pkl")

    df = pd.DataFrame([event])

    # Frequency-encoded ID-like columns
    for col, freq_map in enc["freq_maps"].items():
        df[col] = df[col].map(freq_map).fillna(0) if col in df else 0

    # Label-encoded low-cardinality columns
    for col, le in enc["label_encoders"].items():
        if col in df:
            val = str(df.iloc[0][col])
            df[col] = le.transform([val])[0] if val in le.classes_ else 0
        else:
            df[col] = 0

    # Numeric columns — fill missing with 0
    for col in enc["num_cols"]:
        if col not in df:
            df[col] = 0

    # Correct column order
    df = df[enc["feature_columns"]].astype(float)

    return df
