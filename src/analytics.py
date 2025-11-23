# src/analytics/build_analytics.py
import pandas as pd
import os

os.makedirs("data/analytics", exist_ok=True)

# read raw BI stream
bi_file = "data/bi/live_predictions.csv"
if not os.path.exists(bi_file):
    print("No BI file found yet.")
    exit(0)

df = pd.read_csv(bi_file, parse_dates=["timestamp"] if "timestamp" in pd.read_csv(
    bi_file, nrows=1).columns else None)

# ensure timestamp exists or use current time
if "timestamp" not in df.columns:
    df["timestamp"] = pd.to_datetime("now")

df["hour"] = pd.to_datetime(df["timestamp"]).dt.floor("H")
df["date"] = pd.to_datetime(df["timestamp"]).dt.date

# Campaign-level hourly KPIs
hourly = df.groupby(["hour", "campaign_id"]).agg(
    impressions=("impressions", "sum"),
    clicks=("clicks", "sum"),
    conversions=("conversions", "sum"),
    spend=("spend", "sum"),
    revenue_predicted=("predicted_roas", lambda s: (
        s * df.loc[s.index, "spend"]).sum()),  # approximate revenue predicted
).reset_index()

hourly["pred_roas"] = hourly.apply(lambda r: (
    r["revenue_predicted"] / r["spend"]) if r["spend"] > 0 else 0, axis=1)

hourly.to_csv("data/analytics/hourly_campaign_kpi.csv", index=False)

# Daily KPIs
daily = hourly.copy()
daily["date"] = daily["hour"].dt.date
daily = daily.groupby(["date", "campaign_id"]).agg({
    "impressions": "sum", "clicks": "sum", "conversions": "sum", "spend": "sum", "revenue_predicted": "sum"
}).reset_index()
daily["pred_roas"] = daily.apply(lambda r: (
    r["revenue_predicted"] / r["spend"]) if r["spend"] > 0 else 0, axis=1)
daily.to_csv("data/analytics/daily_campaign_kpi.csv", index=False)

print("Analytics files written to data/analytics/")
