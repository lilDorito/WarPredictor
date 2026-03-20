import pandas as pd
import numpy as np
import joblib
import json
import os
from datetime import datetime, timezone

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
MODELS = os.path.join(ROOT, "models")
FEATURES = os.path.join(ROOT, "datasets", "features.csv")
OUTPUT = os.path.join(ROOT, "predictions", "prediction.json")

TARGET_HOURS = list(range(6, 30))

print("> Loading model & region mapping...\n")
model = joblib.load(os.path.join(MODELS, "lgb_multioutput.pkl"))
region_mapping = joblib.load(os.path.join(MODELS, "region_mapping.pkl"))
inverse_mapping = {v: k for k, v in region_mapping.items()}
print(f"[i] Model loaded, {len(region_mapping)} regions\n")

print("> Loading features...\n")
df = pd.read_csv(FEATURES)
df['timestamp_hour'] = pd.to_datetime(df['timestamp_hour'])

target_cols = [c for c in df.columns if 'target_' in c]
df = df.copy()
df['region_encoded'] = df['region_id'].map(inverse_mapping).fillna(-1)

latest = (
    df.sort_values('timestamp_hour')
    .groupby('region_id')
    .last()
    .reset_index()
)

base_time = latest['timestamp_hour'].max()
print(f"[i] Latest timestamp: {base_time}")
print(f"[i] Regions: {len(latest)}\n")

X_pred = latest.drop(columns=target_cols + ['timestamp_hour', 'region_id'], errors='ignore').astype(np.float32)
regions = latest['region_id'].tolist()

print("> Predicting...\n")
probas = np.array([est.predict_proba(X_pred)[:, 1] for est in model.estimators_])

global_max = probas.max()
normalized = (probas / global_max) * 0.99

scores = {}
probabilities = {}

for region_idx, region in enumerate(regions):
    scores[region] = {}
    probabilities[region] = {}
    for hour_idx, h in enumerate(TARGET_HOURS):
        timestamp = (base_time + pd.Timedelta(hours=h)).strftime("%Y-%m-%dT%H:00:00Z")
        scores[region][timestamp] = round(float(probas[hour_idx, region_idx]), 4)
        probabilities[region][timestamp] = round(float(normalized[hour_idx, region_idx]), 4)

output = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "base_time": base_time.isoformat(),
    "n_regions": len(regions),
    "n_hours": len(TARGET_HOURS),
    "global_max_score": round(float(global_max), 4),
    "scores": scores,
    "probabilities": probabilities,
}

with open(OUTPUT, 'w') as f:
    json.dump(output, f, indent=2)

print(f"[+] Saved predictions -> predictions/prediction.json")
print(f"    {len(regions)} regions x {len(TARGET_HOURS)} hours")
print(f"    Base time: {output['base_time']}")
print(f"    Global max score: {output['global_max_score']}")