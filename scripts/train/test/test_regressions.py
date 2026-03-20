import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import f1_score, precision_score, recall_score
from sklearn.preprocessing import StandardScaler
import time
import warnings
import os

warnings.filterwarnings('ignore')

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
FEATURES = os.path.join(ROOT, "datasets", "features.csv")

print("> Loading features...\n")
df = pd.read_csv(FEATURES).sort_values('timestamp_hour')
df['timestamp_hour'] = pd.to_datetime(df['timestamp_hour'])

target_cols = [c for c in df.columns if 'target_' in c]

cutoff = df['timestamp_hour'].max() - pd.DateOffset(months=18)
df = df[df['timestamp_hour'] >= cutoff].reset_index(drop=True)
print(f"[i] Using data from {cutoff.date()} to {df['timestamp_hour'].max().date()}")

df['region_encoded'] = df['region_id'].astype('category').cat.codes
print(f"[i] Regions encoded: {df['region_id'].nunique()} unique")

X = df.drop(columns=target_cols + ['timestamp_hour', 'region_id'])
X = X.astype(np.float32)

y_single = df['target_alarm_t12']

print(f"[i] Dataset: {X.shape[0]} rows, {X.shape[1]} features\n")

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

tscv = TimeSeriesSplit(n_splits=3, gap=24*7)

class LinearClassifier:
    def __init__(self):
        self.model = LinearRegression()

    def fit(self, X, y):
        self.model.fit(X, y)

    def predict(self, X):
        return (self.model.predict(X) > 0.5).astype(int)

def get_models():
    return {
        "LinearRegression": LinearClassifier(),
        "LogisticRegression": LogisticRegression(
            solver='saga',
            max_iter=200,
            tol=1e-2,
            n_jobs=-1
        ),
    }

leaderboard = []

for name in get_models().keys():
    print(f" > Training {name} on t12...")
    start_time = time.time()

    fold_f1, fold_prec, fold_rec = [], [], []

    for fold, (train_idx, test_idx) in enumerate(tscv.split(X), 1):
        model = get_models()[name]

        X_train = X_scaled[train_idx]
        X_test = X_scaled[test_idx]
        y_train = y_single.iloc[train_idx]
        y_test = y_single.iloc[test_idx]

        model.fit(X_train, y_train)
        preds = model.predict(X_test)

        f1 = f1_score(y_test, preds)
        prec = precision_score(y_test, preds)
        rec = recall_score(y_test, preds)

        fold_f1.append(f1)
        fold_prec.append(prec)
        fold_rec.append(rec)

        print(f"   Fold {fold}: F1={f1:.3f} | Precision={prec:.3f} | Recall={rec:.3f}")

    duration = time.time() - start_time

    leaderboard.append({
        "Model": name,
        "F1": round(np.mean(fold_f1), 4),
        "Precision": round(np.mean(fold_prec), 4),
        "Recall": round(np.mean(fold_rec), 4),
        "Time": f"{duration:.2f}s"
    })

    print()

results_df = pd.DataFrame(leaderboard).sort_values(by="F1", ascending=False)
print("\n[i] Regression competition results (t12):")
print(results_df.to_string(index=False))