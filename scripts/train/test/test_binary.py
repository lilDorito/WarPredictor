import pandas as pd
import numpy as np
import xgboost as xgb
import lightgbm as lgb

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

cutoff = df['timestamp_hour'].max() - pd.DateOffset(months=18)
df = df[df['timestamp_hour'] >= cutoff].reset_index(drop=True)
print(f"[i] Using data from {cutoff.date()} to {df['timestamp_hour'].max().date()}")

df['region_encoded'] = df['region_id'].astype('category').cat.codes
print(f"[i] Regions encoded: {df['region_id'].nunique()} unique")

target_cols = [c for c in df.columns if 'target_' in c]

X = df.drop(columns=target_cols + ['timestamp_hour', 'region_id'])

target_cols_predict = [f'target_alarm_t{h}' for h in range(6, 30)]
y = (df[target_cols_predict].sum(axis=1) > 0).astype(int)

X = X.astype(np.float32)

print(f"[i] Dataset: {X.shape[0]} rows, {X.shape[1]} features")

neg = (y == 0).sum()
pos = (y == 1).sum()
ratio = neg / pos
print(f"[i] Class balance: {pos} positive, {neg} negative | scale_pos_weight: {ratio:.2f}\n")

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

def get_models(ratio):
    return {
        "LinearRegression": LinearClassifier(),

        "LogisticRegression": LogisticRegression(
            solver='saga',
            max_iter=200,
            tol=1e-2,
            n_jobs=-1
        ),

        "LightGBM": lgb.LGBMClassifier(
            n_estimators=1000,
            learning_rate=0.03,
            max_depth=8,
            num_leaves=63,
            subsample=0.8,
            colsample_bytree=0.7,
            min_child_samples=10,
            class_weight='balanced',
            n_jobs=-1,
            verbosity=-1
        ),

        "XGBoost": xgb.XGBClassifier(
            n_estimators=1000,
            learning_rate=0.03,
            max_depth=8,
            subsample=0.8,
            colsample_bytree=0.7,
            min_child_weight=3,
            scale_pos_weight=ratio,
            tree_method='hist',
            nthread=-1,
            verbosity=0
        )
    }

leaderboard = []

for name in get_models(ratio).keys():
    print(f" > Training {name}...")
    start_time = time.time()

    fold_f1, fold_prec, fold_rec = [], [], []

    for fold, (train_idx, test_idx) in enumerate(tscv.split(X), 1):

        model = get_models(ratio)[name]

        if name in ["LogisticRegression", "LinearRegression"]:
            X_train = X_scaled[train_idx]
            X_test = X_scaled[test_idx]
        else:
            X_train = X.iloc[train_idx]
            X_test = X.iloc[test_idx]

        y_train = y.iloc[train_idx]
        y_test = y.iloc[test_idx]

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
        "F1-Score": round(np.mean(fold_f1), 4),
        "Precision": round(np.mean(fold_prec), 4),
        "Recall": round(np.mean(fold_rec), 4),
        "Time": f"{duration:.2f}s"
    })

    print()

results_df = pd.DataFrame(leaderboard).sort_values(by="F1-Score", ascending=False)

print("\n[i] Binary prediction competition results:")
print(results_df.to_string(index=False))