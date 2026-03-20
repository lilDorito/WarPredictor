import pandas as pd
import numpy as np
import lightgbm as lgb
import optuna
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import f1_score
import warnings
import os

warnings.filterwarnings('ignore')
optuna.logging.set_verbosity(optuna.logging.WARNING)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
FEATURES = os.path.join(ROOT, "datasets", "features.csv")

print("> Loading features...\n")
df = pd.read_csv(FEATURES).sort_values('timestamp_hour')
df['timestamp_hour'] = pd.to_datetime(df['timestamp_hour'])

target_cols = [c for c in df.columns if 'target_' in c]

cutoff = df['timestamp_hour'].max() - pd.DateOffset(months=18)
df = df[df['timestamp_hour'] >= cutoff].reset_index(drop=True)
print(f"[i] Using data from {cutoff.date()} to {df['timestamp_hour'].max().date()}")
print(f"[i] Full dataset: {len(df)} rows")

df_tune = df.sample(frac=0.5, random_state=42).sort_values('timestamp_hour').reset_index(drop=True)
X_tune = df_tune.drop(columns=target_cols + ['timestamp_hour', 'region_id'])
X_tune = X_tune.astype(np.float32)
y_tune = df_tune['target_alarm_t12']
print(f"[i] Tuning dataset: {len(df_tune)} rows, {X_tune.shape[1]} features\n")

tscv = TimeSeriesSplit(n_splits=2, gap=24*7)

def objective(trial):
    params = {
        "n_estimators": trial.suggest_int("n_estimators", 200, 1500),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.1, log=True),
        "max_depth": trial.suggest_int("max_depth", 4, 10),
        "num_leaves": trial.suggest_int("num_leaves", 20, 150),
        "subsample": trial.suggest_float("subsample", 0.6, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
        "min_child_samples": trial.suggest_int("min_child_samples", 5, 50),
        "reg_alpha": trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
        "reg_lambda": trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
        "class_weight": "balanced",
        "n_jobs": -1,
        "verbosity": -1,
    }

    fold_f1 = []
    for train_idx, test_idx in tscv.split(X_tune):
        X_train, X_test = X_tune.iloc[train_idx], X_tune.iloc[test_idx]
        y_train, y_test = y_tune.iloc[train_idx], y_tune.iloc[test_idx]

        model = lgb.LGBMClassifier(**params)
        model.fit(X_train, y_train)
        preds = model.predict(X_test)
        fold_f1.append(f1_score(y_test, preds))

    return np.mean(fold_f1)

print("Starting Optuna search (50 trials, 50% data)...")
study = optuna.create_study(direction="maximize")
study.optimize(objective, n_trials=50, show_progress_bar=True)

print(f"\n[i] Best F1: {study.best_value:.4f}")
print(f"[i] Best params:")
for k, v in study.best_params.items():
    print(f"    {k}: {v}")