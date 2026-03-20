import pandas as pd
import numpy as np
import xgboost as xgb
import lightgbm as lgb
from sklearn.multioutput import MultiOutputClassifier
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import f1_score, precision_score, recall_score
from tqdm import tqdm
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

target_cols = [c for c in df.columns if 'target_' in c]

df['region_encoded'] = df['region_id'].astype('category').cat.codes

X = df.drop(columns=target_cols + ['timestamp_hour', 'region_id'])
X = X.astype(np.float32)

target_hours = list(range(6, 30))
target_col_names = [f'target_alarm_t{h}' for h in target_hours]
y = df[target_col_names]

print(f"[i] Dataset: {X.shape[0]} rows, {X.shape[1]} features, {len(target_hours)} targets\n")

MODELS = {
    "LightGBM": MultiOutputClassifier(
        lgb.LGBMClassifier(
            n_estimators=1000,
            learning_rate=0.03,
            max_depth=8,
            num_leaves=63,
            subsample=0.8,
            colsample_bytree=0.7,
            min_child_samples=10,
            class_weight='balanced',
            n_jobs=-1,
            verbosity=-1,
        ),
        n_jobs=1
    ),
    "XGBoost": MultiOutputClassifier(
        xgb.XGBClassifier(
            n_estimators=1000,
            learning_rate=0.03,
            max_depth=8,
            subsample=0.8,
            colsample_bytree=0.7,
            min_child_weight=3,
            tree_method='hist',
            nthread=-1,
            verbosity=0
        ),
        n_jobs=1
    ),
}

tscv = TimeSeriesSplit(n_splits=3, gap=24*7)
leaderboard = []

for name, model in MODELS.items():
    print(f" > {name}:")
    start_time = time.time()

    fold_f1, fold_prec, fold_rec = [], [], []

    for fold, (train_idx, test_idx) in enumerate(tqdm(list(tscv.split(X)), desc=f"  Folds progress", unit="fold"), 1):
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]

        model.fit(X_train, y_train)
        preds = np.array(model.predict(X_test))

        hour_f1, hour_prec, hour_rec = [], [], []
        for i in range(len(target_hours)):
            hour_f1.append(f1_score(y_test.iloc[:, i], preds[:, i]))
            hour_prec.append(precision_score(y_test.iloc[:, i], preds[:, i]))
            hour_rec.append(recall_score(y_test.iloc[:, i], preds[:, i]))

        fold_f1.append(np.mean(hour_f1))
        fold_prec.append(np.mean(hour_prec))
        fold_rec.append(np.mean(hour_rec))

        print(f"  Fold {fold} | F1={np.mean(hour_f1):.3f} | Prec={np.mean(hour_prec):.3f} | Rec={np.mean(hour_rec):.3f}")

    duration = time.time() - start_time

    leaderboard.append({
        "Model": name,
        "F1": round(np.mean(fold_f1), 4),
        "Precision": round(np.mean(fold_prec), 4),
        "Recall": round(np.mean(fold_rec), 4),
        "Time": f"{duration:.1f}s"
    })
    print()

results_df = pd.DataFrame(leaderboard).sort_values('F1', ascending=False)
print("\n[i] Multioutput competition results:")
print(results_df.to_string(index=False))