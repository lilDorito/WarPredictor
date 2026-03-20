import os, warnings
import numpy as np
import pandas as pd
import lightgbm as lgb
import joblib
from datetime import datetime
from sklearn.multioutput import MultiOutputClassifier
from sklearn.metrics import roc_auc_score, f1_score, precision_score, recall_score
from tqdm import tqdm

warnings.filterwarnings('ignore')

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
MODELS = os.path.join(ROOT, "models")
LOG = os.path.join(ROOT, "logs", "train", "train.log")
os.makedirs(MODELS, exist_ok=True); os.makedirs(os.path.dirname(LOG), exist_ok=True)

def log(msg):
    line = f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}"
    print(line)
    with open(LOG, "a") as f: f.write(line + "\n")

log("Training starting...")
df = pd.read_csv(os.path.join(ROOT, "datasets", "features.csv"))
df['timestamp_hour'] = pd.to_datetime(df['timestamp_hour'])
df = df.sort_values('timestamp_hour')

cutoff = df['timestamp_hour'].max() - pd.DateOffset(months=18)
df = df[df['timestamp_hour'] >= cutoff].reset_index(drop=True)
log(f"Period: {cutoff.date()} -> {df['timestamp_hour'].max().date()}")

df['region_encoded'] = df['region_id'].astype('category').cat.codes
joblib.dump(dict(zip(df['region_encoded'], df['region_id'])), os.path.join(MODELS, "region_mapping.pkl"))

target_hours = list(range(6, 30))
target_cols = [f'target_alarm_t{h}' for h in target_hours]
drop_cols = [c for c in df.columns if 'target_' in c] + ['timestamp_hour', 'region_id']

X = df.drop(columns=drop_cols).astype(np.float32)
y = df[target_cols]
log(f"Shape: {X.shape[0]:,} × {X.shape[1]}, {len(target_hours)} targets")

holdout_mask = (df['timestamp_hour'] >= df['timestamp_hour'].max() - pd.DateOffset(days=30)).values
X_tr, y_tr = X[~holdout_mask], y[~holdout_mask]
X_ho, y_ho = X[holdout_mask], y[holdout_mask]
log(f"Train: {len(X_tr):,} | Holdout: {len(X_ho):,}")

PARAMS = dict(
    n_estimators=796,
    learning_rate=0.013,
    max_depth=10,
    num_leaves=82,
    subsample=0.9,
    colsample_bytree=0.534,
    min_child_samples=41,
    reg_alpha=0.0035,
    reg_lambda=0.0043,
    class_weight="balanced",
    random_state=42,
    n_jobs=-1,
    verbosity=-1,
)

N_TARGETS = len(target_hours)
N_TREES = PARAMS['n_estimators']

def fit_with_progress(X, y, label):
    pbar = tqdm(total=N_TREES * N_TARGETS, unit="tree", desc=label)

    def _callback(env):
        pbar.update(1)

    estimators = []
    for i in range(N_TARGETS):
        clf = lgb.LGBMClassifier(**PARAMS)
        clf.fit(X, y.iloc[:, i], callbacks=[_callback])
        estimators.append(clf)

    pbar.close()

    model = MultiOutputClassifier(lgb.LGBMClassifier(**PARAMS), n_jobs=1)
    model.estimators_ = estimators
    model.classes_ = [clf.classes_ for clf in estimators]
    return model

print()
eval_model = fit_with_progress(X_tr, y_tr, "Eval model")

probs_1 = np.column_stack([p[:, 1] for p in eval_model.predict_proba(X_ho)])

aucs = [roc_auc_score(y_ho.iloc[:, i], probs_1[:, i]) for i in range(N_TARGETS)]
log(f"AUC={np.mean(aucs):.3f} | min={np.min(aucs):.3f} | max={np.max(aucs):.3f}")

preds = (probs_1 >= 0.5).astype(int)
f1s = [f1_score(y_ho.iloc[:, i], preds[:, i], zero_division=0) for i in range(N_TARGETS)]
precs = [precision_score(y_ho.iloc[:, i], preds[:, i], zero_division=0) for i in range(N_TARGETS)]
recs = [recall_score(y_ho.iloc[:, i], preds[:, i], zero_division=0) for i in range(N_TARGETS)]
log(f"F1={np.mean(f1s):.3f} | Prec={np.mean(precs):.3f} | Rec={np.mean(recs):.3f}")

print()
final_model = fit_with_progress(X, y, "Final model")

joblib.dump(final_model, os.path.join(MODELS, "lgb_multioutput.pkl"))
log("Model saved. Done.")