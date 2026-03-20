import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import f1_score, precision_score, recall_score
from sklearn.preprocessing import StandardScaler
import joblib, warnings, os

warnings.filterwarnings('ignore')

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
FEATURES = os.path.join(ROOT, "datasets", "features.csv")
MODELS = os.path.join(ROOT, "models")
os.makedirs(MODELS, exist_ok=True)

df = pd.read_csv(FEATURES).sort_values('timestamp_hour')
df['timestamp_hour'] = pd.to_datetime(df['timestamp_hour'])
target_cols = [c for c in df.columns if 'target_' in c]

cutoff = df['timestamp_hour'].max() - pd.DateOffset(months=18)
df = df[df['timestamp_hour'] >= cutoff].reset_index(drop=True)
print(f"[i] Period: {cutoff.date()} → {df['timestamp_hour'].max().date()}")

df['region_encoded'] = df['region_id'].astype('category').cat.codes
joblib.dump(dict(zip(df['region_encoded'], df['region_id'])), os.path.join(MODELS, "region_mapping.pkl"))

target_hours = list(range(6, 30))
target_cols  = [f'target_alarm_t{h}' for h in target_hours]

X = df.drop(columns=[c for c in df.columns if 'target_' in c] + ['timestamp_hour', 'region_id']).astype(np.float32)
y = df[target_cols]
print(f"[i] {X.shape[0]:,} rows × {X.shape[1]} features, {len(target_cols)} targets\n")

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)
tscv = TimeSeriesSplit(n_splits=3, gap=24*7)

all_f1, all_prec, all_rec = [], [], []

for i, col in enumerate(target_cols):
    yi = y[col]
    fold_f1, fold_prec, fold_rec = [], [], []

    for tr, te in tscv.split(X_scaled):
        model = LinearRegression()
        model.fit(X_scaled[tr], yi.iloc[tr])
        preds = (model.predict(X_scaled[te]) > 0.5).astype(int)
        fold_f1.append(f1_score(yi.iloc[te], preds, zero_division=0))
        fold_prec.append(precision_score(yi.iloc[te], preds, zero_division=0))
        fold_rec.append(recall_score(yi.iloc[te], preds, zero_division=0))

    all_f1.append(np.mean(fold_f1))
    all_prec.append(np.mean(fold_prec))
    all_rec.append(np.mean(fold_rec))
    print(f"  t+{target_hours[i]:02d}  F1={all_f1[-1]:.3f} | Prec={all_prec[-1]:.3f} | Rec={all_rec[-1]:.3f}")

print(f"\n[+] LinearRegression: F1={np.mean(all_f1):.3f} | Prec={np.mean(all_prec):.3f} | Rec={np.mean(all_rec):.3f}")

estimators = []
for col in target_cols:
    m = LinearRegression()
    m.fit(X_scaled, y[col])
    estimators.append(m)

joblib.dump(estimators, os.path.join(MODELS, "linear_regression.pkl"))
joblib.dump(scaler, os.path.join(MODELS, "linear_regression_scaler.pkl"))
print(f"[+] Saved -> models/linear_regression.pkl")