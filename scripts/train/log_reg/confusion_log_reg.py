import os, warnings
import numpy as np
import pandas as pd
import joblib
import matplotlib.pyplot as plt
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay

warnings.filterwarnings('ignore')

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
MODELS = os.path.join(ROOT, "models")
PLOTS = os.path.join(ROOT, "plots")
os.makedirs(PLOTS, exist_ok=True)

estimators = joblib.load(os.path.join(MODELS, "logistic_regression.pkl"))
scaler = joblib.load(os.path.join(MODELS, "logistic_regression_scaler.pkl"))

df = pd.read_csv(os.path.join(ROOT, "datasets", "features.csv"))
df['timestamp_hour'] = pd.to_datetime(df['timestamp_hour'])
df = df.sort_values('timestamp_hour')

cutoff = df['timestamp_hour'].max() - pd.DateOffset(months=18)
df = df[df['timestamp_hour'] >= cutoff].reset_index(drop=True)

df['region_encoded'] = df['region_id'].astype('category').cat.codes

target_hours = list(range(6, 30))
target_cols = [f'target_alarm_t{h}' for h in target_hours]
drop_cols = [c for c in df.columns if 'target_' in c] + ['timestamp_hour', 'region_id']

X = df.drop(columns=drop_cols).astype(np.float32)
X = X[list(scaler.feature_names_in_)]
y = df[target_cols]

holdout_mask = (df['timestamp_hour'] >= df['timestamp_hour'].max() - pd.DateOffset(days=30)).values
X_ho, y_ho = X[holdout_mask], y[holdout_mask]
X_ho_scaled = scaler.transform(X_ho)

preds = np.column_stack([m.predict(X_ho_scaled) for m in estimators])

cm = confusion_matrix(y_ho.values.flatten(), preds.flatten())
disp = ConfusionMatrixDisplay(cm, display_labels=["No Alarm", "Alarm"])

fig, ax = plt.subplots(figsize=(6, 5))
disp.plot(ax=ax, colorbar=False, cmap="Blues")
ax.set_title("LogisticRegression confusion matrix (all targets, holdout)")
plt.tight_layout()
plt.savefig(os.path.join(PLOTS, "confusion_logistic.png"), dpi=150)
print("[+] Saved -> plots/confusion_logistic.png")