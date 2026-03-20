import pandas as pd
import numpy as np
import joblib
import matplotlib.pyplot as plt
import os

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
MODELS = os.path.join(ROOT, "models")
PLOTS = os.path.join(ROOT, "plots")
FEATURES = os.path.join(ROOT, "datasets", "features.csv")
os.makedirs(PLOTS, exist_ok=True)

estimators = joblib.load(os.path.join(MODELS, "linear_regression.pkl"))
scaler = joblib.load(os.path.join(MODELS, "linear_regression_scaler.pkl"))

feature_names = list(scaler.feature_names_in_)

coeffs = np.array([m.coef_ for m in estimators])
mean_coef = np.abs(coeffs).mean(axis=0)
std_coef = np.abs(coeffs).std(axis=0)

importance_df = pd.DataFrame({
    'feature': feature_names,
    'importance': mean_coef,
    'std': std_coef,
}).sort_values('importance', ascending=False).reset_index(drop=True)

print(f"[i] Top 20 features:\n{importance_df.head(20).to_string(index=False)}")

top = importance_df.head(30)
fig, ax = plt.subplots(figsize=(10, 8))
ax.barh(top['feature'][::-1], top['importance'][::-1], xerr=top['std'][::-1], color='seagreen', ecolor='gray', capsize=3)
ax.set_title("LinearRegression feature importance (mean |coef| across 24 targets)")
ax.set_xlabel("|Coefficient|")
plt.tight_layout()
plt.savefig(os.path.join(PLOTS, "feature_importance_linear.png"), dpi=150)
print("[+] Saved -> plots/feature_importance_linear.png")