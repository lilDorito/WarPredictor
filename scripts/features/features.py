import pandas as pd
import timed, alarms, weather, telegram, reddit, isw
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from util.regions import UA_TO_EN

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
MERGED = os.path.join(ROOT, "datasets", "merged.csv")
FEATURES = os.path.join(ROOT, "datasets", "features.csv")

def add_targets(df: pd.DataFrame) -> pd.DataFrame:
    df["alarms_active"] = (df["alarms_active"] > 0).astype(int)
    
    for h in range(6, 30):
        df[f"target_alarm_t{h}"] = df.groupby("region_id")["alarms_active"].shift(-h)
    
    return df

def main():
    if os.path.exists(FEATURES):
        os.remove(FEATURES)
        print("[!] Old features deleted. Starting fresh...\n")

    df = pd.read_csv(MERGED, parse_dates=["timestamp_hour"])
    df.columns = df.columns.str.strip()

    df["region"] = df["region"].str.replace(" ", "_")
    df["region_id"] = df["region"]

    initial_len = len(df)
    df = df.drop_duplicates(subset=["timestamp_hour", "region_id"], keep="first")
    print(f"Cleaned data: {initial_len} -> {len(df)} rows.")

    df = df.sort_values(["region_id", "timestamp_hour"]).reset_index(drop=True)

    print("> Applying Timed...")
    df = timed.add_time_features(df)
    
    print("> Applying Alarms...")
    df = alarms.add_alarm_features(df)
    df = df.copy()
    
    print("> Applying Weather...")
    df = weather.add_weather_features(df)
    df = df.copy()
    
    print("> Applying Telegram/Reddit...")
    df = telegram.add_telegram_features(df)
    df = reddit.add_reddit_features(df)
    df = df.copy()
    
    print("> Applying ISW...")
    df = isw.add_isw_features(df)
    
    df = add_targets(df)

    df = df.drop(columns=["region", "toplines"], errors="ignore")
    df = df.fillna(0)

    protected_prefixes = ['isw_event_', 'tg_event_', 'reddit_event_', 'target_']
    protected_cols = [c for c in df.columns if any(c.startswith(p) for p in protected_prefixes)]
    
    selector = df.nunique() > 1
    cols_to_keep = selector[selector].index.tolist() + ['region_id', 'timestamp_hour'] + protected_cols
    
    final_cols = list(dict.fromkeys(cols_to_keep)) 
    df = df[df.columns.intersection(final_cols)]

    df = df.fillna(0)

    target_cols = [c for c in df.columns if "target" in c]
    df[target_cols] = df[target_cols].astype("int8")

    df.to_csv(FEATURES, index=False)
    print(f"\n[i] Features saved. Shape: {df.shape}")

if __name__ == "__main__":
    main()