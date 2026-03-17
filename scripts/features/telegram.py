import pandas as pd

TG_EVENTS = [
    "air_alert", "air_defense", "casualties", "drones",
    "explosion", "himars", "infrastructure", "iskander",
    "izdeliye30", "kalibr", "kh_series", "kinzhal", "kn23",
    "missiles", "oreshnik", "patriot", "s_system", "shahed",
    "strike", "zircon"
]

def add_telegram_features(df: pd.DataFrame) -> pd.DataFrame:
    for lag in [1, 3, 6]:
        df[f"tg_message_count_lag{lag}"] = df.groupby("region_id")["tg_message_count"].shift(lag)
    
    for w in [3, 6, 12]:
        df[f"tg_message_roll{w}h"] = df.groupby("region_id")["tg_message_count"].shift(1).rolling(w).sum()

    for ev in TG_EVENTS:
        df[f"tg_event_{ev}_count"] = df[f"tg_event_{ev}"].astype("int16")
        df[f"tg_event_{ev}_roll6h"] = df.groupby("region_id")[f"tg_event_{ev}"].shift(1).rolling(6).sum()
    return df
