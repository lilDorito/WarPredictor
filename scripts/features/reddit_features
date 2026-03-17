import pandas as pd

REDDIT_EVENTS = [
    "air_alert", "air_defense", "casualties", "drones",
    "explosion", "himars", "infrastructure", "iskander",
    "izdeliye30", "kalibr", "kh_series", "kinzhal", "kn23",
    "missiles", "oreshnik", "patriot", "s_system", "shahed",
    "strike", "zircon"
]

def add_reddit_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    for lag in [1, 3, 6]:
        df[f"reddit_post_count_lag{lag}"] = df.groupby("region_id")["reddit_post_count"].shift(lag)
    
    new_cols = {}
    
    for ev in REDDIT_EVENTS:
        col = f"reddit_event_{ev}"
        if col in df.columns:
            new_cols[f"reddit_event_{ev}_count"] = df[col].astype("int16")
            new_cols[f"reddit_event_{ev}_roll6h"] = df.groupby("region_id")[col].shift(1).rolling(6, min_periods=1).sum()
    
    df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)
    return df.copy()
