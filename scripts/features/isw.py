import pandas as pd
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from util.event_detector import detect_events, PATTERNS, WEAPON_PATTERNS

def add_isw_features(df: pd.DataFrame) -> pd.DataFrame:
    existing_isw_cols = [c for c in df.columns if c.startswith("isw_event_")]
    if existing_isw_cols:
        df = df.drop(columns=existing_isw_cols)

    isw_text_map = df[["timestamp_hour", "toplines"]].drop_duplicates().copy()
    isw_text_map["toplines"] = isw_text_map["toplines"].fillna("")
    
    event_results = isw_text_map["toplines"].apply(detect_events)
    
    ALL_POSSIBLE_EVENTS = list(PATTERNS.keys()) + list(WEAPON_PATTERNS.keys())
    
    for ev in ALL_POSSIBLE_EVENTS:
        col_name = f"isw_event_{ev}_count"
        isw_text_map[col_name] = event_results.apply(lambda x: 1 if ev in x else 0).astype("int8")

    isw_features = isw_text_map.drop(columns=["toplines"])
    
    df = df.merge(isw_features, on="timestamp_hour", how="left")
    
    isw_cols = [c for c in df.columns if c.startswith("isw_event_")]
    df[isw_cols] = df[isw_cols].fillna(0).astype("int8")
    
    return df
