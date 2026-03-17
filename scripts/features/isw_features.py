import pandas as pd
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from util.event_detector import detect_events

ISW_EVENTS = ["air_alert", "missiles", "drones", "strike", "explosion", "air_defense"]

def add_isw_features(df: pd.DataFrame) -> pd.DataFrame:
    isw_text_map = df[["timestamp_hour", "toplines"]].drop_duplicates().copy()
    isw_text_map["toplines"] = isw_text_map["toplines"].fillna("")
    
    print(f"[isw] Detecting events for {len(isw_text_map)} unique hours...")
    
    event_results = isw_text_map["toplines"].apply(detect_events)
    
    for ev in ISW_EVENTS:
        isw_text_map[f"isw_event_{ev}_count"] = event_results.apply(lambda x: 1 if ev in x else 0).astype("int8")
    
    isw_features = isw_text_map.drop(columns=["toplines"])
    df = df.merge(isw_features, on="timestamp_hour", how="left")
    
    return df.copy()
