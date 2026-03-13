import numpy as np
import pandas as pd
from pathlib import Path
from tqdm import tqdm
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from util.regions import REGION_FIXES, UA_TO_EN, REGION_IDS

# Helpers

def ua_to_region(ua_name: str) -> str | None:
    ua_name = REGION_FIXES.get(ua_name, ua_name)
    if ua_name is None:
        return None
    return UA_TO_EN.get(ua_name)


def build_spine(date_start: str, date_end: pd.Timestamp) -> pd.DataFrame:
    hours = pd.date_range(start=date_start, end=date_end, freq="h")
    idx = pd.MultiIndex.from_product(
        [hours, REGION_IDS], names=["timestamp_hour", "region"]
    )
    return pd.DataFrame(index=idx).reset_index()

# Process funcs

def process_weather(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["timestamp_hour"] = pd.to_datetime(df["datetime"]).dt.floor("h")
    df = df.rename(columns={"region_id": "region"})

    return df.groupby(["timestamp_hour", "region"]).agg(
        temp_mean=("temp", "mean"),
        wind_mean=("wind", "mean"),
        precip_sum=("precip", "sum"),
        pressure_mean=("pressure", "mean"),
        cloudcover_mean=("cloudcover", "mean"),
    ).reset_index()

def process_alarms(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["alarm_start"] = pd.to_datetime(df["alarm_start"])
    df["alarm_end"] = pd.to_datetime(df["alarm_end"])
    df["region"] = df["region_en"]
    df = df.dropna(subset=["region"])

    df["duration_min"] = (df["alarm_end"] - df["alarm_start"]).dt.total_seconds() / 60

    df["hour_start"] = df["alarm_start"].dt.floor("h")
    df["hour_end"] = df["alarm_end"].dt.floor("h")

    now_hour = pd.Timestamp.now().floor("h")

    open_mask = df["alarm_end"].isna()
    closed = df[~open_mask].copy()
    open_ = df[open_mask].copy()

    closed["n_hours"] = ((closed["hour_end"] - closed["hour_start"]) / pd.Timedelta("1h")).astype(int) + 1

    records = []

    for _, row in tqdm(closed.iterrows(), total=len(closed), desc="  alarms (closed)", unit="alarms"):
        for h in pd.date_range(start=row["hour_start"], periods=row["n_hours"], freq="h"):
            records.append({
                "timestamp_hour": h,
                "region": row["region"],
                "alarm_started": int(h == row["hour_start"]),
                "alarm_ended": int(h == row["hour_end"]),
                "alarm_active": 1,
                "duration_min": row["duration_min"],
            })

    for _, row in tqdm(open_.iterrows(), total=len(open_), desc="  alarms (open)", unit="alarms"):
        n_hours = int((now_hour - row["hour_start"]) / pd.Timedelta("1h")) + 1
        for h in pd.date_range(start=row["hour_start"], periods=n_hours, freq="h"):
            records.append({
                "timestamp_hour": h,
                "region": row["region"],
                "alarm_started": int(h == row["hour_start"]),
                "alarm_ended": 0,
                "alarm_active": 1,
                "duration_min": None,
            })

    return pd.DataFrame(records).groupby(["timestamp_hour", "region"]).agg(
        alarms_started=("alarm_started", "sum"),
        alarms_ended=("alarm_ended", "sum"),
        alarms_active=("alarm_active", "sum"),
        alarm_duration_min_sum=("duration_min", "sum"),
    ).reset_index()

def process_telegram(path: str, chunk_size: int = 10_000) -> pd.DataFrame:
    print("  [telegram] reading file...")
    df = pd.read_csv(path)
    df["timestamp_hour"] = (
        pd.to_datetime(df["message_date"], utc=True)
        .dt.tz_convert(None)
        .dt.floor("h")
    )
 
    tagged = df[df["region"].notna()].copy()
    tagged["tg_untagged"] = 0
 
    untagged = df[df["region"].isna()].drop(columns="region").reset_index(drop=True)
 
    print(f"  [telegram] {len(tagged):,} tagged | {len(untagged):,} untagged messages")
 
    untagged_chunks = []
    n_chunks = (len(untagged) + chunk_size - 1) // chunk_size
 
    for i in tqdm(range(n_chunks), desc="  [telegram] expanding untagged", unit="chunk"):
        chunk = untagged.iloc[i * chunk_size:(i + 1) * chunk_size]
        expanded = chunk.loc[chunk.index.repeat(len(REGION_IDS))].reset_index(drop=True)
        expanded["region"] = REGION_IDS * len(chunk)
        expanded["tg_untagged"] = 1
        untagged_chunks.append(expanded)
 
    print("  [telegram] concatenating...")
    df = pd.concat([tagged] + untagged_chunks, ignore_index=True)
 
    print("  [telegram] computing event dummies...")
    event_dummies = df["events"].str.get_dummies(sep=",").add_prefix("tg_event_")
    df = pd.concat([df, event_dummies], axis=1)
    event_cols = list(event_dummies.columns)
 
    agg_dict = {
        "tg_message_count": ("message_id", "count"),
        "tg_untagged_count": ("tg_untagged", "sum"),
    }
    for col in event_cols:
        agg_dict[col] = (col, "sum")
 
    print("  [telegram] aggregating...")
    result = df.groupby(["timestamp_hour", "region"]).agg(**agg_dict).reset_index()
    print(f"  [telegram] done -> {len(result):,} rows")
    return result

def process_isw(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, sep=";")
    df["date"] = pd.to_datetime(df["date"])

    rows = []
    for _, row in df.iterrows():
        for h in range(24):
            rows.append({
                "timestamp_hour": row["date"] + pd.Timedelta(hours=h),
                "toplines": row["toplines"],
            })

    return pd.DataFrame(rows)

# Merge

def merge_sources(
    spine:    pd.DataFrame,
    weather:  pd.DataFrame,
    alarms:   pd.DataFrame,
    telegram: pd.DataFrame,
    isw:      pd.DataFrame,
    reddit:   pd.DataFrame,
) -> pd.DataFrame:
    df = spine
    for name, source, keys in [
        ("weather",  weather,  ["timestamp_hour", "region"]),
        ("alarms",   alarms,   ["timestamp_hour", "region"]),
        ("telegram", telegram, ["timestamp_hour", "region"]),
        ("isw",      isw,      ["timestamp_hour"]),
        ("reddit",   reddit,   ["timestamp_hour"]),
    ]:
        df = df.merge(source, on=keys, how="left")
        print(f"  > {name:<10} {df.shape}")

    num_cols = df.select_dtypes(include=[np.number]).columns
    df[num_cols] = df[num_cols].fillna(0)
    return df

# Save / append

def save_to_csv(df: pd.DataFrame, path: str, alarms_path: str = None):
    df["timestamp_hour"] = df["timestamp_hour"].astype(str)
    output = Path(path)

    if not output.exists():
        df.sort_values(["timestamp_hour", "region"], inplace=True)
        df.to_csv(output, index=False)
        print(f"Created {path}  ({len(df):,} rows)")
        return

    existing = pd.read_csv(output)

    if alarms_path:
        alarm_cols = [c for c in existing.columns if c.startswith("alarm")]
        existing = existing.drop(columns=alarm_cols, errors="ignore")
        fresh_alarms = process_alarms(alarms_path)
        fresh_alarms["timestamp_hour"] = fresh_alarms["timestamp_hour"].astype(str)
        existing = existing.merge(fresh_alarms, on=["timestamp_hour", "region"], how="left")
        alarm_num_cols = [c for c in fresh_alarms.columns if c not in ["timestamp_hour", "region"]]
        existing[alarm_num_cols] = existing[alarm_num_cols].fillna(0)
        print(f"  Alarms reprocessed from scratch")

    existing["_key"] = existing["timestamp_hour"] + "|" + existing["region"].astype(str)
    df["_key"] = df["timestamp_hour"] + "|" + df["region"].astype(str)
    new_rows = df[~df["_key"].isin(existing["_key"])].drop(columns="_key")
    existing = existing.drop(columns="_key")

    if not len(new_rows):
        print("No new rows - already up to date.")
        existing.sort_values(["timestamp_hour", "region"], inplace=True)
        existing.to_csv(output, index=False)
        return

    combined = pd.concat([existing, new_rows], ignore_index=True)
    combined.drop_duplicates(subset=["timestamp_hour", "region"], keep="first", inplace=True)
    combined.sort_values(["timestamp_hour", "region"], inplace=True)
    combined.to_csv(output, index=False)
    print(f"Appended {len(new_rows):,} rows -> {path}  ({len(combined):,} total)")