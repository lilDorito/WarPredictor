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

def process_alarms(path: str, date_end: pd.Timestamp = None) -> pd.DataFrame:
    df = pd.read_csv(path)

    for col in ["alarm_start", "alarm_end"]:
        df[col] = pd.to_datetime(df[col], errors="coerce", format="ISO8601")

    if "region_en" in df.columns:
        df["region"] = df["region_en"]
    elif "region" not in df.columns:
        raise ValueError("No region column found in alarms CSV!")
    df = df.drop(columns=[c for c in ["region_en"] if c in df.columns])
    df = df.dropna(subset=["region"])

    if date_end is not None:
        df = df[df["alarm_start"] <= pd.Timestamp(date_end)].copy()

    df["hour_start"] = df["alarm_start"].dt.floor("h")
    df["hour_end"] = df["alarm_end"].dt.floor("h")
    df["duration_min"] = (df["alarm_end"] - df["alarm_start"]).dt.total_seconds() / 60

    def close_open_alarms(group: pd.DataFrame) -> pd.DataFrame:
        group = group.sort_values("alarm_start").reset_index(drop=True)
        open_idx = group.index[group["alarm_end"].isna()].to_list()
    
        for i in range(len(open_idx) - 1):
            idx = open_idx[i]
            next_start = group.loc[open_idx[i+1], "alarm_start"]
            group.at[idx, "alarm_end"] = next_start - pd.Timedelta(seconds=1)
            group.at[idx, "hour_end"] = (next_start - pd.Timedelta(seconds=1)).floor('h')
            group.at[idx, "duration_min"] = (group.at[idx, "alarm_end"] - group.at[idx, "alarm_start"]).total_seconds() / 60

        if open_idx:
            last_idx = open_idx[-1]
            last_start = group.at[last_idx, "alarm_start"]
            later_closed = group[
                (group["alarm_end"].notna()) & 
                (group["alarm_start"] > last_start)
            ]
            if not later_closed.empty:
                next_start = later_closed["alarm_start"].min()
                group.at[last_idx, "alarm_end"] = next_start - pd.Timedelta(seconds=1)
                group.at[last_idx, "hour_end"] = (next_start - pd.Timedelta(seconds=1)).floor('h')
                group.at[last_idx, "duration_min"] = (group.at[last_idx, "alarm_end"] - last_start).total_seconds() / 60

        return group

    group_cols = ["region", "alarm_type"] if "alarm_type" in df.columns else ["region"]
    chunks = []
    for key, grp in df.groupby(group_cols):
        chunks.append(close_open_alarms(grp))
    df = pd.concat(chunks, ignore_index=True)

    mask_open = df["alarm_end"].isna()
    closed = df[~mask_open].copy()
    open_ = df[mask_open].copy()

    closed = closed[closed["hour_start"].notna() & closed["hour_end"].notna()].copy()
    closed["n_hours"] = ((closed["hour_end"] - closed["hour_start"]) / pd.Timedelta("1h")).astype(int) + 1
    closed = closed.loc[closed.index.repeat(closed["n_hours"])].copy()
    closed["timestamp_hour"] = closed["hour_start"] + pd.to_timedelta(
        closed.groupby(level=0).cumcount(), unit="h"
    )
    closed = closed.reset_index(drop=True)
    closed["alarm_started"] = (closed["timestamp_hour"] == closed["hour_start"]).astype(int)
    closed["alarm_ended"] = (closed["timestamp_hour"] == closed["hour_end"]).astype(int)
    closed["alarm_active"] = 1

    if not open_.empty:
        if date_end is None:
            date_end = closed["hour_end"].max()
        max_hour = pd.Timestamp(date_end).floor("h")
        open_ = open_[open_["hour_start"].notna()].copy()
        open_["n_hours"] = ((max_hour - open_["hour_start"]) / pd.Timedelta("1h")).astype(int) + 1
        open_ = open_.loc[open_.index.repeat(open_["n_hours"])].copy()
        open_["timestamp_hour"] = open_["hour_start"] + pd.to_timedelta(
            open_.groupby(level=0).cumcount(), unit="h"
        )
        open_ = open_.reset_index(drop=True)
        open_["alarm_started"] = (open_["timestamp_hour"] == open_["hour_start"]).astype(int)
        open_["alarm_ended"] = 0
        open_["alarm_active"] = 1
        open_["duration_min"] = None

    all_alarms = pd.concat([closed, open_], ignore_index=True, sort=False)

    result = all_alarms.groupby(["timestamp_hour", "region"]).agg(
        alarms_started=("alarm_started", "sum"),
        alarms_ended=("alarm_ended", "sum"),
        alarms_active=("alarm_active", "sum"),
        alarm_duration_min_sum=("duration_min", "sum")
    ).reset_index()

    return result

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

def process_reddit(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["timestamp_hour"] = (
        pd.to_datetime(df["created_utc"], utc=True)
        .dt.tz_convert(None)
        .dt.floor("h")
    )

    event_dummies = df["events"].str.get_dummies(sep=",").add_prefix("reddit_event_")
    df = pd.concat([df, event_dummies], axis=1)
    event_cols = list(event_dummies.columns)

    agg_dict = {
        "reddit_post_count": ("id", "count"),
        "reddit_score_sum": ("score", "sum"),
    }
    for col in event_cols:
        agg_dict[col] = (col, "sum")

    return df.groupby("timestamp_hour").agg(**agg_dict).reset_index()

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
    spine: pd.DataFrame,
    weather: pd.DataFrame,
    alarms: pd.DataFrame,
    telegram: pd.DataFrame,
    isw: pd.DataFrame,
    reddit: pd.DataFrame,
) -> pd.DataFrame:
    df = spine
    for name, source, keys in [
        ("weather", weather, ["timestamp_hour", "region"]),
        ("alarms", alarms, ["timestamp_hour", "region"]),
        ("telegram", telegram, ["timestamp_hour", "region"]),
        ("isw", isw, ["timestamp_hour"]),
        ("reddit", reddit, ["timestamp_hour"]),
    ]:
        df = df.merge(source, on=keys, how="left")
        print(f"  > {name:<10} {df.shape}")

    num_cols = df.select_dtypes(include=[np.number]).columns
    df[num_cols] = df[num_cols].fillna(0)
    if "alarm_duration_min_sum" in df.columns:
        df["alarm_duration_min_sum"] = pd.to_numeric(df["alarm_duration_min_sum"], errors="coerce").fillna(0)
    if "toplines" in df.columns:
        df["toplines"] = df["toplines"].fillna("")
    return df

# Save / append

def save_to_csv(df: pd.DataFrame, path: str, alarms_path=None):
    output = Path(path)

    df = df.copy()
    df["timestamp_hour"] = pd.to_datetime(df["timestamp_hour"])
    df.set_index(["timestamp_hour", "region"], inplace=True)

    if not output.exists():
        df.sort_index(inplace=True)
        num_cols = df.select_dtypes(include=[np.number]).columns
        df[num_cols] = df[num_cols].fillna(0)
        if "toplines" in df.columns:
            df["toplines"] = df["toplines"].fillna("")
        df.to_csv(output)
        print(f"Created {path} ({len(df):,} rows)")
        return

    existing = pd.read_csv(output, parse_dates=["timestamp_hour"])
    existing.set_index(["timestamp_hour", "region"], inplace=True)

    new_rows = df.loc[~df.index.isin(existing.index)]
    combined = pd.concat([existing, new_rows]) if len(new_rows) else existing
    combined.sort_index(inplace=True)

    if alarms_path:
        fresh_alarms = process_alarms(
            alarms_path,
            date_end=combined.index.get_level_values("timestamp_hour").max()
        )
        fresh_alarms["timestamp_hour"] = pd.to_datetime(fresh_alarms["timestamp_hour"])
        fresh_alarms = fresh_alarms.set_index(["timestamp_hour", "region"])

        alarm_cols = fresh_alarms.columns
        max_ts = combined.index.get_level_values("timestamp_hour").max()
        combined = combined.reset_index()
        fresh_alarms = fresh_alarms.reset_index()

        combined = combined.merge(
            fresh_alarms[["timestamp_hour", "region"] + list(alarm_cols)],
            on=["timestamp_hour", "region"],
            how="left",
            suffixes=("_old", "")
        )
        for col in alarm_cols:
            if f"{col}_old" in combined.columns:
                combined = combined.drop(columns=[f"{col}_old"])
            combined[col] = pd.to_numeric(combined[col], errors="coerce").fillna(0)
        combined = combined.set_index(["timestamp_hour", "region"])
        print("Alarms recomputed")

    num_cols = combined.select_dtypes(include=[np.number]).columns
    combined[num_cols] = combined[num_cols].fillna(0)
    if "toplines" in combined.columns:
        combined["toplines"] = combined["toplines"].fillna("")
    combined.sort_index(inplace=True)
    combined.to_csv(output)

    if len(new_rows):
        print(f"Appended {len(new_rows):,} rows -> {path}")
    else:
        print("No new rows - already up to date.")