import pandas as pd
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
FULL_FILE = os.path.join(ROOT, "datasets", "alarms", "alarms_data.csv")
DAILY_FILE = os.path.join(ROOT, "datasets", "alarms", "alarms_daily.csv")
LOG_FILE = os.path.join(ROOT, "logs", "alarms", "append.log")

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from datetime import datetime

KEEP = ["alarm_start", "alarm_end", "region", "region_en", "alarm_type", "duration_min"]

def log(msg: str):
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def merge_overlapping(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.sort_values(["region", "alarm_type", "alarm_start"]).reset_index(drop=True)
    rows = df.to_dict("records")
    merged = []
    cur = rows[0].copy()

    for r in rows[1:]:
        same_group = (
            r["region"] == cur["region"]
            and r["alarm_type"] == cur["alarm_type"]
        )

        if not same_group:
            merged.append(cur)
            cur = r.copy()
            continue

        cur_is_open = pd.isna(cur["alarm_end"])
        is_phantom = (
            cur_is_open
            and abs((r["alarm_start"] - cur["alarm_start"]).total_seconds()) <= 60
        )
        overlaps = (
            pd.notna(cur["alarm_end"])
            and r["alarm_start"] <= cur["alarm_end"]
        )

        if is_phantom or overlaps:
            if pd.notna(r["alarm_end"]):
                cur["alarm_end"] = r["alarm_end"]
        else:
            merged.append(cur)
            cur = r.copy()

    merged.append(cur)
    return pd.DataFrame(merged)

def main():
    log("> Alarms append starting <")

    if not os.path.exists(DAILY_FILE):
        log("[!] No daily file found, nothing to append.")
        return

    full = pd.read_csv(FULL_FILE, encoding="utf-8-sig")
    daily = pd.read_csv(DAILY_FILE, encoding="utf-8-sig")
    log(f"Full: {len(full):,} rows | Daily: {len(daily):,} rows")

    full["alarm_start"] = pd.to_datetime(full["alarm_start"], format="ISO8601")
    full["alarm_end"] = pd.to_datetime(full["alarm_end"], format="ISO8601")
    daily["alarm_start"] = pd.to_datetime(daily["alarm_start"], format="ISO8601")
    daily["alarm_end"] = pd.to_datetime(daily["alarm_end"], format="ISO8601")

    combined = pd.concat([full, daily], ignore_index=True)

    combined["_start_5min"] = combined["alarm_start"].dt.floor("5min")
    before = len(combined)
    combined = (
        combined
        .groupby(["region", "alarm_type", "_start_5min"], as_index=False)
        .agg(
            alarm_start=("alarm_start", "min"),
            alarm_end=("alarm_end", "max"),
            region_en=("region_en", "first"),
        )
        .drop(columns=["_start_5min"])
    )
    log(f"Pass 1 dedup: {before:,} -> {len(combined):,} rows")

    before = len(combined)
    combined = merge_overlapping(combined).reset_index(drop=True)
    log(f"Pass 2 overlap merge: {before:,} -> {len(combined):,} rows")

    combined["duration_min"] = (
        (combined["alarm_end"] - combined["alarm_start"]).dt.total_seconds() / 60
    )

    max_date = combined["alarm_start"].max()
    stale = combined["alarm_end"].isna() & (combined["alarm_start"] < max_date - pd.Timedelta(days=30))
    stale_count = stale.sum()
    combined.loc[stale, "alarm_end"] = combined.loc[stale, "alarm_start"] + pd.Timedelta(hours=1)
    combined.loc[stale, "duration_min"] = 60
    if stale_count:
        log(f"Closed {stale_count} stale open alarms (DST artifacts)")

    bad_mask = combined["duration_min"].notna() & (combined["duration_min"] <= 0)
    bad_count = bad_mask.sum()
    combined = combined[~bad_mask]
    if bad_count:
        log(f"Dropped {bad_count:,} rows with zero or negative duration")

    combined = combined[KEEP].sort_values("alarm_start").reset_index(drop=True)
    combined.to_csv(FULL_FILE, index=False, encoding="utf-8-sig")
    log(f"Saved {len(combined):,} rows -> {FULL_FILE}")
    log("Done.\n")

if __name__ == "__main__":
    main()