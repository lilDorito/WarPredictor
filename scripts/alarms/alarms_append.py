import pandas as pd
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
FULL_FILE = os.path.join(ROOT, "datasets", "alarms", "alarms_data.csv")
DAILY_FILE = os.path.join(ROOT, "datasets", "alarms", "alarms_daily.csv")
LOG_FILE = os.path.join(ROOT, "logs", "alarms", "append.log")

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from datetime import datetime

def log(msg: str):
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def append_overlapping(df: pd.DataFrame) -> pd.DataFrame:
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
        open_alarm = pd.isna(cur["alarm_end"])
        overlaps = (
            pd.notna(cur["alarm_end"])
            and r["alarm_start"] <= cur["alarm_end"]
        )
        if same_group and (overlaps or open_alarm):
            if pd.notna(r["alarm_end"]):
                if pd.isna(cur["alarm_end"]):
                    cur["alarm_end"] = r["alarm_end"]
                else:
                    cur["alarm_end"] = max(cur["alarm_end"], r["alarm_end"])
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

    full = pd.read_csv(FULL_FILE)
    daily = pd.read_csv(DAILY_FILE)
    log(f"Full: {len(full):,} rows | Daily: {len(daily):,} rows")

    for df in [full, daily]:
        df["alarm_start"] = pd.to_datetime(df["alarm_start"])
        df["alarm_end"] = pd.to_datetime(df["alarm_end"])

    combined = pd.concat([full, daily], ignore_index=True)

    combined["_start_5min"] = combined["alarm_start"].dt.floor("5min")
    before = len(combined)
    combined = (
        combined
        .groupby(["region", "alarm_type", "_start_5min"], as_index=False)
        .agg(
            alarm_start = ("alarm_start", "min"),
            alarm_end   = ("alarm_end",   "max"),
            region_en   = ("region_en",   "first"),
        )
        .drop(columns=["_start_5min"])
    )
    log(f"Pass 1 dedup: {before:,} -> {len(combined):,} rows")

    before = len(combined)
    combined = append_overlapping(combined).reset_index(drop=True)
    log(f"Pass 2 dedup: {before:,} -> {len(combined):,} rows")

    combined["duration_min"] = (
        (combined["alarm_end"] - combined["alarm_start"]).dt.total_seconds() / 60
    )

    bad_mask = combined["duration_min"].notna() & (combined["duration_min"] <= 0)
    combined = combined[~bad_mask]

    combined = combined[["alarm_start", "alarm_end", "region", "region_en", "alarm_type", "duration_min"]]
    combined.sort_values("alarm_start", inplace=True)
    combined.to_csv(FULL_FILE, index=False, encoding="utf-8-sig")
    log(f"Saved {len(combined):,} rows -> {FULL_FILE}")
    log("Done.\n")

if __name__ == "__main__":
    main()