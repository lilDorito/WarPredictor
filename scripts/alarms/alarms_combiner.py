import os
import sys
import glob
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from util.regions import REGIONS, REGION_FIXES

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
INPUT_DIR = os.path.join(ROOT, "datasets", "alarms", "alert_csvs")
OUTPUT_FILE = os.path.join(ROOT, "datasets", "alarms", "alarms_data.csv")

COLUMNS = {
    "Оголошено о": "alarm_start",
    "Закінчено о": "alarm_end",
    "Регіон": "region",
    "Тип": "alarm_type",
}

KEEP = ["alarm_start", "alarm_end", "region", "region_en", "alarm_type", "duration_min"]

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
    files = sorted(glob.glob(os.path.join(INPUT_DIR, "alerts_*.csv")))

    if not files:
        print(f"[!] No CSV files found in {INPUT_DIR}")
        return

    print(f"[i] Found {len(files)} files. Combining...")

    dfs = []
    errors = []

    for path in files:

        try:
            df = pd.read_csv(path, encoding="utf-8", usecols=COLUMNS.keys())
            df = df.rename(columns=COLUMNS)
            df["alarm_start"] = pd.to_datetime(
                df["alarm_start"],
                format="%d.%m.%Y, %H:%M:%S",
                errors="coerce"
            )
            df["alarm_end"] = pd.to_datetime(
                df["alarm_end"],
                format="%d.%m.%Y, %H:%M:%S",
                errors="coerce"
            )
            dfs.append(df)

        except Exception as e:
            errors.append((path, str(e)))
            print(f"[!] Skipped {os.path.basename(path)}: {e}")

    if not dfs:
        print("[!] No data loaded.")
        return

    combined = pd.concat(dfs, ignore_index=True)

    print(f"[i] Total rows after concat: {len(combined):,}")

    before = len(combined)
    combined = combined[combined["alarm_start"].notna()]
    dropped = before - len(combined)

    if dropped:
        print(f"[i] Dropped {dropped:,} rows with missing alarm_start")

    combined["region"] = combined["region"].map(lambda x: REGION_FIXES.get(x, x))
    before = len(combined)
    combined = combined[combined["region"].isin(REGIONS.keys())]
    dropped = before - len(combined)

    if dropped:
        print(f"[i] Dropped {dropped:,} rows with unknown regions")

    combined["_start_5min"] = combined["alarm_start"].dt.floor("5min")
    before = len(combined)

    combined = (
        combined
        .groupby(["region", "alarm_type", "_start_5min"], as_index=False)
        .agg(
            alarm_start=("alarm_start", "min"),
            alarm_end=("alarm_end", "max"),
        )
        .drop(columns=["_start_5min"])
    )

    print(f"[i] Pass 1 dedup: {before:,} -> {len(combined):,} rows")

    before = len(combined)
    combined = merge_overlapping(combined).reset_index(drop=True)

    print(f"[i] Pass 2 overlap merge: {before:,} -> {len(combined):,} rows")

    combined["duration_min"] = (
        (combined["alarm_end"] - combined["alarm_start"])
        .dt.total_seconds() / 60
    )

    bad_mask = combined["duration_min"].notna() & (combined["duration_min"] <= 0)
    bad_count = bad_mask.sum()
    combined = combined[~bad_mask]

    if bad_count:
        print(f"[i] Dropped {bad_count:,} rows with zero or negative duration")

    combined["region_en"] = combined["region"].map(lambda x: REGIONS[x][2])
    combined = combined[KEEP].sort_values("alarm_start").reset_index(drop=True)
    combined.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    remaining_nat = combined["alarm_end"].isna().sum()

    print(f"\n[✓] Combined {len(dfs)} files -> {len(combined):,} rows")
    print(f"[✓] Regions: {combined['region'].nunique()} unique")
    print(f"[✓] Open-ended alarms: {remaining_nat:,}")
    print(f"[✓] Date range: {combined['alarm_start'].min()} -> {combined['alarm_start'].max()}")
    print(f"[✓] Saved to: {OUTPUT_FILE}")

    if errors:
        print(f"\n[!] {len(errors)} files had errors:")
        for path, err in errors:
            print(f"    {os.path.basename(path)}: {err}")

if __name__ == "__main__":
    main()