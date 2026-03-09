import os
import sys
import glob
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from util.regions import REGIONS, REGION_FIXES

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

INPUT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "alert_csvs"))
OUTPUT_FILE = os.path.abspath(os.path.join(BASE_DIR, "..", "alarms_combined.csv"))

COLUMNS = {
    "Оголошено о": "alarm_start",
    "Закінчено о": "alarm_end",
    "Регіон": "region",
    "Тип": "alarm_type",
}

KEEP = ["alarm_start", "alarm_end", "region", "region_en", "alarm_type", "duration_min"]

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
            df["alarm_start"] = pd.to_datetime(df["alarm_start"], format="%d.%m.%Y, %H:%M:%S", errors="coerce")
            df["alarm_end"] = pd.to_datetime(df["alarm_end"],   format="%d.%m.%Y, %H:%M:%S", errors="coerce")
            dfs.append(df)
        except Exception as e:
            errors.append((path, str(e)))
            print(f"[!] Skipped {os.path.basename(path)}: {e}")

    if not dfs:
        print("[!] No data loaded.")
        return

    combined = pd.concat(dfs, ignore_index=True)

    complete_keys = set(
        zip(
            combined.loc[combined["alarm_end"].notna(), "region"],
            combined.loc[combined["alarm_end"].notna(), "alarm_start"],
        )
    )
    is_dup_nat = combined["alarm_end"].isna() & combined.apply(
        lambda r: (r["region"], r["alarm_start"]) in complete_keys, axis=1
    )
    dup_nat_count = is_dup_nat.sum()
    combined = combined[~is_dup_nat]
    if dup_nat_count:
        print(f"[i] Dropped {dup_nat_count:,} duplicate NaT rows")

    combined["duration_min"] = (
        (combined["alarm_end"] - combined["alarm_start"]).dt.total_seconds() / 60
    )

    bad_mask  = combined["duration_min"].notna() & (combined["duration_min"] <= 0)
    bad_count = bad_mask.sum()
    combined  = combined[~bad_mask]
    if bad_count:
        print(f"[i] Dropped {bad_count:,} rows with zero or negative duration")

    combined["region"] = combined["region"].map(lambda x: REGION_FIXES.get(x, x))

    before   = len(combined)
    combined = combined[combined["region"].isin(REGIONS.keys())]
    dropped  = before - len(combined)
    if dropped:
        print(f"[i] Dropped {dropped} rows with unknown regions")

    combined["region_en"] = combined["region"].map(lambda x: REGIONS[x][2])

    combined = combined[KEEP]
    combined = combined.sort_values("alarm_start").reset_index(drop=True)
    combined.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    remaining_nat = combined["alarm_end"].isna().sum()
    print(f"\n[✓] Combined {len(dfs)} files -> {len(combined):,} rows")
    print(f"[✓] Regions: {combined['region'].nunique()} unique")
    print(f"[✓] Open-ended alarms (NaT alarm_end): {remaining_nat:,}")
    print(f"[✓] Date range: {combined['alarm_start'].min()} -> {combined['alarm_start'].max()}")
    print(f"[✓] Saved to: {OUTPUT_FILE}")

    if errors:
        print(f"\n[!] {len(errors)} files had errors:")
        for path, err in errors:
            print(f"    {os.path.basename(path)}: {err}")

if __name__ == "__main__":
    main()
