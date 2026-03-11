import pandas as pd
import os
from datetime import datetime

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
FULL_FILE  = os.path.join(ROOT, "datasets", "isw", "isw_data.csv")
DAILY_FILE = os.path.join(ROOT, "datasets", "isw", "isw_daily.csv")
LOG_FILE   = os.path.join(ROOT, "logs", "isw", "append.log")

def log(msg: str):
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def main():
    log("> ISW append starting <")

    if not os.path.exists(DAILY_FILE):
        log("[!] No daily file found, nothing to append.")
        return

    full  = pd.read_csv(FULL_FILE,  sep=";") if os.path.exists(FULL_FILE)  else pd.DataFrame()
    daily = pd.read_csv(DAILY_FILE, sep=";")
    log(f"Full: {len(full):,} rows | Daily: {len(daily):,} rows")

    combined = pd.concat([full, daily], ignore_index=True)
    before   = len(combined)
    combined.drop_duplicates(subset=["date"], keep="first", inplace=True)
    log(f"Dedup: {before:,} -> {len(combined):,} rows ({before - len(combined)} dropped)")

    combined.sort_values("date", inplace=True)
    os.makedirs(os.path.dirname(FULL_FILE), exist_ok=True)
    combined.to_csv(FULL_FILE, index=False, sep=";")
    log(f"Saved {len(combined):,} rows -> {FULL_FILE}")
    log("Done.\n")

if __name__ == "__main__":
    main()