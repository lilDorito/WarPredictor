import requests
import pandas as pd
from datetime import date, timedelta, datetime
import os

from alarms_scraper import (
    validate_config,
    get_date_history,
    parse_records,
    OUTPUT_FILE,
)

def load_existing_dates() -> set:
    if not os.path.exists(OUTPUT_FILE):
        return set()
    df = pd.read_csv(OUTPUT_FILE, usecols=["date"])
    return set(df["date"].tolist())

def main():
    validate_config()

    yesterday = date.today() - timedelta(days=1)
    date_key = str(yesterday)

    existing_dates = load_existing_dates()
    if date_key in existing_dates:
        print(f"[SKIP] {yesterday} already collected.")
        return

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] Fetching alarms for {yesterday}...")

    try:
        raw = get_date_history(yesterday)
    except requests.exceptions.RequestException as e:
        print(f"[!] API error: {e}")
        return

    if not raw:
        print(f"[!] No data returned for {yesterday}.")
        return

    rows = parse_records(raw, yesterday)
    df = pd.DataFrame(rows)

    file_exists = os.path.exists(OUTPUT_FILE)
    df.to_csv(OUTPUT_FILE, mode="a", index=False, header=not file_exists, encoding="utf-8")
    print(f"[+] Added {len(df)} alert records for {yesterday} to {OUTPUT_FILE}.")

if __name__ == "__main__":
    main()
  
