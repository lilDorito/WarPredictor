import requests
import pandas as pd
from datetime import date, timedelta, datetime
import os
import json
import time

API_KEY = os.getenv("UKRAINE_ALARM_API_KEY")
BASE_URL = "https://api.ukrainealarm.com/api/v3"
OUTPUT_FILE = "air_alarms_historical.csv"
CHECKPOINT_FILE = "alarm_checkpoint.json"

def validate_config():
    if not API_KEY:
        raise Exception("UKRAINE_ALARM_API_KEY environment variable is not set")

def get_date_history(target_date: date) -> list:
    date_str = target_date.strftime("%Y%m%d")
    headers = {"Authorization": API_KEY}
    response = requests.get(
        f"{BASE_URL}/alerts/dateHistory",
        headers=headers,
        params={"date": date_str},
        timeout=10
    )
    response.raise_for_status()
    return response.json()

def parse_records(raw: list, target_date: date) -> list:
    rows = []
    for alert in raw:
        duration = alert.get("duration", {})
        rows.append({
            "date": str(target_date),
            "region_id": alert.get("regionId"),
            "region_name": alert.get("regionName"),
            "start_date": alert.get("startDate"),
            "end_date": alert.get("endDate"),
            "alert_type": alert.get("alertType"),
            "is_continue": alert.get("isContinue"),
            "duration_hours": duration.get("hours", 0),
            "duration_minutes": duration.get("minutes", 0),
            "duration_seconds": duration.get("seconds", 0),
            "total_hours": duration.get("totalHours", 0),
            "total_minutes": duration.get("totalMinutes", 0),
        })
    return rows

def load_checkpoint() -> set:
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            data = json.load(f)
        print(f"[i] Resuming from checkpoint — {len(data)} days already scraped.")
        return set(data)
    return set()

def save_checkpoint(scraped_dates: set) -> None:
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(sorted(scraped_dates), f)

def main():
    validate_config()

    scraped_dates = load_checkpoint()

    start = date(2022, 2, 24)
    end = date.today() - timedelta(days=1)
    current = start

    total_days = (end - start).days + 1
    done = 0

    print(f"Scraping {start} -> {end} ({total_days} days total)")
    print(f"Already done: {len(scraped_dates)} days\n")

    while current <= end:
        date_key = str(current)

        if date_key in scraped_dates:
            current += timedelta(days=1)
            done += 1
            continue

        try:
            raw = get_date_history(current)
            rows = parse_records(raw, current)
            df = pd.DataFrame(rows)

            file_exists = os.path.exists(OUTPUT_FILE)
            df.to_csv(OUTPUT_FILE, mode="a", index=False, header=not file_exists, encoding="utf-8")

            scraped_dates.add(date_key)
            save_checkpoint(scraped_dates)

            done += 1
            print(f"[+] {date_key} — {len(rows)} alerts ({done}/{total_days})")

        except requests.exceptions.RequestException as e:
            print(f"[!] {date_key} - API error: {e}, retrying in 10s...")
            time.sleep(10)
            continue

        except Exception as e:
            print(f"[!] {date_key} - Unexpected error: {e}, skipping.")

        time.sleep(0.5)
        current += timedelta(days=1)

    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)

    print(f"\nDone. Output saved to {OUTPUT_FILE}.")

    df_full = pd.read_csv(OUTPUT_FILE)
    print(f"Total rows: {len(df_full)}")
    print(f"Alert types:\n{df_full['alert_type'].value_counts()}")
    print(f"\nTop regions by alert count:\n{df_full['region_name'].value_counts().head(10)}")


if __name__ == "__main__":
    main()
  
