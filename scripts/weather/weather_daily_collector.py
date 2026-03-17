import requests
import pandas as pd
import time
import os
import sys
from datetime import datetime, timedelta

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
OUTPUT_FILE = os.path.join(ROOT, "datasets", "weather", "weather_daily.csv")
LOG_FILE = os.path.join(ROOT, "logs", "weather", "daily_collector.log")

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from util.regions import REGIONS

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

def log(msg: str):
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def main():
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    # yesterday = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")

    log("> Weather daily collector starting <")
    log(f"Window: {yesterday}")

    all_records = []
    errors = []

    for region_name_ua, (lat, lon, region_id) in REGIONS.items():
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": yesterday,
            "end_date": yesterday,
            "hourly": ["temperature_2m", "wind_speed_10m", "precipitation", "pressure_msl", "cloud_cover"],
            "timezone": "UTC",
        }
        try:
            response = requests.get(ARCHIVE_URL, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            hourly = data.get("hourly", {})
            times = hourly.get("time", [])

            for i in range(len(times)):
                all_records.append({
                    "region_id": region_id,
                    "datetime": times[i],
                    "temp": hourly["temperature_2m"][i],
                    "wind": hourly["wind_speed_10m"][i],
                    "precip": hourly["precipitation"][i],
                    "pressure": hourly["pressure_msl"][i],
                    "cloudcover": hourly["cloud_cover"][i],
                })
            log(f"[+] {region_id} ({len(times)} rows)")
            time.sleep(0.5)

        except Exception as e:
            log(f"[!] Error in {region_id}: {e}")
            errors.append((region_id, str(e)))

    if not all_records:
        log("[!] No data collected.")
        return

    df = pd.DataFrame(all_records)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df.to_csv(OUTPUT_FILE, mode="w", index=False, header=True, encoding="utf-8")
    log(f"Collected {len(df):,} rows -> {OUTPUT_FILE}")

    if errors:
        log(f"[!] {len(errors)} region(s) failed: {[e[0] for e in errors]}")

    log("Done.\n")

if __name__ == "__main__":
    main()