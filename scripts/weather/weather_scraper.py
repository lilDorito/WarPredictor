import requests
import pandas as pd
import time
import os
from datetime import datetime, timedelta

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
OUTPUT_DIR = os.path.join(ROOT, "datasets", "weather")

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from util.regions import REGIONS

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

def build_weather_dataset(start_date: str) -> pd.DataFrame:
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"[i] Starting data collection: {start_date} -> {yesterday}")

    all_records = []

    for region_name_ua, (lat, lon, region_id) in REGIONS.items():
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date,
            "end_date": yesterday,
            "hourly": ["temperature_2m", "wind_speed_10m", "precipitation", "pressure_msl"],
            "timezone": "UTC",
        }
        try:
            response = requests.get(ARCHIVE_URL, params=params)
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
                })
            print(f"[+] {region_id} ({len(times)} rows)")
            time.sleep(0.5)

        except Exception as e:
            print(f"[!] Error in {region_id}: {e}")

    df = pd.DataFrame(all_records)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_file = os.path.join(OUTPUT_DIR, f"weather_data.csv")
    df.to_csv(output_file, index=False)
    print(f"[i] Done. {len(df):,} rows -> {output_file}")
    return df

if __name__ == "__main__":
    START_DATE = "2022-02-24"
    build_weather_dataset(START_DATE)