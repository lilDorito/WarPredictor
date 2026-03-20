import pandas as pd
import os
from merge_utils import (
    build_spine, merge_sources, save_to_csv,
    process_weather, process_alarms, process_reddit,
    process_telegram, process_isw,
)

DATE_START = "2022-02-24"

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

PATHS = {
    "weather": os.path.join(ROOT, "datasets/weather/weather_data.csv"),
    "alarms": os.path.join(ROOT, "datasets/alarms/alarms_data.csv"),
    "reddit": os.path.join(ROOT, "datasets/reddit/reddit_data.csv"),
    "telegram": os.path.join(ROOT, "datasets/telegram/telegram_data.csv"),
    "isw": os.path.join(ROOT, "datasets/isw/isw_data.csv"),
    "output": os.path.join(ROOT, "datasets/merged.csv"),
}

if __name__ == "__main__":
    date_end = pd.Timestamp.now("UTC").tz_localize(None).floor("D") - pd.Timedelta(seconds=1)
    # date_end = pd.Timestamp("2026-03-14") - pd.Timedelta(seconds=1)

    print("Building spine...")
    spine = build_spine(DATE_START, date_end)
    print(f"  {len(spine):,} rows\n")

    print("Processing sources...")
    weather = process_weather(PATHS["weather"])
    print("  [+] weather")
    alarms = process_alarms(PATHS["alarms"], date_end=date_end)
    print("  [+] alarms")
    telegram = process_telegram(PATHS["telegram"])
    print("  [+] telegram")
    isw = process_isw(PATHS["isw"])
    print("  [+] isw")
    reddit = process_reddit(PATHS["reddit"])
    print("  [+] reddit")

    print("\nMerging...")
    df = merge_sources(spine, weather, alarms, telegram, isw, reddit)

    print(f"\nFinal shape: {df.shape}")
    save_to_csv(df, PATHS["output"], alarms_path=PATHS["alarms"])