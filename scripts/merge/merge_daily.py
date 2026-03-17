import pandas as pd
from merge_utils import (
    build_spine, merge_sources, save_to_csv,
    process_weather, process_alarms, process_reddit,
    process_telegram, process_isw,
)

PATHS = {
    "weather": "datasets/weather/weather_daily.csv",
    "alarms_full": "datasets/alarms/alarms_data.csv",
    "reddit": "datasets/reddit/reddit_daily.csv",
    "telegram": "datasets/telegram/telegram_daily.csv",
    "isw": "datasets/isw/isw_daily.csv",
    "output": "datasets/merged.csv",
}

if __name__ == "__main__":
    # day_before_yesterday = pd.Timestamp.now("UTC").tz_localize(None).floor("D") - pd.Timedelta(days=2)
    # date_start = day_before_yesterday
    # date_end = day_before_yesterday + pd.Timedelta(hours=23)

    yesterday = pd.Timestamp.now("UTC").tz_localize(None).floor("D") - pd.Timedelta(days=1)
    date_start = yesterday
    date_end = yesterday + pd.Timedelta(hours=23)

    print(f"Daily merge for {date_start.date()}")
    print("Building spine...")
    spine = build_spine(str(date_start.date()), date_end)
    print(f"  {len(spine)} rows\n")

    print("Processing sources...")
    weather = process_weather(PATHS["weather"])
    print("  [+] weather")
    alarms = pd.DataFrame(columns=["timestamp_hour", "region", "alarms_started", "alarms_ended", "alarms_active", "alarm_duration_min_sum"])
    print("  [+] alarms (skipped - recomputed from full in save_to_csv)")
    telegram = process_telegram(PATHS["telegram"])
    print("  [+] telegram")
    isw = process_isw(PATHS["isw"])
    print("  [+] isw")
    reddit = process_reddit(PATHS["reddit"])
    print("  [+] reddit")

    print("\nMerging...")
    df = merge_sources(spine, weather, alarms, telegram, isw, reddit)

    print(f"Final shape: {df.shape}")
    save_to_csv(df, PATHS["output"], alarms_path=PATHS["alarms_full"])