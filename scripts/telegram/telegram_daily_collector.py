import pandas as pd
from datetime import datetime, timedelta, timezone
import os
import asyncio
from telethon import TelegramClient
from dotenv import load_dotenv
from text_cleaner import clean_text as clean
from event_detector import detect_events

load_dotenv()

API_ID = int(os.getenv("TELEGRAM_API_ID"))
API_HASH = os.getenv("TELEGRAM_API_HASH")

CHANNELS = [
    "radar_raketa", "Ukraine_UA_24_7", "air_alert_telegram", "alarmua",
    "ukrpravda_news", "pravda_ukraineee", "suspilnenews", "uniannet",
    "hromadske_ua", "war_monitor", "nexta_live", "GeneralStaffZSU", "kpszsu"
]

OUTPUT_FILE = "telegram_data_daily.csv"

def load_existing_ids():
    if os.path.exists(OUTPUT_FILE):
        df = pd.read_csv(OUTPUT_FILE, usecols=["message_id"])
        return set(df["message_id"].tolist())
    return set()

async def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] Start collecting...")

    existing_ids = load_existing_ids()
    since_date = datetime.now(timezone.utc) - timedelta(days=1)
    data = []

    async with TelegramClient("session", API_ID, API_HASH) as client:
        for channel in CHANNELS:
            try:
                async for message in client.iter_messages(channel):
                    if message.date < since_date:
                        break
                    if message.id in existing_ids:
                        continue
                    if not message.text:
                        continue
                    clean_text = clean(message.text)
                    events = detect_events(clean_text)
                    if not events:
                        continue
                    data.append({
                        "message_id": message.id,
                        "message_date": message.date,
                        "message_text": clean_text,
                        "channel": channel,
                        "events": ",".join(sorted(events))
                    })
            except Exception as e:
                print(f"Error in {channel}: {e}")

    if not data:
        print("Nothing collected.")
        return

    df = pd.DataFrame(data)
    file_exists = os.path.exists(OUTPUT_FILE)
    df.to_csv(OUTPUT_FILE, mode="a", index=False, header=not file_exists, encoding="utf-8")

    print(f"Added {len(df)} new posts.")

if __name__ == "__main__":
    asyncio.run(main())
