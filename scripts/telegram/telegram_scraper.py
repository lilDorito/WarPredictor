import pandas as pd
from datetime import datetime, timezone
import os
import sys
import asyncio
from telethon import TelegramClient
from dotenv import load_dotenv

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
OUTPUT_FILE = os.path.join(ROOT, "datasets", "telegram", "telegram_data.csv")
SESSION = os.path.join(ROOT, "scripts", "telegram", "session")

sys.path.append(os.path.dirname(__file__))
from text_cleaner import clean_text as clean
from event_detector import detect_events

load_dotenv(os.path.join(ROOT, ".env"))
API_ID = int(os.getenv("TELEGRAM_API_ID"))
API_HASH = os.getenv("TELEGRAM_API_HASH")

CHANNELS   = [
    "radar_raketa", "Ukraine_UA_24_7", "air_alert_telegram", "alarmua",
    "ukrpravda_news", "pravda_ukraineee", "suspilnenews", "uniannet",
    "hromadske_ua", "war_monitor", "nexta_live", "GeneralStaffZSU", "kpszsu"
]
SINCE_DATE = datetime(2022, 2, 24, tzinfo=timezone.utc)

async def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] Start collecting...")
    data = []

    async with TelegramClient(SESSION, API_ID, API_HASH) as client:
        for channel in CHANNELS:
            try:
                async for message in client.iter_messages(channel):
                    if message.date < SINCE_DATE:
                        break
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
                print(f"[!] Error in {channel}: {e}")

    if not data:
        print("Nothing collected.")
        return

    df = pd.DataFrame(data)
    df = df.drop_duplicates(subset=["message_id"])
    df = df.sort_values("message_date", ascending=False)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
    print(f"Collected {len(df)} posts -> {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
