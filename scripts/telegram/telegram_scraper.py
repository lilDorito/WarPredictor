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

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from util.text_cleaner import clean_text as clean
from util.event_detector import detect_events

load_dotenv(os.path.join(ROOT, ".env"))
API_ID = int(os.getenv("TELEGRAM_API_ID"))
API_HASH = os.getenv("TELEGRAM_API_HASH")

CHANNELS = [
    "radar_raketa", "Ukraine_UA_24_7", "air_alert_telegram", "alarmua",
    "ukrpravda_news", "pravda_ukraineee", "suspilnenews", "uniannet",
    "hromadske_ua", "war_monitor", "nexta_live", "GeneralStaffZSU", "kpszsu"
]
SINCE_DATE = datetime(2022, 2, 24, tzinfo=timezone.utc)

def log(msg: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}")

async def main():
    log("Start collecting...")

    data = []

    async with TelegramClient(SESSION, API_ID, API_HASH) as client:
        for channel in CHANNELS:
            log(f"Fetching messages from channel: {channel}")
            channel_count = 0
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
                    channel_count += 1
                    if channel_count % 1000 == 0:
                        log(f"  -> {channel_count} messages collected so far in {channel}")
            except Exception as e:
                log(f"[!] Error in {channel}: {e}")

            log(f"Finished channel {channel}: {channel_count} messages collected")

    if not data:
        log("Nothing collected.")
        return

    df = pd.DataFrame(data)
    df = df.drop_duplicates(subset=["channel", "message_id"])
    df["message_date"] = pd.to_datetime(df["message_date"], utc=True).dt.tz_convert(None)
    df = df.sort_values("message_date", ascending=True)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    log(f"Collected {len(df)} posts -> {OUTPUT_FILE}")

if __name__ == "__main__":
    asyncio.run(main())