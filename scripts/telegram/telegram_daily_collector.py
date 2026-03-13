import pandas as pd
from datetime import datetime, timedelta, timezone
import os
import sys
import asyncio
from telethon import TelegramClient
from dotenv import load_dotenv

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
OUTPUT_FILE = os.path.join(ROOT, "datasets", "telegram", "telegram_daily.csv")
SESSION = os.path.join(ROOT, "scripts", "telegram", "session")
LOG_FILE = os.path.join(ROOT, "logs", "telegram", "daily_collector.log")

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from util.text_cleaner import clean_text as clean
from util.event_detector import detect_events
from util.geo_tagger import smart_extract

load_dotenv(os.path.join(ROOT, ".env"))
API_ID = int(os.getenv("TELEGRAM_API_ID"))
API_HASH = os.getenv("TELEGRAM_API_HASH")

CHANNELS = [
    "radar_raketa", "Ukraine_UA_24_7", "air_alert_telegram", "alarmua",
    "ukrpravda_news", "pravda_ukraineee", "suspilnenews", "uniannet",
    "hromadske_ua", "war_monitor", "nexta_live", "GeneralStaffZSU", "kpszsu"
]

def log(msg: str):
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def load_existing_ids() -> set:
    if os.path.exists(OUTPUT_FILE):
        df = pd.read_csv(OUTPUT_FILE, usecols=["message_id"])
        return set(df["message_id"].tolist())
    return set()

async def main():
    log("> Telegram daily collector starting <")

    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    since_date = today - timedelta(days=1)
    until_date = today
    log(f"Window: {since_date.date()} 00:00 UTC -> {until_date.date()} 00:00 UTC")

    existing_ids = load_existing_ids()
    data = []

    async with TelegramClient(SESSION, API_ID, API_HASH) as client:
        for channel in CHANNELS:
            try:
                async for message in client.iter_messages(channel):
                    if message.date >= until_date:
                        continue
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
                        "events": ",".join(sorted(events)),
                        "region": smart_extract(clean_text),
                    })

            except Exception as e:
                log(f"[!] Error in {channel}: {e}")

    if not data:
        log("Nothing collected.")
        return

    df = pd.DataFrame(data)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df = df.sort_values("message_date", ascending=True)
    df.to_csv(OUTPUT_FILE, mode="w", index=False, header=True, encoding="utf-8")
    log(f"Added {len(df)} new posts ({since_date.date()}) -> {OUTPUT_FILE}")
    log("Done.\n")

if __name__ == "__main__":
    asyncio.run(main())
