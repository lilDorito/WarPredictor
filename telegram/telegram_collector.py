import pandas as pd
from datetime import datetime, timedelta, timezone
import re
import os
import asyncio
from telethon import TelegramClient
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.getenv("TELEGRAM_API_ID"))
API_HASH = os.getenv("TELEGRAM_API_HASH")

CHANNELS = ["radar_raketa", "Ukraine_UA_24_7", "air_alert_telegram", "alarmua", "ukrpravda_news", "pravda_ukraineee", "suspilnenews",
            "uniannet", "hromadske_ua", "war_monitor", "nexta_live", "GeneralStaffZSU", "kpszsu"
]

KEYWORDS = [
    "тривога", "відбій", "обстріл", "ракета", "приліт", "шахед", "бпла",
    "тревога", "отбой", "обстрел", " прилёт", "ураження", "ліквідовано", "ликвидирован", "поражение",
    "влучання", "попадание"
]

OUTPUT_FILE = os.path.join(
    os.getcwd(),
    "war_prediction",
    "telegram_data.parquet"
)

def load_existing_ids():
    if os.path.exists(OUTPUT_FILE):
        df = pd.read_parquet(OUTPUT_FILE, columns=["message_id"])
        return set(df["message_id"].tolist())
    return set()

def clean_for_alert_prediction(text: str) -> str:
    if not text:
        return ""

    text = text.lower()

    text = re.sub(r"http\S+|www\S+", "", text)

    text = re.sub(r"@\w+", "", text)

    text = re.sub(r"["
                  r"\U0001F300-\U0001F5FF"
                  r"\U0001F600-\U0001F64F"
                  r"\U0001F680-\U0001F6FF"
                  r"\U0001F1E0-\U0001F1FF"
                  r"]+", "", text)

    text = re.sub(r"[\n\t]", " ", text)

    text = re.sub(r"[^a-zа-яёіїєґ0-9\s!?]", "", text)

    text = re.sub(r"\s+", " ", text).strip()

    return text

async def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] Start colecting...")

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    existing_ids = load_existing_ids()
    data = []

    if os.path.exists(OUTPUT_FILE):
        df = pd.read_parquet(OUTPUT_FILE)
        since_date = df["message_date"].max()
        if since_date.tzinfo is None:
            since_date = since_date.replace(tzinfo=timezone.utc)
    else:
        since_date = datetime.now(timezone.utc) - timedelta(days=1)
    
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

                    clean_text = clean_for_alert_prediction(message.text)

                    if any(keyword in clean_text for keyword in KEYWORDS):
                        data.append({
                            "message_id": message.id,
                            "message_date": message.date,
                            "message_text": clean_text,
                            "channel": channel
                        })

            except Exception as e:
                print(f"Error in {channel}: {e}")

    if not data:
        print("Нічого не зібрано")
        return    

    df = pd.DataFrame(data)

    if os.path.exists(OUTPUT_FILE):
        existing_df = pd.read_parquet(OUTPUT_FILE)
        combined_df = pd.concat([existing_df, df], ignore_index=True)
    else:
        combined_df = df

    combined_df = combined_df.drop_duplicates(subset=["message_id"])        
    combined_df = combined_df.sort_values("message_date", ascending=False)
    combined_df.to_parquet(OUTPUT_FILE, index=False)

    print(f"Added {len(data)} new posts. Current number: {len(combined_df)}")


asyncio.run(main())
