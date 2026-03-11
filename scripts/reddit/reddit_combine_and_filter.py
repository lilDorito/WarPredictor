import pandas as pd
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
INPUT_DIR = os.path.join(ROOT, "datasets", "reddit", "raw")
OUTPUT_FILE = os.path.join(ROOT, "datasets", "reddit", "reddit_data.csv")

sys.path.append(os.path.join(ROOT, "scripts"))
from util.text_cleaner import clean_text as clean
from util.event_detector import detect_events

COLUMNS = ["id", "author", "subreddit", "created_utc", "score", "body"]

def process_file(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, header=None, names=COLUMNS)
    df["source"] = "RC"
    df = df.dropna(subset=["body"])
    df = df[df["body"].str.strip() != ""]
    df = df[~df["body"].isin(["[removed]", "[deleted]"])]
    df["body"] = df["body"].apply(clean)
    df["events"] = df["body"].apply(detect_events).apply(lambda e: ",".join(sorted(e)))
    df = df[df["events"] != ""]
    df["created_utc"] = pd.to_datetime(df["created_utc"].astype(float).astype(int), unit="s", utc=True)
    return df

frames = []
for filename in sorted(os.listdir(INPUT_DIR)):
    if not filename.startswith("RC_") or not filename.endswith(".csv"):
        continue
    path = os.path.join(INPUT_DIR, filename)
    print(f"Processing {filename}...", end=" ", flush=True)
    df = process_file(path)
    print(f"{len(df)} rows matched")
    frames.append(df)

if not frames:
    print("No files found.")
else:
    combined = pd.concat(frames, ignore_index=True)
    combined.drop_duplicates(subset=["id"], keep="first", inplace=True)
    combined.sort_values("created_utc", inplace=True)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    combined.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print(f"\nDone. {len(combined)} total rows -> {OUTPUT_FILE}")
