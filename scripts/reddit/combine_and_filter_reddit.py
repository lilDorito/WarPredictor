import pandas as pd
import os
from text_cleaner import clean_text as clean
from event_detector import detect_events

INPUT_DIR = "output"
OUTPUT_FILE = "reddit_data.csv"

def process_file(path: str, source: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str)
    df = df[["id", "author", "subreddit", "created_utc", "score", "body"]].copy()
    df["source"] = source
    df = df.dropna(subset=["body"])
    df = df[df["body"].str.strip() != ""]
    df = df[~df["body"].isin(["[removed]", "[deleted]"])]
    df["body"] = df["body"].apply(clean)
    df["events"] = df["body"].apply(detect_events).apply(lambda e: ",".join(sorted(e)))
    df = df[df["events"] != ""]
    return df

frames = []

for filename in sorted(os.listdir(INPUT_DIR)):
    if not filename.endswith(".csv"):
        continue
    path = os.path.join(INPUT_DIR, filename)
    if filename.startswith("RC_"):
        source = "RC"
    elif filename.startswith("RS_"):
        source = "RS"
    else:
        continue
    print(f"Processing {filename}...", end=" ", flush=True)
    df = process_file(path, source)
    print(f"{len(df)} rows matched")
    frames.append(df)

if not frames:
    print("No files found.")
else:
    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["id"])
    combined = combined.sort_values("created_utc", ascending=False)
    combined.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
    print(f"\nDone. {len(combined)} total rows saved to {OUTPUT_FILE}")
