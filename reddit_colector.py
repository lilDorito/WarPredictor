import praw
import pandas as pd
from datetime import datetime
import time
import re
import os

SUBREDDITS = ["ukraine", "worldnews", "combatfootage"]

KEYWORDS = [
    "missile", "rocket", "explosion", "strike",
    "artillery", "drone", "attack", "air defense",
    "вибух", "обстріл", "ракета", "приліт",
    "взрыв", "обстрел", "ракета", "прилет"
]

KEYWORDS_PATTERN = re.compile('|'.join(KEYWORDS), re.IGNORECASE)

OUTPUT_FILE = "/home/ec2-user/data/reddit_realtime.parquet" 

reddit = praw.Reddit(
    client_id="my client id",
    client_secret="my client secret",
    user_agent="ukraine-data-collector/1.0 by u/VoGnNk",
)

def load_existing_ids():
    if os.path.exists(OUTPUT_FILE):
        df = pd.read_parquet(OUTPUT_FILE, columns=["post_id"])
        return set(df["post_id"].tolist())
    return set()

def collect():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] Start colecting...")

    existing_ids = load_existing_ids()
    new_data = []

    for subreddit_name in SUBREDDITS:
        try:
            subreddit = reddit.subreddit(subreddit_name)

            for post in subreddit.new(limit=1000):
                if post.id in existing_ids:
                    continue

                full_text = (post.title or "") + " " + (post.selftext or "")

                if KEYWORDS_PATTERN.search(full_text):
                    new_data.append({
                        "date": datetime.utcfromtimestamp(post.created_utc),
                        "subreddit": subreddit_name,
                        "title": post.title,
                        "text": post.selftext,
                        "score": post.score,
                        "num_comments": post.num_comments,
                        "post_id": post.id,
                    })

            time.sleep(1)

        except Exception as e:
            print(f"Error in {subreddit_name}: {e}")
            time.sleep(5)

    if not new_data:
        print("New posts not found")
        return

    new_df = pd.DataFrame(new_data)
    new_df["date"] = pd.to_datetime(new_df["date"])

    if os.path.exists(OUTPUT_FILE):
        existing_df = pd.read_parquet(OUTPUT_FILE)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df

    combined_df = combined_df.sort_values("date", ascending=False)
    combined_df.to_parquet(OUTPUT_FILE, index=False)

    print(f"Added {len(new_data)} new posts. Current number: {len(combined_df)}")

collect()
