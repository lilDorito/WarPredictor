import requests
import pandas as pd
from datetime import datetime, timezone
import time
import random
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
OUTPUT_FILE = os.path.join(ROOT, "datasets", "reddit", "reddit_data.csv")

sys.path.append(os.path.join(ROOT, "scripts"))
from util.text_cleaner import clean_text as clean
from util.event_detector import detect_events

SUBREDDITS = ["ukraine", "worldnews"]
SINCE = datetime(2026, 3, 1, tzinfo=timezone.utc)
UNTIL = datetime(2026, 3, 10, tzinfo=timezone.utc)
HEADERS = {"User-Agent": "conflict-event-backfill/1.0"}
ARCTIC = "https://arctic-shift.photon-reddit.com/api"

def fetch_posts(subreddit):
    posts = []
    before_ts = int(UNTIL.timestamp())
    since_ts = int(SINCE.timestamp())

    while True:
        r = requests.get(f"{ARCTIC}/posts/search", headers=HEADERS, params={
            "subreddit": subreddit,
            "after": since_ts,
            "before": before_ts,
            "limit": 100,
        })
        if r.status_code != 200:
            print(f"  [!] HTTP {r.status_code} - {r.json()}")
            break
        data = r.json().get("data", [])
        if not data:
            break

        data.sort(key=lambda p: int(p.get("created_utc", 0)), reverse=True)
        posts.extend(data)

        oldest_ts = int(data[-1]["created_utc"])
        print(f"  ... {len(posts)} posts fetched, oldest at {datetime.fromtimestamp(oldest_ts, tz=timezone.utc)}")

        if oldest_ts <= since_ts:
            break
        before_ts = oldest_ts
        time.sleep(random.uniform(0.4, 1.0))

    return posts

def fetch_comments(post_id):
    r = requests.get(f"{ARCTIC}/comments/search", headers=HEADERS,
                     params={"link_id": f"t3_{post_id}", "limit": 500})
    return r.json().get("data", []) if r.status_code == 200 else []

def main():
    existing_ids = set()
    if os.path.exists(OUTPUT_FILE):
        existing_ids = set(pd.read_csv(OUTPUT_FILE, usecols=["id"])["id"].astype(str))
    print(f"Existing IDs loaded: {len(existing_ids)}")

    rows = []

    for subreddit in SUBREDDITS:
        print(f"\nFetching r/{subreddit}...")
        posts = fetch_posts(subreddit)
        print(f"  -> {len(posts)} posts fetched total")

        for post in posts:
            pid = str(post.get("id", ""))
            if pid in existing_ids:
                continue
            text = f"{post.get('title', '')} {post.get('selftext', '') or ''}"
            cleaned = clean(text)
            events = detect_events(cleaned)
            if events:
                rows.append({
                    "id": pid, "author": post.get("author"),
                    "subreddit": subreddit, "created_utc": datetime.fromtimestamp(int(post.get("created_utc")), tz=timezone.utc),
                    "score": post.get("score"), "body": cleaned,
                    "events": ",".join(sorted(events)), "source": "RS"
                })
                existing_ids.add(pid)

            time.sleep(random.uniform(0.3, 0.8))
            for comment in fetch_comments(pid):
                cid = str(comment.get("id", ""))
                if cid in existing_ids:
                    continue
                created = int(comment.get("created_utc", 0))
                if not (int(SINCE.timestamp()) <= created < int(UNTIL.timestamp())):
                    continue
                cleaned_c = clean(comment.get("body", ""))
                events_c = detect_events(cleaned_c)
                if events_c:
                    rows.append({
                        "id": cid, "author": comment.get("author"),
                        "subreddit": subreddit, "created_utc": datetime.fromtimestamp(created, tz=timezone.utc),
                        "score": comment.get("score"), "body": cleaned_c,
                        "events": ",".join(sorted(events_c)), "source": "RC"
                    })
                    existing_ids.add(cid)

        print(f"  -> r/{subreddit} done, {len(rows)} total rows so far")

    if rows:
        df = pd.DataFrame(rows)
        file_exists = os.path.exists(OUTPUT_FILE)
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        df.to_csv(OUTPUT_FILE, mode="a", index=False, header=not file_exists, encoding="utf-8-sig")
        print(f"\nDone - {len(df)} rows added to {OUTPUT_FILE}")
    else:
        print("\nNothing collected.")

if __name__ == "__main__":
    main()