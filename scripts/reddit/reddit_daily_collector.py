import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import time
import random
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
OUTPUT_FILE = os.path.join(ROOT, "datasets", "reddit", "reddit_daily.csv")
LOG_FILE = os.path.join(ROOT, "logs", "reddit", "daily_collector.log")

sys.path.append(os.path.join(ROOT, "scripts"))
from util.text_cleaner import clean_text as clean
from util.event_detector import detect_events

SUBREDDITS = ["ukraine", "worldnews"]
today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
SINCE = today - timedelta(days=1)
UNTIL = today
# SINCE = today - timedelta(days=2)
# UNTIL = today - timedelta(days=1)
HEADERS = {"User-Agent": "conflict-event-backfill/1.0"}
ARCTIC = "https://arctic-shift.photon-reddit.com/api"

def log(msg: str):
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def human_delay():
    time.sleep(random.uniform(0.5, 1.5))

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
            log(f"[!] HTTP {r.status_code} on r/{subreddit} - {r.json()}")
            break
        data = r.json().get("data", [])
        if not data:
            break
        data.sort(key=lambda p: int(float(p.get("created_utc", 0))), reverse=True)
        posts.extend(data)
        oldest_ts = int(float(data[-1]["created_utc"]))
        if oldest_ts <= since_ts:
            break
        before_ts = oldest_ts
        human_delay()
    return posts

def fetch_comments(post_id):
    r = requests.get(f"{ARCTIC}/comments/search", headers=HEADERS,
                     params={"link_id": f"t3_{post_id}", "limit": 100})
    return r.json().get("data") or [] if r.status_code == 200 else []

def main():
    log("> Reddit daily collector starting <")
    log(f"Window: {SINCE.date()} 00:00 UTC -> {UNTIL.date()} 00:00 UTC")

    rows = []
    seen_ids = set()
    since_ts, until_ts = int(SINCE.timestamp()), int(UNTIL.timestamp())

    for subreddit in SUBREDDITS:
        log(f"Fetching r/{subreddit}...")
        posts = fetch_posts(subreddit)
        log(f"{len(posts)} posts fetched from r/{subreddit}")

        for post in posts:
            pid = str(post.get("id", ""))
            if pid not in seen_ids:
                text = f"{post.get('title', '')} {post.get('selftext', '') or ''}"
                cleaned = clean(text)
                events = detect_events(cleaned)
                if events:
                    rows.append({
                        "id": pid,
                        "author": post.get("author"),
                        "subreddit": subreddit,
                        "created_utc": int(float(post.get("created_utc", 0))),
                        "score": post.get("score"),
                        "body": cleaned,
                        "source": "RS",
                        "events": ",".join(sorted(events))
                    })
                seen_ids.add(pid)

            human_delay()
            for comment in fetch_comments(pid):
                cid = str(comment.get("id", ""))
                if cid in seen_ids:
                    continue
                created = int(float(comment.get("created_utc", 0)))
                if not (since_ts <= created < until_ts):
                    continue
                cleaned_c = clean(comment.get("body", ""))
                events_c = detect_events(cleaned_c)
                if not events_c:
                    continue
                rows.append({
                    "id": cid,
                    "author": comment.get("author"),
                    "subreddit": subreddit,
                    "created_utc": created,
                    "score": comment.get("score"),
                    "body": cleaned_c,
                    "source": "RC",
                    "events": ",".join(sorted(events_c))
                })
                seen_ids.add(cid)

    if not rows:
        log("Nothing collected.")
        return

    df = pd.DataFrame(rows)
    df = df.sort_values("created_utc").reset_index(drop=True)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df["created_utc"] = pd.to_datetime(df["created_utc"], unit="s", utc=True).dt.tz_localize(None)
    df.to_csv(OUTPUT_FILE, mode="w", index=False, header=True, encoding="utf-8")
    log(f"Collected {len(df)} rows -> {OUTPUT_FILE}")
    log("Done.\n")

if __name__ == "__main__":
    main()