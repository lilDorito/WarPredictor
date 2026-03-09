import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import time
import random
import os
from text_cleaner import clean_text as clean
from event_detector import detect_events

SUBREDDITS = ["ukraine", "worldnews", "combatfootage"]
OUTPUT_FILE = "reddit_data.csv"
SINCE = datetime.now(timezone.utc) - timedelta(days=1)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

def human_delay():
    time.sleep(random.uniform(0.5, 2.0))

def fetch_submissions(subreddit):
    posts = []
    after = None

    while True:
        url = f"https://www.reddit.com/r/{subreddit}/new.json?limit=100"
        if after:
            url += f"&after={after}"

        r = requests.get(url, headers=HEADERS)
        if r.status_code != 200:
            print(f"  [!] HTTP {r.status_code} on {subreddit}")
            break

        data = r.json()["data"]
        children = data["children"]

        if not children:
            break

        for child in children:
            post = child["data"]
            created = datetime.fromtimestamp(post["created_utc"], tz=timezone.utc)
            if created < SINCE:
                return posts
            posts.append(post)

        after = data.get("after")
        if not after:
            break

        human_delay()

    return posts

def fetch_comments(subreddit, post_id):
    url = f"https://www.reddit.com/r/{subreddit}/comments/{post_id}.json?limit=500"
    r = requests.get(url, headers=HEADERS)
    if r.status_code != 200:
        return []

    try:
        listing = r.json()[1]["data"]["children"]
    except (IndexError, KeyError):
        return []

    comments = []
    for child in listing:
        if child["kind"] != "t1":
            continue
        comments.append(child["data"])

    return comments

def load_existing_ids():
    if os.path.exists(OUTPUT_FILE):
        df = pd.read_csv(OUTPUT_FILE, usecols=["id"])
        return set(df["id"].tolist())
    return set()

def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] Start collecting...")

    existing_ids = load_existing_ids()
    rows = []

    for subreddit in SUBREDDITS:
        print(f"  Fetching r/{subreddit}...")
        posts = fetch_submissions(subreddit)
        print(f"  {len(posts)} posts in last 24h")

        for post in posts:
            if post["id"] in existing_ids:
                continue

            text = f"{post.get('title', '')} {post.get('selftext', '')}"
            cleaned = clean(text)
            events = detect_events(cleaned)

            if events:
                rows.append({
                    "id": post["id"],
                    "author": post.get("author"),
                    "subreddit": post.get("subreddit"),
                    "created_utc": post.get("created_utc"),
                    "score": post.get("score"),
                    "body": cleaned,
                    "events": ",".join(sorted(events)),
                    "source": "RS"
                })
                existing_ids.add(post["id"])

            human_delay()
            comments = fetch_comments(subreddit, post["id"])

            for comment in comments:
                if comment["id"] in existing_ids:
                    continue
                cleaned_c = clean(comment.get("body", ""))
                events_c = detect_events(cleaned_c)
                if not events_c:
                    continue
                rows.append({
                    "id": comment["id"],
                    "author": comment.get("author"),
                    "subreddit": subreddit,
                    "created_utc": comment.get("created_utc"),
                    "score": comment.get("score"),
                    "body": cleaned_c,
                    "events": ",".join(sorted(events_c)),
                    "source": "RC"
                })
                existing_ids.add(comment["id"])

    if not rows:
        print("Nothing collected.")
        return

    df = pd.DataFrame(rows)
    file_exists = os.path.exists(OUTPUT_FILE)
    df.to_csv(OUTPUT_FILE, mode="a", index=False, header=not file_exists, encoding="utf-8")
    print(f"Added {len(df)} rows to {OUTPUT_FILE}")


main()
