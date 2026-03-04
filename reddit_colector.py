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
    "alert", "all clear",
    "вибух", "обстріл", "ракета", "приліт",
    "артилерія", "дрон", "атака", "протиповітряна оборона",
    "тривога", "відбій",
    "взрыв", "обстрел", "ракета", "прилет",
    "артиллерия", "дрон", "атака", "противовоздушная оборона",
    "тревога", "отбой",
]

KEYWORDS_PATTERN = re.compile('|'.join(KEYWORDS), re.IGNORECASE)

OUTPUT_FILE_POST = "/home/ec2-user/data/reddit_realtime_post.parquet"
OUTPUT_FILE_COMMENTS = "/home/ec2-user/data/reddit_realtime_comments.parquet"

reddit = praw.Reddit(
    client_id="my client id",
    client_secret="my client secret",
    user_agent="ukraine-data-collector/1.0 by u/VoGnNk",
)


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


def load_existing_ids():
    ids = {"posts": set(), "comments": set()}

    if os.path.exists(OUTPUT_FILE_POST):
        ids["posts"] = set(pd.read_parquet(OUTPUT_FILE_POST, columns=["post_id"])["post_id"])

    if os.path.exists(OUTPUT_FILE_COMMENTS):
        ids["comments"] = set(pd.read_parquet(OUTPUT_FILE_COMMENTS, columns=["comment_id"])["comment_id"])

    return ids


def save_parquet(new_data, output_file, id_col):
    if not new_data:
        return 0

    new_df = pd.DataFrame(new_data)
    new_df["date"] = pd.to_datetime(new_df["date"])

    if os.path.exists(output_file):
        existing_df = pd.read_parquet(output_file)
        combined_df = (
            pd.concat([existing_df, new_df], ignore_index=True)
            .drop_duplicates(subset=[id_col])
            .sort_values("date", ascending=False)
        )
    else:
        combined_df = new_df.sort_values("date", ascending=False)

    combined_df.to_parquet(output_file, index=False)
    return len(new_df)


def collect():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] Start collecting...")

    existing_ids = load_existing_ids()
    new_post_data = []
    new_comment_data = []

    for subreddit_name in SUBREDDITS:
        try:
            subreddit = reddit.subreddit(subreddit_name)

            for post in subreddit.new(limit=1000):
                if post.id in existing_ids["posts"]:
                    continue

                full_text = (post.title or "") + " " + (post.selftext or "")

                if KEYWORDS_PATTERN.search(full_text):
                    new_post_data.append({
                        "date": datetime.utcfromtimestamp(post.created_utc),
                        "subreddit": subreddit_name,
                        "title": clean_for_alert_prediction(post.title),
                        "text": clean_for_alert_prediction(post.selftext),
                        "score": post.score,
                        "num_comments": post.num_comments,
                        "post_id": post.id,
                    })

                post.comments.replace_more(limit=0)

                for comment in post.comments.list():
                    if comment.id in existing_ids["comments"]:
                        continue
                    if KEYWORDS_PATTERN.search(comment.body):
                        new_comment_data.append({
                            "date": datetime.utcfromtimestamp(comment.created_utc),
                            "text": clean_for_alert_prediction(comment.body),
                            "score": comment.score,
                            "comment_id": comment.id,
                            "post_id": post.id,
                        })

            time.sleep(1)

        except Exception as e:
            print(f"Error in {subreddit_name}: {e}")
            time.sleep(5)

    if not new_post_data and not new_comment_data:
        print("New posts not found")
        return

    added_posts = save_parquet(new_post_data, OUTPUT_FILE_POST, "post_id")
    added_comments = save_parquet(new_comment_data, OUTPUT_FILE_COMMENTS, "comment_id")

    if added_posts:
        print(f"Added {added_posts} new posts")
    if added_comments:
        print(f"Added {added_comments} new comments")


collect()