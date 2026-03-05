import sys
import json
import csv
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--subreddits", required=True)
parser.add_argument("--output", required=True)
args = parser.parse_args()

subreddits = set(args.subreddits.split(","))
outfile = args.output

exists = os.path.exists(outfile) and os.path.getsize(outfile) > 0
out = open(outfile, "a", newline="", encoding="utf-8")

writer = None
fields = ["id", "author", "subreddit", "created_utc", "score", "body"]

count = 0
matched = 0
errors = 0

for line in sys.stdin:
    count += 1
    try:
        row = json.loads(line)

        if row.get("subreddit") not in subreddits:
            continue

        if writer is None:
            writer = csv.DictWriter(out, fieldnames=fields, extrasaction="ignore")
            if not exists:
                writer.writeheader()

        writer.writerow(row)
        matched += 1

    except Exception:
        errors += 1
        continue

out.close()

print(f"[DONE] Lines: {count:,} | Matched: {matched:,} | Errors: {errors:,}")
