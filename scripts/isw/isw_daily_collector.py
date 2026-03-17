import requests
import os
import sys
import csv
from datetime import date, datetime
from bs4 import BeautifulSoup
from datetime import date, datetime, timedelta

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
OUTPUT_FILE = os.path.join(ROOT, "datasets", "isw", "isw_daily.csv")
LOG_FILE = os.path.join(ROOT, "logs", "isw", "daily_collector.log")

sys.path.append(os.path.dirname(__file__))
from isw_scraper import WEB_HEADERS, scrape_toplines

def log(msg: str):
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def load_existing_dates() -> set:
    if not os.path.exists(OUTPUT_FILE):
        return set()
    with open(OUTPUT_FILE, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        return {row["date"] for row in reader}

def get_all_links() -> list[tuple[date, str]]:
    url = "https://understandingwar.org/analysis/russia-ukraine/?_paged=1"
    response = requests.get(url, headers=WEB_HEADERS, timeout=10)
    soup = BeautifulSoup(response.content, "html.parser")
    articles = soup.find_all("div", class_="research-card-loop-item-3colgrid")
    results = []
    for article in articles:
        topic = article.find("a")
        category_el = article.find("p", class_="research-card-category-label")
        if not (topic and category_el):
            continue
        text = topic.get_text().strip()
        category = category_el.get_text().strip()
        if "Russian Offensive Campaign Assessment" not in text or category != "Update":
            continue
        try:
            parts = text.replace(",", " ").split()[4:]
            day   = int(parts[1])
            month = parts[0]
            year  = int(parts[2]) if len(parts) == 3 else date.today().year
            link_date = datetime.strptime(f"{day} {month} {year}", "%d %B %Y").date()
            results.append((link_date, topic["href"]))
        except Exception as e:
            log(f"[!] Failed to parse article date from '{text}': {e}")
    return results

def append_row(report_date: date, toplines: str) -> None:
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["date", "toplines"])
        writer.writerow([str(report_date), toplines])

def run_daily() -> None:
    log("> ISW daily collector starting <")

    yesterday = (datetime.now() - timedelta(days=1)).date()
    # yesterday = (datetime.now() - timedelta(days=2)).date()

    links = get_all_links()
    if not links:
        log("[!] No ISW assessments found on page 1.")
        return

    log(f"[i] Found {len(links)} article(s) on page 1.")

    match = next(((d, link) for d, link in links if d == yesterday), None)
    if not match:
        log(f"[!] No ISW report found for yesterday ({yesterday}).")
        return

    try:
        toplines = scrape_toplines(match[1])
        append_row(yesterday, toplines)
        log(f"[+] Collected {yesterday} - {len(toplines.split('/'))} toplines -> {OUTPUT_FILE}")
    except Exception as e:
        log(f"[!] Failed to scrape {yesterday}: {e}")

    log("Done.\n")

if __name__ == "__main__":
    run_daily()
