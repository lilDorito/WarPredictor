import requests
from datetime import date, timedelta, datetime
from bs4 import BeautifulSoup
import csv
import os

from isw_scraper import (
    WEB_HEADERS,
    OUTPUT_FILE,
    top_line_check,
    scrape_toplines,
    load_existing_dates,
    append_rows,
)

def get_latest_link() -> tuple[date, str] | None:
    url = "https://understandingwar.org/analysis/russia-ukraine/?_paged=1"
    response = requests.get(url, headers=WEB_HEADERS, timeout=10)
    soup = BeautifulSoup(response.content, "html.parser")
    articles = soup.find_all("div", class_="research-card-loop-item-3colgrid")

    for article in articles:
        topic = article.find('a')
        article_category_el = article.find('p', class_="research-card-category-label")
        if not (topic and article_category_el):
            continue
        text = topic.get_text().strip()
        article_category = article_category_el.get_text().strip()
        if "Russian Offensive Campaign Assessment" in text and article_category == "Update":
            article_link = topic["href"]
            article_date = text.replace(',', ' ').split()[4:]
            day = int(article_date[1])
            month = article_date[0]
            year = int(article_date[2]) if len(article_date) == 3 else date.today().year
            link_date = datetime.strptime(f"{day} {month} {year}", "%d %B %Y").date()
            return link_date, article_link

    return None

def run_daily() -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] ISW daily collector starting...")

    result = get_latest_link()
    if not result:
        print("[!] No ISW assessment found on page 1.")
        return

    report_date, link = result
    existing_dates = load_existing_dates()

    if str(report_date) in existing_dates:
        print(f"[SKIP] {report_date} already collected.")
        return

    try:
        toplines = scrape_toplines(link)
        append_rows([[str(report_date), toplines]])
        print(f"[+] Collected {report_date} — {len(toplines.split('/'))} toplines.")
    except Exception as e:
        print(f"[!] Failed to scrape {report_date}: {e}")

if __name__ == "__main__":
    run_daily()
