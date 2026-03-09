import requests
from datetime import date, timedelta, datetime
from bs4 import BeautifulSoup
import csv
import os

OUTPUT_FILE = "isw_data.csv"

WEB_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
}

def top_line_check(top_line: str) -> bool:
    banned_words = ["Note: ", "https:", "Click", "to see ISW's", "interactive map",
                    "This map", "Karolina", "Kateryna", "to access ISW's", "These maps complement",
                    "ISW will update this time-lapse map", "Grace", "Frederick", "Christina", "ISW"]
    return 70 < len(top_line) < 350 and not any(word in top_line for word in banned_words)

def get_links(links: list) -> None:
    url = "https://understandingwar.org/analysis/russia-ukraine/?_paged="
    page_num = 1
    end_cycle = False
    while True:
        response = requests.get(f"{url}{page_num}", headers=WEB_HEADERS, timeout=10)
        soup = BeautifulSoup(response.content, "html.parser")
        articles = soup.find_all("div", class_="research-card-loop-item-3colgrid")
        if articles:
            for article in articles:
                topic = article.find('a')
                article_category_el = article.find('p', class_="research-card-category-label")
                if topic and article_category_el:
                    text = topic.get_text().strip()
                    article_category = article_category_el.get_text().strip()
                    if "Russian Offensive Campaign Assessment" in text and article_category == "Update":
                        article_link = topic["href"]
                        article_date = text.replace(',', ' ').split()[4:]
                        day = int(article_date[1])
                        month = article_date[0]
                        if len(article_date) == 3:
                            year = int(article_date[2])
                        else:
                            year = (links[0][0] - timedelta(days=1)).year
                        link_date = datetime.strptime(f"{day} {month} {year}", "%d %B %Y").date()
                        links.insert(0, [link_date, article_link])
                        if link_date == date(2022, 3, 1):
                            end_cycle = True
                            break
        if end_cycle:
            break
        page_num += 1

def scrape_toplines(link: str) -> str:
    response = requests.get(link, headers=WEB_HEADERS, timeout=10)
    soup = BeautifulSoup(response.content, "html.parser")
    top_lines = soup.find_all("strong")
    top_line_list = [
        tl.get_text().strip()
        for tl in top_lines
        if top_line_check(tl.get_text().strip())
    ]
    return "/".join(top_line_list)

def load_existing_dates() -> set:
    if not os.path.exists(OUTPUT_FILE):
        return set()
    with open(OUTPUT_FILE, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        return {row["date"] for row in reader}

def append_rows(rows: list) -> None:
    file_exists = os.path.exists(OUTPUT_FILE)
    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, delimiter=";")
        if not file_exists:
            writer.writerow(["date", "toplines"])
        writer.writerows(rows)

def get_isw_data() -> None:
    link_list = []
    get_links(link_list)

    existing_dates = load_existing_dates()
    rows = []

    for [report_date, link] in link_list:
        if str(report_date) in existing_dates:
            print(f"[SKIP] {report_date} already in dataset")
            continue
        try:
            toplines = scrape_toplines(link)
            rows.append([str(report_date), toplines])
            print(f"[+] {report_date}")
        except Exception as e:
            print(f"[!] {report_date}: {e}")

    if rows:
        append_rows(rows)
        print(f"\nDone. Added {len(rows)} reports to {OUTPUT_FILE}")
    else:
        print("Nothing new to add.")

if __name__ == "__main__":
    get_isw_data()
