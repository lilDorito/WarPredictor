import requests
from datetime import date, timedelta, datetime
from bs4 import BeautifulSoup
import csv
import os


def get_links(links: list) -> None:
    url = "https://understandingwar.org/analysis/russia-ukraine/?_paged="
    web_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    }
    page_num = 1
    end_cycle = False

    while True:
        response = requests.get(f"{url}{page_num}", headers = web_headers, timeout = 10)
        soup = BeautifulSoup(response.content, "html.parser")
        articles = soup.find_all("div", class_ = "research-card-loop-item-3colgrid")

        if articles:
            for article in articles:
                topic = article.find('a')
                article_category_el = article.find('p', class_ = "research-card-category-label")

                if topic and article_category_el:
                    text = topic.get_text().strip()
                    article_category = article_category_el.get_text().strip()

                    if "Russian Offensive Campaign Assessment" in text and article_category == "Update":
                        article_link = topic["href"]
                        article_date = text.replace(',', ' ')
                        article_date = article_date.split()[4:]

                        day = int(article_date[1])
                        month = article_date[0]
                        if len(article_date) == 3:
                            year = int(article_date[2])
                        else:
                            year = (links[0][0] - timedelta(days = 1)).year

                        link_date = datetime.strptime(f"{day} {month} {year}", "%d %B %Y").date()

                        links.insert(0, [link_date, article_link])

                        if link_date == date(2022, 3, 1):
                            end_cycle = True
                            break

        if end_cycle:
            break
        page_num += 1

def top_line_check(top_line: str) -> bool:
    banned_words = ["Note: ", "https:", "Click", "to see ISW’s", "interactive map",
                    "This map", "Karolina", "Kateryna", "to access ISW’s", "These maps complement",
                    "ISW will update this time-lapse map", "Grace", "Frederick", "Christina", "ISW"]
    return 70 < len(top_line) < 350 and not any(word in top_line for word in banned_words)

def get_isw_data() -> None:
    link_list = []
    get_links(link_list)

    web_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    }

    rows = []

    for [date, link] in link_list:
        try:
            response = requests.get(link, headers = web_headers, timeout = 10)
            soup = BeautifulSoup(response.content, "html.parser")
            top_lines = soup.find_all("strong")
            top_line_list = []

            if top_lines:
                for top_line in top_lines:
                    text = top_line.get_text().strip()

                    if top_line_check(text):
                        top_line_list.append(text)

            all_toplines_str = "/".join(top_line_list)
            rows.append([str(date), all_toplines_str])

        except Exception as e:
            pass

    file_name = "isw_data.csv"
    try:
        with open(file_name, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f, delimiter = ';')
            writer.writerow(["date", "toplines"])
            writer.writerows(rows)
    except IOError as e:
        print(f"[!] Error saving file: {e}")

if __name__ == "__main__":
    get_isw_data()
