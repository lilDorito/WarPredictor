from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

import re
import json
import time
import random
from datetime import date, timedelta

# Driver Setup
def make_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    # Human-like fingerprinting
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    return webdriver.Chrome(options=options)

# Helpers
def human_delay(a=2.5, b=6.5):
    time.sleep(random.uniform(a, b))


def decode_unicode(text):
    return re.sub(
        r'\\u([0-9a-fA-F]{4})',
        lambda m: chr(int(m.group(1), 16)),
        text
    )

# Chart Extraction
def extract_chart_data(html, driver=None):
    html = decode_unicode(html)
    results = {}

    if driver is not None:
        try:
            js_data = driver.execute_script("""
                return window.chartData || window.__chartData || null;
            """)

            if js_data:
                results.update(js_data)
                return results

        except:
            pass

    # Alarms by region
    m = re.search(r"run_count.*?labels:\[([^\]]+)\].*?data:\[([^\]]+)\]", html, re.DOTALL)
    if m:
        labels = re.findall(r'"(.*?)"', m.group(1))
        data = re.findall(r'"?(\d+)"?', m.group(2))
        results["by_region_count"] = dict(zip(labels, map(int, data)))

    # Weekday stats
    m = re.search(r"Кількість тривог по днях тижня.*?data:\[([^\]]+)\]", html, re.DOTALL)
    if m:
        data = list(map(int, re.findall(r'\d+', m.group(1))))
        days = ['Понеділок', 'Вівторок', 'Середа', 'Четвер', "П'ятниця", 'Субота', 'Неділя']
        results["by_weekday"] = dict(zip(days, data))

    # Hour stats
    m = re.search(r"Кількість тривог по годинам.*?data:\[([^\]]+)\]", html, re.DOTALL)
    if m:
        data = list(map(int, re.findall(r'\d+', m.group(1))))
        hours = [f"{h:02d}:00" for h in range(24)]
        results["by_hour"] = dict(zip(hours, data))

    # Duration
    m = re.search(
        r"labels:\[([^\]]+)\],datasets:\[\{label:'Тривалість тривог[^']*',data:\[([^\]]+)\]",
        html,
        re.DOTALL
    )
    if m:
        labels = re.findall(r'"(.*?)"', m.group(1))
        data = re.findall(r'([\d.]+)', m.group(2))
        results["by_duration"] = dict(zip(labels, map(float, data)))

    # Explosions
    m = re.search(r"run_by_explosions_count.*?labels:\[([^\]]+)\].*?data:\[([^\]]+)\]", html, re.DOTALL)
    if m:
        labels = re.findall(r'"(.*?)"', m.group(1))
        data = re.findall(r'"?(\d+)"?', m.group(2))
        results["explosions_by_region"] = dict(zip(labels, map(int, data)))

    # Artillery
    m = re.search(r"run_by_attacks_count.*?labels:\[([^\]]+)\].*?data:\[([^\]]+)\]", html, re.DOTALL)
    if m:
        labels = re.findall(r'"(.*?)"', m.group(1))
        data = re.findall(r'"?(\d+)"?', m.group(2))
        results["artillery_by_region"] = dict(zip(labels, map(int, data)))

    return results

# Period Scraper
def scrape_period(driver, from_date, to_date):
    url = f"https://air-alarms.in.ua/?from={from_date}&to={to_date}"

    for attempt in range(3):
        driver.get(url)

        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.ID, "by-count"))
            )
        except:
            pass

        human_delay()

        html = driver.page_source

        if "Just a moment" in html:
            print("  [!] Cloudflare detected — waiting longer...")
            time.sleep(20)
            continue

        return extract_chart_data(html, driver)

    return {}

def main():
    driver = make_driver()

    driver.get("https://air-alarms.in.ua")
    human_delay(4, 8)

    all_results = []

    start = date(2022, 2, 24)
    end = date(2026, 3, 1)

    current = start

    while current < end:
        next_month = (current.replace(day=1) + timedelta(days=32)).replace(day=1)
        to_date = min(next_month - timedelta(days=1), end)

        print(f"Scraping {current} → {to_date}")

        try:
            data = scrape_period(driver, current.isoformat(), to_date.isoformat())

            data["period_from"] = str(current)
            data["period_to"] = str(to_date)

            all_results.append(data)

            print(f"  ✓ OK (fields: {len(data)})")

        except Exception as e:
            print(f"  ✗ Failed: {e}")

        human_delay()
        current = next_month

    driver.quit()

    with open("air_alarms_historical.json", "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)

    print(f"\nDone. Saved {len(all_results)} months.")


if __name__ == "__main__":
    main()
