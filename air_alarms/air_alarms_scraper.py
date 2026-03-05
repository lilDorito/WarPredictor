from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

import re
import json
import time
import random
import os
from datetime import date, timedelta

def make_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    return webdriver.Chrome(options=options)

def human_delay(a=2.5, b=6.5):
    time.sleep(random.uniform(a, b))

def decode_unicode(text):
    return re.sub(
        r'\\u([0-9a-fA-F]{4})',
        lambda m: chr(int(m.group(1), 16)),
        text
    )

def load_checkpoint(path="checkpoint.json"):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"[i] Resuming from checkpoint — {len(data)} months already scraped.")
        return data
    return []

def save_checkpoint(results, path="checkpoint.json"):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

def extract_chart_data(html):
    html = decode_unicode(html)
    results = {}

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
    m = re.search(r"labels:\[([^\]]+)\],datasets:\[\{label:'Тривалість тривог[^']*',data:\[([^\]]+)\]", html, re.DOTALL)
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

def scrape_period(driver, from_date, to_date, max_retries=3):
    url = f"https://air-alarms.in.ua/?from={from_date}&to={to_date}"

    for attempt in range(1, max_retries + 1):
        try:
            driver.get(url)

            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.ID, "by-count"))
            )

            human_delay()
            html = driver.page_source

            if "Just a moment" in html:
                print(f"  [!] Cloudflare detected (attempt {attempt}/{max_retries}) — waiting 20s...")
                time.sleep(20)
                continue

            data = extract_chart_data(html)

            if not data:
                print(f"  [!] Empty data on attempt {attempt}/{max_retries} — retrying...")
                human_delay(3, 7)
                continue

            return data

        except Exception as e:
            print(f"  [!] Attempt {attempt}/{max_retries} failed: {e}")
            human_delay(3, 7)

    print(f"  [x] All {max_retries} attempts failed for {from_date} → {to_date}")
    return {}

def main():
    OUTPUT_FILE = "air_alarms_historical.json"
    CHECKPOINT_FILE = "checkpoint.json"

    all_results = load_checkpoint(CHECKPOINT_FILE)
    scraped_periods = {r["period_from"] for r in all_results}

    start = date(2022, 2, 24)
    end = date(2026, 3, 1)
    current = start

    driver = make_driver()

    try:
        driver.get("https://air-alarms.in.ua")
        human_delay(4, 8)

        while current < end:
            next_month = (current.replace(day=1) + timedelta(days=32)).replace(day=1)
            to_date = min(next_month - timedelta(days=1), end)

            period_key = str(current)

            if period_key in scraped_periods:
                print(f"Skipping {current} -> {to_date} (already scraped)")
                current = next_month
                continue

            print(f"Scraping {current} -> {to_date} ...", end=" ", flush=True)

            data = scrape_period(driver, current.isoformat(), to_date.isoformat())

            data["period_from"] = str(current)
            data["period_to"] = str(to_date)

            all_results.append(data)
            scraped_periods.add(period_key)

            fields = len([k for k in data if k not in ("period_from", "period_to")])
            print(f"[+] ({fields} chart fields extracted)")

            save_checkpoint(all_results, CHECKPOINT_FILE)

            human_delay()
            current = next_month

    except KeyboardInterrupt:
        print("\n[i] Interrupted by user - progress saved to checkpoint.")

    finally:
        driver.quit()

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)

    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)

    print(f"\nDone. Saved {len(all_results)} months to {OUTPUT_FILE}.")


if __name__ == "__main__":
    main()
