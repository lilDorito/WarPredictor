from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

import time
import os
import glob
import json
import shutil
from datetime import date, timedelta
import calendar

DOWNLOAD_DIR = os.path.abspath("alert_downloads")
CHECKPOINT_FILE = "alerts_checkpoint.json"
OUTPUT_DIR = os.path.abspath("alert_csvs")

def make_driver():
    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")

    prefs = {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }

    options.add_experimental_option("prefs", prefs)
    return webdriver.Chrome(options=options)

def wait_for_app(driver, timeout=40):
    WebDriverWait(driver, timeout).until(lambda d: len(d.find_elements(By.CSS_SELECTOR, ".vc-container")) > 0)
    time.sleep(4)

def navigate_to_month(driver, target_date, current_display: list):
    displayed = current_display[0]
    months_delta = (displayed.year - target_date.year) * 12 + (displayed.month - target_date.month)

    if months_delta > 0:
        for _ in range(months_delta):
            prev_btn = driver.find_element(By.CSS_SELECTOR, ".vc-arrow.is-left")
            driver.execute_script("arguments[0].click();", prev_btn)
            time.sleep(0.25)
    elif months_delta < 0:
        for _ in range(abs(months_delta)):
            next_btn = driver.find_element(By.CSS_SELECTOR, ".vc-arrow.is-right")
            driver.execute_script("arguments[0].click();", next_btn)
            time.sleep(0.25)

    current_display[0] = target_date.replace(day=1)

def click_day(driver, target_date):
    try:
        selector = (
            f'.vc-day.id-{target_date.isoformat()}'
            ':not(.is-not-in-month):not(.is-disabled) .vc-day-content'
        )
        day_el = WebDriverWait(driver, 10).until(
            lambda d: d.find_element(By.CSS_SELECTOR, selector)
        )
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", day_el)
        driver.execute_script("arguments[0].click();", day_el)
        time.sleep(2)
        return True
    except Exception as e:
        print(f"[!] Could not click day {target_date}: {e}")
        return False

def click_export(driver) -> bool:
    try:
        links = WebDriverWait(driver, 15).until(
            lambda d: [
                a for a in d.find_elements(By.CSS_SELECTOR, "a")
                if "CSV" in (a.text or "") or "Експортувати" in (a.text or "")
            ]
        )
        if links:
            driver.execute_script("arguments[0].click();", links[0])
            return True
    except Exception as e:
        print(f"[!] Export click error: {e}")
    return False

def wait_for_download(timeout=40):
    start = time.time()
    while time.time() - start < timeout:
        files = [
            f for f in glob.glob(os.path.join(DOWNLOAD_DIR, "*.csv"))
            if not f.endswith(".crdownload")
        ]
        if files:
            time.sleep(0.5)
            return files[0]
        time.sleep(0.5)
    return None

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            data = json.load(f)
        print(f"[i] Resuming - {len(data)} days already downloaded.")
        return set(data)
    return set()

def save_checkpoint(done):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(sorted(done), f)

def main():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    scraped = load_checkpoint()

    start = date(2022, 2, 24)
    end = date.today() - timedelta(days=1)
    total_days = (end - start).days + 1

    print(f"Downloading alerts.in.ua: {start} -> {end} ({total_days} days)")
    print(f"Already done: {len(scraped)} days")
    print(f"CSVs -> {OUTPUT_DIR}\n")

    driver = make_driver()

    current_display = [date.today().replace(day=1)]

    try:
        driver.get("https://alerts.in.ua")
        wait_for_app(driver)
        print("[i] App loaded.\n")

        current = start
        done_counter = 0

        while current <= end:
            date_key = str(current)
            done_counter += 1

            if date_key in scraped:
                print(f"[SKIP] {date_key}")
                current += timedelta(days=1)
                continue

            print(f"[{done_counter}/{total_days}] {date_key}...", end=" ", flush=True)

            navigate_to_month(driver, current, current_display)

            for f in glob.glob(os.path.join(DOWNLOAD_DIR, "*.csv")):
                os.remove(f)

            if not click_day(driver, current):
                current += timedelta(days=1)
                continue

            if not click_export(driver):
                print("[!] Export button not found")
                current += timedelta(days=1)
                continue

            path = wait_for_download()

            if path:
                dest = os.path.join(OUTPUT_DIR, f"alerts_{date_key}.csv")
                shutil.move(path, dest)
                print(f"[+] {os.path.getsize(dest)} bytes")
                scraped.add(date_key)
                save_checkpoint(scraped)
            else:
                print("[!] Download timed out")

            time.sleep(0.6)
            current += timedelta(days=1)

    except KeyboardInterrupt:
        print("\n[i] Interrupted - progress saved.")
    finally:
        driver.quit()

    print(f"\nDone. {len(scraped)} files in {OUTPUT_DIR}")

if __name__ == "__main__":
    main()
  
