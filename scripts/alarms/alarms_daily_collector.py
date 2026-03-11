import requests
import pandas as pd
import time
import os
import sys
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
OUTPUT_FILE = os.path.join(ROOT, "datasets", "alarms", "alarms_daily.csv")
LOG_FILE = os.path.join(ROOT, "logs", "alarms", "daily_collector.log")

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from util.regions import REGIONS, REGION_FIXES

load_dotenv(os.path.join(ROOT, ".env"))
API_KEY = os.getenv("ALERTS_IN_UA_API_KEY")
BASE_URL = "https://api.alerts.in.ua/v1/regions/{uid}/alerts/month_ago.json"
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

REQUEST_DELAY = 31

OBLAST_UIDS = {
    "Вінницька область":         4,
    "Волинська область":         8,
    "Дніпропетровська область":  9,
    "Донецька область":          28,
    "Житомирська область":       10,
    "Закарпатська область":      11,
    "Запорізька область":        12,
    "Івано-Франківська область": 13,
    "Київська область":          14,
    "Кіровоградська область":    15,
    "Луганська область":         16,
    "Львівська область":         27,
    "Миколаївська область":      17,
    "Одеська область":           18,
    "Полтавська область":        19,
    "Рівненська область":        5,
    "Сумська область":           20,
    "Тернопільська область":     21,
    "Харківська область":        22,
    "Херсонська область":        23,
    "Хмельницька область":       3,
    "Черкаська область":         24,
    "Чернівецька область":       26,
    "Чернігівська область":      25,
    "м. Київ":                   31,
}

ALARM_TYPE_MAP = {
    "air_raid": "Повітряна тривога",
    "artillery_shelling": "Загроза артобстрілу",
    "urban_fights": "Вуличні бої",
    "nuclear": "Радіаційна загроза",
    "chemical": "Хімічна загроза",
    "informational": "Інформаційне повідомлення",
}

KEEP = ["alarm_start", "alarm_end", "region", "region_en", "alarm_type", "duration_min"]

def log(msg: str):
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def fetch_oblast(uid: int, retries: int = 3) -> list:
    url = BASE_URL.format(uid=uid)
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 429:
                log(f"  Rate limited on UID {uid}, sleeping 60s...")
                time.sleep(60)
                continue
            r.raise_for_status()
            return r.json().get("alerts", [])
        except requests.exceptions.RequestException as e:
            log(f"  [!] UID {uid} attempt {attempt + 1} failed: {e}")
            time.sleep(10)
    return []

def parse_dt(value: str | None) -> pd.Timestamp:
    if not value:
        return pd.NaT
    return pd.to_datetime(value, utc=True).tz_convert(None)

def parse_alert(alert: dict, region: str, since_dt: datetime, until_dt: datetime) -> dict | None:
    try:
        alarm_start = parse_dt(alert.get("started_at"))
        alarm_end = parse_dt(alert.get("finished_at"))

        if pd.isna(alarm_start):
            return None

        if not (since_dt <= alarm_start < until_dt):
            return None

        duration_min = (
            (alarm_end - alarm_start).total_seconds() / 60
            if pd.notna(alarm_end)
            else None
        )
        if duration_min is not None and duration_min <= 0:
            return None

        region_norm = REGION_FIXES.get(region, region)
        if region_norm not in REGIONS:
            log(f"  [!] Unknown region after fix: '{region_norm}' (original: '{region}')")
            return None

        raw_type   = alert.get("alert_type", "").lower()
        alarm_type = ALARM_TYPE_MAP.get(raw_type)
        if alarm_type is None:
            log(f"  [!] Unknown alarm_type: '{raw_type}' - skipping")
            return None

        return {
            "alarm_start": alarm_start,
            "alarm_end": alarm_end,
            "region": region_norm,
            "region_en": REGIONS[region_norm][2],
            "alarm_type": alarm_type,
            "duration_min": duration_min,
        }

    except Exception as e:
        log(f"  [!] Failed to parse alert: {e} | raw: {alert}")
        return None

def merge_overlapping(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.sort_values(["region", "alarm_type", "alarm_start"]).reset_index(drop=True)
    rows = df.to_dict("records")
    merged = []
    cur = rows[0].copy()
    for r in rows[1:]:
        same_group = (
            r["region"] == cur["region"]
            and r["alarm_type"] == cur["alarm_type"]
        )
        open_alarm = pd.isna(cur["alarm_end"])
        overlaps = (
            pd.notna(cur["alarm_end"])
            and r["alarm_start"] <= cur["alarm_end"]
        )
        if same_group and (overlaps or open_alarm):
            if pd.notna(r["alarm_end"]):
                if pd.isna(cur["alarm_end"]):
                    cur["alarm_end"] = r["alarm_end"]
                else:
                    cur["alarm_end"] = max(cur["alarm_end"], r["alarm_end"])
        else:
            merged.append(cur)
            cur = r.copy()
    merged.append(cur)
    return pd.DataFrame(merged)

def main():
    log("> Alarms daily collector starting (alerts.in.ua) <")

    if not API_KEY:
        log("[!] ALERTS_IN_UA_API_KEY not set in .env")
        return

    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    since_dt = (today - timedelta(days=1)).replace(tzinfo=None)
    until_dt = today.replace(tzinfo=None)
    log(f"Window: {since_dt} -> {until_dt}")

    rows = []
    n = len(OBLAST_UIDS)

    for i, (region, uid) in enumerate(OBLAST_UIDS.items(), 1):
        log(f"  [{i}/{n}] {region} (UID {uid})")
        alerts = fetch_oblast(uid)
        count = 0
        for alert in alerts:
            row = parse_alert(alert, region, since_dt, until_dt)
            if row:
                rows.append(row)
                count += 1
        log(f"    -> {count} alert(s) for yesterday")
        if i < n:
            time.sleep(REQUEST_DELAY)

    COMBINED_FILE = os.path.join(ROOT, "datasets", "alarms", "alarms_combined.csv")
    PERMANENT_REGIONS = {"Луганська область", "Автономна Республіка Крим"}

    if os.path.exists(COMBINED_FILE):
        try:
            hist = pd.read_csv(COMBINED_FILE, encoding="utf-8-sig")
            hist["alarm_start"] = pd.to_datetime(hist["alarm_start"])
            hist["alarm_end"] = pd.to_datetime(hist["alarm_end"])
            present_regions = {r["region"] for r in rows} if rows else set()
            for region in PERMANENT_REGIONS - present_regions:
                passthrough = (
                    hist[(hist["region"] == region) & hist["alarm_end"].isna()]
                    .sort_values("alarm_start")
                    .drop_duplicates(subset=["region", "alarm_type"], keep="last")
                )
                for _, row in passthrough.iterrows():
                    rows.append({
                        "alarm_start": row["alarm_start"],
                        "alarm_end": row["alarm_end"],
                        "region": row["region"],
                        "region_en": row["region_en"],
                        "alarm_type": row["alarm_type"],
                        "duration_min": row["duration_min"],
                    })
                    log(f"  [passthrough] {region} - carried forward open alarm from {row['alarm_start']}")
        except Exception as e:
            log(f"  [!] Failed to load passthrough from combined dataset: {e}")

    if not rows:
        log("[!] No alerts found for yesterday.")
        return

    df = pd.DataFrame(rows)[KEEP]

    df["alarm_start"] = pd.to_datetime(df["alarm_start"])
    df["alarm_end"] = pd.to_datetime(df["alarm_end"])
    df["_start_5min"] = df["alarm_start"].dt.floor("5min")

    df = (
        df.groupby(["region", "alarm_type", "_start_5min"], as_index=False)
        .agg(
            alarm_start  = ("alarm_start", "min"),
            alarm_end = ("alarm_end", "max"),
            region_en = ("region_en", "first"),
            duration_min = ("duration_min","max"),
        )
        .drop(columns=["_start_5min"])
    )

    region_en_map = df.set_index("region")["region_en"].to_dict()
    df = merge_overlapping(df).reset_index(drop=True)
    df["region_en"] = df["region"].map(region_en_map)
    df["duration_min"] = (df["alarm_end"] - df["alarm_start"]).dt.total_seconds() / 60

    df = df[KEEP].sort_values("alarm_start").reset_index(drop=True)
    log(f"  Deduplicated to {len(df)} unique alarm events")
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df.to_csv(OUTPUT_FILE, mode="w", index=False, header=True, encoding="utf-8-sig")
    log(f"[+] Wrote {len(df)} rows -> {OUTPUT_FILE}")
    log("Done.\n")

if __name__ == "__main__":
    main()