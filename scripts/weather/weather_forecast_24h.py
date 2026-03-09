import requests
import json
import os
from datetime import datetime
import time

WEATHER_KEY = os.getenv("WEATHER_KEY")

REGION_CAPITALS = {
    "vinnytsia_oblast": "Vinnytsia",
    "volyn_oblast": "Lutsk",
    "dnipropetrovsk_oblast": "Dnipro",
    "donetsk_oblast": "Donetsk",
    "zhytomyr_oblast": "Zhytomyr",
    "zakarpattia_oblast": "Uzhhorod",
    "zaporizhzhia_oblast": "Zaporizhzhia",
    "ivano_frankivsk_oblast": "Ivano-Frankivsk",
    "kyiv_oblast": "Kyiv",
    "kirovohrad_oblast": "Kropyvnytskyi",
    "luhansk_oblast": "Luhansk",
    "lviv_oblast": "Lviv",
    "mykolaiv_oblast": "Mykolaiv",
    "odesa_oblast": "Odesa",
    "poltava_oblast": "Poltava",
    "rivne_oblast": "Rivne",
    "sumy_oblast": "Sumy",
    "ternopil_oblast": "Ternopil",
    "kharkiv_oblast": "Kharkiv",
    "kherson_oblast": "Kherson",
    "khmelnytskyi_oblast": "Khmelnytskyi",
    "cherkasy_oblast": "Cherkasy",
    "chernivtsi_oblast": "Chernivtsi",
    "chernihiv_oblast": "Chernihiv",
    "crimea_republic": "Simferopol",
    "sevastopol_city": "Sevastopol"
}

BASE_WEATHER_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"

def validate_config():
    if not WEATHER_KEY:
        raise Exception("WEATHER_KEY environment variable is not set")

def safe_get_hours(raw_data):
    try:
        days = raw_data.get("days", [])
        if not days:
            return []
        return days[0].get("hours", [])[:24]
    except Exception:
        return []

def get_weather_for_city(region_id, city):
    url = f"{BASE_WEATHER_URL}/{city}/today?unitGroup=metric&key={WEATHER_KEY}&contentType=json"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        raw_data = response.json()
        hours = safe_get_hours(raw_data)

        processed_hours = []
        for hour in hours:
            processed_hours.append({
                "region_id": region_id,
                "datetime": datetime.utcfromtimestamp(hour["datetimeEpoch"]).isoformat(),
                "temp": hour.get("temp"),
                "wind": hour.get("windspeed"),
                "precip": hour.get("precip"),
                "pressure": hour.get("pressure")
            })
        return processed_hours
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] {city}: {e}")
        return []

def get_weather():
    validate_config()
    all_records = []
    for region, city in REGION_CAPITALS.items():
        print(f"[COLLECT] {region} -> {city}")
        records = get_weather_for_city(region, city)
        all_records.extend(records)
        time.sleep(1)
    return {
        "success": True,
        "generated_at": datetime.utcnow().isoformat(),
        "data": all_records
    }

if __name__ == "__main__":
    result = get_weather()
    print(json.dumps(result, indent=2, ensure_ascii=False))
