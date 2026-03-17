import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from util.regions import REGIONS

session = requests.Session()
retries = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retries))

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
EN_TO_COORDS = {en.replace(" ", "_"): (lat, lon) for ua, (lat, lon, en) in REGIONS.items()}

def add_weather_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["region_id", "timestamp_hour"]).reset_index(drop=True)

    for h in [3, 6, 12, 24]:
        df[f"temp_forecast_t{h}"] = df.groupby("region_id")["temp_mean"].shift(-h)
        df[f"wind_forecast_t{h}"] = df.groupby("region_id")["wind_mean"].shift(-h)
        if h == 6:
             df[f"precip_forecast_t6"] = df.groupby("region_id")["precip_sum"].shift(-h)

    for col in ["temp_mean", "wind_mean", "precip_sum", "pressure_mean", "cloudcover_mean"]:
        df[f"{col.split('_')[0]}_lag1"] = df.groupby("region_id")[col].shift(1)

    unique_rids = df["region_id"].unique()
    all_live_forecasts = []

    for rid in unique_rids:
        lookup_rid = rid.replace(" ", "_")
        if lookup_rid not in EN_TO_COORDS: continue
            
        lat, lon = EN_TO_COORDS[lookup_rid]
        params = {
            "latitude": lat, "longitude": lon,
            "hourly": "temperature_2m,windspeed_10m,precipitation,pressure_msl,cloudcover",
            "timezone": "UTC", "forecast_days": 3 
        }
        
        try:
            r = session.get(OPEN_METEO_URL, params=params, timeout=25)
            r.raise_for_status()
            res = r.json()
            
            if "hourly" not in res: continue
                
            f_df = pd.DataFrame(res["hourly"])
            f_df["timestamp_hour"] = pd.to_datetime(f_df["time"])
            f_df["region_id"] = rid
            
            for h in [3, 6, 12, 24]:
                f_df[f"api_temp_t{h}"] = f_df["temperature_2m"].shift(-h)
                f_df[f"api_wind_t{h}"] = f_df["windspeed_10m"].shift(-h)
            
            cols = ["timestamp_hour", "region_id"] + [c for c in f_df.columns if "api_" in c]
            all_live_forecasts.append(f_df[cols])
        except Exception as e:
            print(f"[weather] Warning: Failed to fetch live forecast for {rid}: {e}")

    if all_live_forecasts:
        master_f = pd.concat(all_live_forecasts)
        df = df.merge(master_f, on=["timestamp_hour", "region_id"], how="left")
        
        for h in [3, 6, 12, 24]:
            df[f"temp_forecast_t{h}"] = df[f"temp_forecast_t{h}"].fillna(df[f"api_temp_t{h}"])
            df[f"wind_forecast_t{h}"] = df[f"wind_forecast_t{h}"].fillna(df[f"api_wind_t{h}"])
            
        api_cols = [c for c in df.columns if "api_" in c]
        df = df.drop(columns=api_cols)

    return df