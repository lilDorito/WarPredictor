import requests
import json

MY_API_KEY = "API-from-postman-website"
ENV_ID = "postman-environment-id"
WEATHER_KEY = "key-from-wether-website"

def to_postman(weather_data):
    url = f"https://api.getpostman.com/environments/{ENV_ID}"
    headers = {
        "X-Api-Key": MY_API_KEY,
        "Content-Type": "application/json"
    }
    pretty_weather = json.dumps(weather_data, indent = 4)
    payload = {
        "environment": {
            "values": [
                {
                    "key": "latest_weather",
                    "value": pretty_weather,
                    "enabled": True
                }
            ]
        }
    }

    res = requests.put(url, headers = headers, json = payload)
    if res.status_code == 200:
        print("Seccess: Postman updated with pretty JSON ^^,")
    else:
        print(f"Postman Error: {res.text}")

def get_weather():
    w_url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/Kyiv?unitGroup=metric&key={WEATHER_KEY}&contentType=json"   
    response = requests.get(w_url)
    if response.status_code == 200:
        forecast = response.json()['days'][0]['hours'][':24']
        to_postman(forecast)
    else:
        print(f"Weather API Error: {response.status_code}")                         
if __name__ == "__main__":
    get_weather()