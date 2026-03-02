import json
import pandas as pd
import math

with open("air_alarms_historical.json", "r", encoding="utf-8") as f:
    raw = f.read()
    raw = raw.encode('utf-8').decode('unicode_escape').encode('latin-1').decode('utf-8')
    data = json.loads(raw)

region_map = {
    'Харківщина': 'Харківська обл.',
    'Херсонщина': 'Херсонська обл.',
    'Одещина': 'Одеська обл.',
    'Дніпропетровщина': 'Дніпропетровська обл.',
    'Миколаївщина': 'Миколаївська обл.',
    'Сумщина': 'Сумська обл.',
    'Житомирщина': 'Житомирська обл.',
    'Луганщина': 'Луганська обл.',
    'Чернігівщина': 'Чернігівська обл.',
    'Донеччина': 'Донецька обл.',
    'Запоріжжя': 'Запорізька обл.',
    'Київщина': 'Київська обл.',
    'Полтавщина': 'Полтавська обл.',
    'Черкащина': 'Черкаська обл.',
    'Кіровоградщина': 'Кіровоградська обл.',
    'Вінниччина': 'Вінницька обл.',
    'Хмельниччина': 'Хмельницька обл.',
    'Рівненщина': 'Рівненська обл.',
    'Волинь': 'Волинська обл.',
    'Тернопільщина': 'Тернопільська обл.',
    'Львівщина': 'Львівська обл.',
    'Івано-Франківщина': 'Івано-Франківська обл.',
    'Чернівеччина': 'Чернівецька обл.',
    'Закарпаття': 'Закарпатська обл.',
    ' АР Крим': 'АР Крим',
    'АР Крим': 'АР Крим',
}

def normalize(d):
    return {region_map.get(k, k): v for k, v in d.items()}

def clean_value(v):
    if not isinstance(v, (int, float)):
        try:
            v = float(v)
        except:
            return 0
    v = float(v)
    if v == 0:
        return 0
    magnitude = math.floor(math.log10(abs(v)))
    if magnitude > 6:
        leading = round(v / (10 ** magnitude))
        return leading
    return round(v)

rows = []
for month in data:
    from_date = month.get("period_from")
    to_date = month.get("period_to")

    region_counts = normalize(month.get("by_region_count", {}))
    durations = normalize(month.get("by_duration", {}))
    artillery = normalize(month.get("artillery_by_region", {}))
    explosions = normalize(month.get("explosions_by_region", {}))

    for region in region_counts:
        rows.append({
            "period_from": from_date,
            "period_to": to_date,
            "region": region,
            "alarm_count": clean_value(region_counts.get(region, 0)),
            "duration_hours": clean_value(durations.get(region, 0)),
            "artillery_count": clean_value(artillery.get(region, 0)),
            "explosions_count": clean_value(explosions.get(region, 0)),
        })

df = pd.DataFrame(rows)
df.to_csv("air_alarms_historical.csv", index=False, encoding="utf-8")

for col in ['alarm_count', 'duration_hours', 'artillery_count', 'explosions_count']:
    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

numeric_df = df[['alarm_count', 'duration_hours', 'artillery_count', 'explosions_count']]
print("Non-zero counts per column:")
print((numeric_df > 0).sum())

print("\n> Top regions by alarms:")
print(df.groupby('region')['alarm_count'].sum().sort_values(ascending=False).head(10))

print("\n> Sample mid-2022:")
print(df[df['period_from'] == '2022-06-01'][['region','alarm_count','duration_hours','explosions_count']].head(10))

print(f"\nSaved {len(df)} rows")
