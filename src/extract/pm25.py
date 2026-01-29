import pandas as pd
from config.settings import PM25_API_URI
from common.http import fetch_api_data

def fetch_pm25_json(date: str | None = None):
    headers = {}
    params = {}
    if date:
        params["date"] = date

    json_data = fetch_api_data(headers, params, PM25_API_URI)
    return json_data

def extract_pm25_readings(data: dict) -> pd.DataFrame:
    rows = []
    for item in data['data']['items']:
        timestamp = item['timestamp']
        readings = item['readings']['pm25_one_hourly']
        for region, value in readings.items():
            rows.append({
                'timestamp': timestamp,
                'region': region,
                'pm25_value': value
                })
    return pd.DataFrame(rows)

def extract_pm25_region_metadata(data: dict) -> pd.DataFrame:
    rows = []
    for item in data['data']['regionMetadata']:
        region = item['name']
        latitude =item['labelLocation']['latitude']
        longitude = item['labelLocation']['longitude']
        rows.append({
            'region': region,
            'latitude': latitude,
            'longitude': longitude
            })
    return pd.DataFrame(rows)