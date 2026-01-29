import pandas as pd
from config.settings import PSI_API_URI
from common.http import fetch_api_data

def fetch_psi_json(date: str | None = None):
    headers = {}
    params = {}
    if date:
        params["date"] = date

    json_data = fetch_api_data(headers, params, PSI_API_URI)
    return json_data

def extract_psi_readings(data: dict) -> pd.DataFrame:
    rows = []
    for item in data['data']['items']:
        timestamp = item['timestamp']
        readings = item['readings']
        for psi_readings, value_sets in readings.items():
            for region, value in value_sets.items():
                rows.append({
                    'timestamp': timestamp,
                    'psi_readings': psi_readings,
                    'region': region,
                    'psi_value': value
                })
    return pd.DataFrame(rows)

def extract_psi_region_metadata(data: dict) -> pd.DataFrame:
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
