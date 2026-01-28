import requests, json
import pandas as pd
import argparse
from config import DATA_GOV_SG_API_KEY, PSI_API_URI

headers = {}

if DATA_GOV_SG_API_KEY:
    headers["x-api-key"] = DATA_GOV_SG_API_KEY

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

def fetch_psi_data(date: str | None = None):
    params = {}
    if date:
        params["date"] = date

    response = requests.get(PSI_API_URI, headers=headers, params=params, timeout=10)
    response.raise_for_status()  # fail fast

    return response.json()

def extract_psi_data(data: dict) -> pd.DataFrame:
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

def flatten_psi_data(psi_data: dict, metadata: dict) -> pd.DataFrame:
    merged_df = pd.merge(psi_data, metadata, on="region", how="left")
    return merged_df

def save_psi_data_to_csv(flattened_data: pd.DataFrame, file_name: str, write_file: bool):
    # Save the DataFrame if write_file is True
    if write_file:
        flattened_data.to_csv(file_name, index=False)
    else:
        print(flattened_data)