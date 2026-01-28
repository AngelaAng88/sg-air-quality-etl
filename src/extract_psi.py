import pandas as pd
from config import PSI_API_URI
from utils import fetch_api_data

def fetch_psi_data(date: str | None = None):
    params = {}
    if date:
        params["date"] = date

    json_data = fetch_api_data(params, PSI_API_URI)
    return json_data

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
    # Pivot psi_readings into separate columns
    pivoted_df = psi_data.pivot_table(
        index=['timestamp', 'region'],
        columns='psi_readings',
        values='psi_value',
        aggfunc='first'
    ).reset_index()
    
    # Merge with metadata
    merged_df = pd.merge(pivoted_df, metadata, on="region", how="left")
    return merged_df

