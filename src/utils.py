import pandas as pd
import requests
from config import DATA_GOV_SG_API_KEY

headers = {}
if DATA_GOV_SG_API_KEY:
    headers["x-api-key"] = DATA_GOV_SG_API_KEY

def fetch_api_data(params: dict, api_uri: str):
    response = requests.get(api_uri, headers=headers, params=params, timeout=10)
    response.raise_for_status()  # fail fast
    return response.json()

def save_data_to_csv(flattened_data: pd.DataFrame, file_name: str, write_file: bool):
    # Save the DataFrame if write_file is True
    if write_file:
        flattened_data.to_csv(file_name, index=False)
    else:
        print(flattened_data)