import requests, json
import pandas as pd
import argparse
from config import DATA_GOV_SG_API_KEY, PSI_API_URI

headers = {}

if DATA_GOV_SG_API_KEY:
    headers["x-api-key"] = DATA_GOV_SG_API_KEY

def fetch_psi_data(date: str | None = None):
    params = {}
    if date:
        params["date"] = date

    response = requests.get(PSI_API_URI, headers=headers, params=params, timeout=10)
    response.raise_for_status()  # fail fast

    return response.json()

def flatten_psi_data(data: dict) -> pd.DataFrame:
    rows = []

    for item in data['data']['items']:
        timestamp = item['timestamp']
        readings = item['readings']['co_sub_index']
        for region, value in readings.items():
            rows.append({
                'timestamp': timestamp,
                'region': region,
                'psi_value': value
                })
    return pd.DataFrame(rows)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Fetch PSI data for a given date (YYYY-MM-DD)")
    parser.add_argument("--date", type=str, required=False, help="Date to fetch PSI data for (format YYYY-MM-DD)")
    args = parser.parse_args()

    input_date = args.date
    write_file = True

    if not input_date:
        from datetime import datetime
        input_date = datetime.now().strftime("%Y-%m-%d")
        write_file = False

    data = fetch_psi_data(input_date)
    df = flatten_psi_data(data)
    print(df)

    file_name = f"data/psi_{input_date}.csv"  # e.g., data/psi_2026-01-01.csv

    # Save the DataFrame if write_file is True
    if write_file:
        df.to_csv(file_name, index=False)