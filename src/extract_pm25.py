import requests, json
import pandas as pd
import argparse
from config import DATA_GOV_SG_API_KEY, PM25_API_URI

headers = {}

if DATA_GOV_SG_API_KEY:
    headers["x-api-key"] = DATA_GOV_SG_API_KEY

def fetch_pm25_data(date: str | None = None):
    params = {}
    if date:
        params["date"] = date

    response = requests.get(PM25_API_URI, headers=headers, params=params, timeout=10)
    response.raise_for_status()  # fail fast

    return response.json()

def flatten_pm25_data(data: dict) -> pd.DataFrame:
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

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Fetch PM2.5 data for a given date (YYYY-MM-DD)")
    parser.add_argument("--date", type=str, required=True, help="Date to fetch PM2.5 data for (format YYYY-MM-DD)")
    args = parser.parse_args()

    input_date = args.date
    data = fetch_pm25_data(input_date)
    df = flatten_pm25_data(data)
    print(df)

    file_name = f"data/pm25_{input_date}.csv"  # e.g., data/pm25_2026-01-01.csv

    # Save the DataFrame
    df.to_csv(file_name, index=False)