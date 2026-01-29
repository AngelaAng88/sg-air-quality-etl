from extract.pm25 import fetch_pm25_json, extract_pm25_region_metadata, extract_pm25_readings
from extract.psi import fetch_psi_json,extract_psi_region_metadata,extract_psi_readings

from transform.pm25 import flatten_pm25
from transform.psi import flatten_psi
from transform.air_quality import merge_pm25_psi

from load.paths import pm25_csv_path, psi_csv_path, air_quality_csv_path
from load.csv_loader import save_dataframe_to_csv

from datetime import datetime
import argparse
from pathlib import Path

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Fetch air quality data for a given date (YYYY-MM-DD)")
    parser.add_argument("--date", type=str, required=False, help="Date to fetch air quality data for (format YYYY-MM-DD)")
    args = parser.parse_args()

    input_date = args.date
    toArchive = True
    current_date = datetime.now().strftime("%Y-%m-%d")

    if not input_date or input_date == current_date:
        input_date = current_date
        toArchive = False

    # PM2.5 Data Extraction
    # Extract PM2.5 data
    pm25_json = fetch_pm25_json(input_date)
    pm25_metadata_df = extract_pm25_region_metadata(pm25_json)
    pm25_reading_df = extract_pm25_readings(pm25_json)
    # Transform PM2.5 data
    flatten_pm25_df = flatten_pm25(pm25_reading_df, pm25_metadata_df)
    # Load PM2.5 data
    pm25_csv_path = pm25_csv_path(input_date, toArchive)
    save_dataframe_to_csv(flatten_pm25_df, pm25_csv_path)

    # PSI Data Extraction
    # Extract PSI data
    psi_json = fetch_psi_json(input_date)
    psi_metadata_df = extract_psi_region_metadata(psi_json)
    psi_reading_df = extract_psi_readings(psi_json)
    # Transform PSI data
    flatten_psi_df = flatten_psi(psi_reading_df, psi_metadata_df)
    # Load PSI data
    psi_csv_path = psi_csv_path(input_date, toArchive)
    save_dataframe_to_csv(flatten_psi_df, psi_csv_path)

    # Merge PM2.5 and PSI data
    # Transform merged PM2.5 and PSI data
    air_quality_df = merge_pm25_psi(flatten_pm25_df, flatten_psi_df)
    # Load merged data
    air_quality_csv_path = air_quality_csv_path(input_date, toArchive)
    save_dataframe_to_csv(air_quality_df, air_quality_csv_path)