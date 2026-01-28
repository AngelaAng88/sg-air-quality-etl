from extract_pm25 import extract_pm25_region_metadata, extract_pm25_data, flatten_pm25_data, fetch_pm25_data, save_pm25_data_to_csv
from extract_psi import extract_psi_region_metadata, extract_psi_data, flatten_psi_data, fetch_psi_data, save_psi_data_to_csv

import argparse

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Fetch air quality data for a given date (YYYY-MM-DD)")
    parser.add_argument("--date", type=str, required=False, help="Date to fetch air quality data for (format YYYY-MM-DD)")
    args = parser.parse_args()

    input_date = args.date
    write_file = True

    if not input_date:
        from datetime import datetime
        input_date = datetime.now().strftime("%Y-%m-%d")
        write_file = False

    # PM2.5 Data Extraction
    pm25_data = fetch_pm25_data(input_date)
    pm25_metadata_df = extract_pm25_region_metadata(pm25_data)
    pm25_df = extract_pm25_data(pm25_data)
    flatten_pm25_df = flatten_pm25_data(pm25_df, pm25_metadata_df)
    pm25_file_name = f"data/pm25_{input_date}.csv"  # e.g., data/pm25_2026-01-01.csv
    save_pm25_data_to_csv(flatten_pm25_df, pm25_file_name, write_file)

    # PSI Data Extraction
    psi_data = fetch_psi_data(input_date)
    psi_metadata_df = extract_psi_region_metadata(psi_data)
    psi_df = extract_psi_data(psi_data)
    flatten_psi_df = flatten_psi_data(psi_df, psi_metadata_df)
    psi_file_name = f"data/psi_{input_date}.csv"  # e.g., data/psi_2026-01-01.csv
    save_psi_data_to_csv(flatten_psi_df, psi_file_name, write_file)