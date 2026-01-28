from extract_pm25 import extract_pm25_region_metadata, extract_pm25_data, flatten_pm25_data, fetch_pm25_data
from extract_psi import extract_psi_region_metadata, extract_psi_data, flatten_psi_data, fetch_psi_data
from utils import save_data_to_csv
from datetime import datetime
import argparse

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Fetch air quality data for a given date (YYYY-MM-DD)")
    parser.add_argument("--date", type=str, required=False, help="Date to fetch air quality data for (format YYYY-MM-DD)")
    args = parser.parse_args()

    input_date = args.date
    write_file = True
    current_date = datetime.now().strftime("%Y-%m-%d")

    if not input_date or input_date == current_date:
        input_date = current_date
        write_file = False

    # PM2.5 Data Extraction
    pm25_data = fetch_pm25_data(input_date)
    pm25_metadata_df = extract_pm25_region_metadata(pm25_data)
    pm25_df = extract_pm25_data(pm25_data)
    flatten_pm25_df = flatten_pm25_data(pm25_df, pm25_metadata_df)
    pm25_file_name = f"data/pm25_{input_date}.csv"  # e.g., data/pm25_2026-01-01.csv
    save_data_to_csv(flatten_pm25_df, pm25_file_name, write_file)

    # PSI Data Extraction
    psi_data = fetch_psi_data(input_date)
    psi_metadata_df = extract_psi_region_metadata(psi_data)
    psi_df = extract_psi_data(psi_data)
    flatten_psi_df = flatten_psi_data(psi_df, psi_metadata_df)
    psi_file_name = f"data/psi_{input_date}.csv"  # e.g., data/psi_2026-01-01.csv
    save_data_to_csv(flatten_psi_df, psi_file_name, write_file)

    # Combine PM2.5 and PSI data
    flatten_pm25_df = flatten_pm25_df.set_index(["timestamp", "region"])
    flatten_psi_df = flatten_psi_df.set_index(["timestamp", "region"])
    combined_df = flatten_pm25_df.join(flatten_psi_df.drop(columns=['latitude', 'longitude']), how='left')
    combined_file_name = f"data/air_quality_{input_date}.csv"  # e.g., data/air_quality_2026-01-01.csv
    save_data_to_csv(combined_df, combined_file_name, write_file)