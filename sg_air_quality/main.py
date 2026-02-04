from sg_air_quality.extract.pm25 import fetch_pm25_json, extract_pm25_region_metadata, extract_pm25_readings
from sg_air_quality.extract.psi import fetch_psi_json,extract_psi_region_metadata,extract_psi_readings

from sg_air_quality.transform.pm25 import flatten_pm25, transform_pm25, sort_pm25
from sg_air_quality.transform.psi import flatten_psi, transform_psi, sort_psi
from sg_air_quality.transform.air_quality import merge_pm25_psi, sort_air_quality, transform_air_quality

from sg_air_quality.load.paths import retrieve_pm25_csv_path, retrieve_psi_csv_path, retrieve_air_quality_csv_path
from sg_air_quality.load.csv_loader import save_dataframe_to_csv
from sg_air_quality.load.bigquery_loader import save_dataframe_to_bigquery

from datetime import datetime, time, timedelta
from sg_air_quality.common.logger import setup_logging, get_logger
import argparse
import time
from sg_air_quality.config.settings import BQ_PM25_TABLE, BQ_PSI_TABLE, BQ_AIR_QUALITY_TABLE

setup_logging()
logger = get_logger(__name__)

def run_etl_for_date(input_date: str | None = None):
    toArchive = True
    #To-do: refactor date logic into a utility function
    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
 
    if not input_date:
        input_date = yesterday
    elif input_date == today:
        toArchive = False

    pipeline_start_time = time.perf_counter()
        
    # PM2.5 Data Extraction
    # Extract PM2.5 data
    logger.info("PM2.5 Data ETL started")
    pm25_start_time = time.perf_counter()
    pm25_json = fetch_pm25_json(input_date)
    pm25_parsed_start_time = time.perf_counter()
    pm25_metadata_df = extract_pm25_region_metadata(pm25_json)
    pm25_reading_df = extract_pm25_readings(pm25_json)
    pm25_parsed_end_time = time.perf_counter()
    logger.info("Parsed PM2.5 JSON into raw records (rows=%s, duration=%.2fs)", len(pm25_reading_df), pm25_parsed_end_time - pm25_parsed_start_time)
    # Transform PM2.5 data
    pm25_transform_start_time = time.perf_counter()
    flatten_pm25_df = flatten_pm25(pm25_reading_df, pm25_metadata_df)
    logger.info("Transforming PM2.5 DataFrame (type casting, date enrichment, sorting)")
    flatten_pm25_df = transform_pm25(flatten_pm25_df)
    flatten_pm25_df = sort_pm25(flatten_pm25_df)
    pm25_transform_end_time = time.perf_counter()
    logger.info("Transforming PM2.5 DataFrame complete (%s records, duration=%.2fs)", len(flatten_pm25_df), pm25_transform_end_time - pm25_transform_start_time)
    # Load PM2.5 data
    pm25_load_start_time = time.perf_counter()
    pm25_csv_path = retrieve_pm25_csv_path(input_date, toArchive)
    save_dataframe_to_csv(flatten_pm25_df, pm25_csv_path)
    save_dataframe_to_bigquery(flatten_pm25_df,BQ_PM25_TABLE)
    pm25_load_end_time = time.perf_counter()
    logger.info("Loaded PM2.5 DataFrame to CSV and BigQuery (%s records, duration=%.2fs)", len(flatten_pm25_df), pm25_load_end_time - pm25_load_start_time)
    pm25_end_time = time.perf_counter()
    logger.info("PM2.5 Data ETL finished successfully in %.2fs", pm25_end_time - pm25_start_time)

    # PSI Data Extraction
    # Extract PSI data
    logger.info("PSI Data ETL started")
    psi_start_time = time.perf_counter()
    psi_json = fetch_psi_json(input_date)
    psi_parsed_start_time = time.perf_counter()
    psi_metadata_df = extract_psi_region_metadata(psi_json)
    psi_reading_df = extract_psi_readings(psi_json)
    psi_parsed_end_time = time.perf_counter()
    logger.info("Parsed PSI JSON into raw records (rows=%s, duration=%.2fs)", len(psi_reading_df), psi_parsed_end_time - psi_parsed_start_time)
    # Transform PSI data
    psi_transform_start_time = time.perf_counter()
    flatten_psi_df = flatten_psi(psi_reading_df, psi_metadata_df)
    logger.info("Transforming PSI DataFrame (type casting, date enrichment, sorting)")
    flatten_psi_df = transform_psi(flatten_psi_df)
    flatten_psi_df = sort_psi(flatten_psi_df)
    psi_transform_end_time = time.perf_counter()
    logger.info("PSI DataFrame transformation complete (%s records, duration=%.2fs)", len(flatten_psi_df), psi_transform_end_time - psi_transform_start_time)
    # Load PSI data
    psi_load_start_time = time.perf_counter()
    psi_csv_path = retrieve_psi_csv_path(input_date, toArchive)
    save_dataframe_to_csv(flatten_psi_df, psi_csv_path)
    save_dataframe_to_bigquery(flatten_psi_df,BQ_PSI_TABLE)
    psi_load_end_time = time.perf_counter()
    logger.info("Loaded PSI DataFrame to CSV and BigQuery (%s records, duration=%.2fs)", len(flatten_psi_df), psi_load_end_time - psi_load_start_time)
#
    # Merge PM2.5 and PSI data
    # Transform merged PM2.5 and PSI data
    logger.info("Air Quality Data ETL started")
    logger.info("Transforming PSI & PM 2.5 DataFrame (merging, type casting, date enrichment, sorting) to get Air Quality DataFrame")
    air_quality_transfor_start_time = time.perf_counter()
    air_quality_df = merge_pm25_psi(flatten_pm25_df, flatten_psi_df)
    air_quality_df = transform_air_quality(air_quality_df)
    air_quality_df = sort_air_quality(air_quality_df)
    air_quality_transform_end_time = time.perf_counter()
    logger.info("Air Quality DataFrame transformation complete (%s records, duration=%.2fs)", len(air_quality_df), air_quality_transform_end_time - air_quality_transfor_start_time)
    # Load merged data
    air_quality_load_start_time = time.perf_counter()
    air_quality_csv_path = retrieve_air_quality_csv_path(input_date, toArchive)
    save_dataframe_to_csv(air_quality_df, air_quality_csv_path)
    save_dataframe_to_bigquery(air_quality_df,BQ_AIR_QUALITY_TABLE)
    air_quality_load_end_time = time.perf_counter()
    logger.info("Loaded Air Quality DataFrame to CSV and BigQuery (%s records, duration=%.2fs)", len(air_quality_df), air_quality_load_end_time - air_quality_load_start_time)
    pipeline_end_time = time.perf_counter()
    logger.info("Air Quality ETL run completed in %.2f seconds", pipeline_end_time - pipeline_start_time)

if __name__ == "__main__":
    #to-do: logging for start and end of ETL process
    parser = argparse.ArgumentParser(description="Fetch air quality data for a given date (YYYY-MM-DD)")
    parser.add_argument("--date", type=str, required=False, help="Date to fetch air quality data for (format YYYY-MM-DD)")
    args = parser.parse_args()
    input_date = args.date
    run_etl_for_date(input_date)