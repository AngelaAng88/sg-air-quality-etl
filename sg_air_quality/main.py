from typing import Optional
from sg_air_quality.extract.pm25 import fetch_pm25_json, extract_pm25_region_metadata, extract_pm25_readings
from sg_air_quality.extract.psi import fetch_psi_json,extract_psi_region_metadata,extract_psi_readings

from sg_air_quality.pipeline.runner import run_single_etl
from sg_air_quality.transform.pm25 import flatten_pm25, transform_pm25, sort_pm25
from sg_air_quality.transform.psi import flatten_psi, transform_psi, sort_psi
from sg_air_quality.transform.air_quality import merge_pm25_psi, sort_air_quality, transform_air_quality

from sg_air_quality.load.paths import retrieve_pm25_csv_path, retrieve_psi_csv_path, retrieve_air_quality_csv_path
from sg_air_quality.load.csv_loader import save_dataframe_to_csv
from sg_air_quality.load.bigquery_loader import save_dataframe_to_bigquery

from sg_air_quality.common.logger import setup_logging, get_logger
from sg_air_quality.common.runtime import get_today, get_yesterday, to_archive, set_input_date
import argparse
import time
from sg_air_quality.config.settings import BQ_PM25_TABLE, BQ_PSI_TABLE, BQ_AIR_QUALITY_TABLE


logger = get_logger(__name__)

def run_etl_for_date(input_date: Optional[str] = None):
    setup_logging()
    yesterday = get_yesterday()
    today = get_today()
    input_date = set_input_date(input_date)
    toArchive = to_archive(input_date)
    pipeline_start_time = time.perf_counter()
        
    #PM2.5 ETL using run_single_etl function
    flatten_pm25_df = run_single_etl(
        #For logging purposes, we can label this ETL run as "PM2.5"
        label = "PM2.5",
        #fetch_function is a lambda function that calls fetch_pm25_json with the input_date
        fetch_function = lambda: fetch_pm25_json(input_date),
        #extract_functions is a lambda function that takes the raw JSON and returns a tuple of (metadata_df, reading_df)
        #by calling the respective extract functions
        extract_functions = lambda j: (extract_pm25_region_metadata(j), extract_pm25_readings(j)),
        #transform_functions is a lambda function that takes the metadata_df and reading_df and applies the flatten,
        #transform, and sort functions in sequence
        transform_functions = lambda meta, readings: sort_pm25(transform_pm25(flatten_pm25(readings, meta))),
        #csv_path (string) is obtained by calling retrieve_pm25_csv_path with the input_date and toArchive flag
        csv_path = retrieve_pm25_csv_path(input_date, toArchive),
        #bq_table (string) is the constant BQ_PM25_TABLE imported from settings
        bq_table = BQ_PM25_TABLE
    )

    # PSI ETL using run_single_etl function
    flatten_psi_df = run_single_etl(
        #For logging purposes, we can label this ETL run as "PSI"
        label = "PSI",
        #fetch_function is a lambda function that calls fetch_psi_json with the input_date
        fetch_function = lambda: fetch_psi_json(input_date),
        #extract_functions is a lambda function that takes the raw JSON and returns a tuple of (metadata_df, reading_df)
        extract_functions = lambda j: (extract_psi_region_metadata(j), extract_psi_readings(j)),
        #transform_functions is a lambda function that takes the metadata_df and reading_df and applies the flatten,
        #transform, and sort functions in sequence
        transform_functions = lambda meta, readings: sort_psi(transform_psi(flatten_psi(readings, meta))),
        #csv_path (string) is obtained by calling retrieve_psi_csv_path with the input_date and toArchive flag
        csv_path = retrieve_psi_csv_path(input_date, toArchive),
        #bq_table (string) is the constant BQ_PSI_TABLE imported from settings
        bq_table = BQ_PSI_TABLE
    )

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
    try:
        run_etl_for_date(input_date)
    except Exception as e:
        logger.exception("ETL pipeline failed: %s", e)
        raise SystemExit(1)