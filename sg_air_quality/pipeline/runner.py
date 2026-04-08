from sg_air_quality.common.logger import get_logger
from sg_air_quality.load.csv_loader import save_dataframe_to_csv
from sg_air_quality.load.bigquery_loader import save_dataframe_to_bigquery
import time

logger = get_logger(__name__)

def run_single_etl(label, fetch_function, extract_functions, transform_functions, csv_path, bq_table):
    logger.info("%s ETL started", label)
    start = time.perf_counter()

    raw_json = fetch_function()
    metadata_df, reading_df = extract_functions(raw_json)

    df = transform_functions(metadata_df, reading_df)

    save_dataframe_to_csv(df, csv_path)
    save_dataframe_to_bigquery(df, bq_table)

    logger.info("%s ETL finished in %.2fs", label, time.perf_counter() - start)
    return df