import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta, timezone
from sg_air_quality.common.logger import logger
import time

def save_dataframe_to_bigquery(df: pd.DataFrame, table_id: str):
    logger.info(f"Loading DataFrame to BigQuery table {table_id}")
    client = bigquery.Client()
    sgt = timezone(timedelta(hours=8))
    df['ingested_at'] = datetime.now(sgt).replace(microsecond=0)
    df['data_source'] = 'data.gov.sg'
    start_time = time.perf_counter()
    job = client.load_table_from_dataframe(df, table_id, job_config=bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    ))
    # Wait for the job to complete 
    job.result()
    duration_sec = time.perf_counter() - start_time
    
    logger.info("Loaded %d rows into %s (total duration %.2f seconds, BigQuery execution %.2f seconds)",
    job.output_rows,table_id,duration_sec,(job.ended - job.started).total_seconds())