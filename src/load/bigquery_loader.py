import pandas as pd
from google.cloud import bigquery
from config.settings import GOOGLE_APPLICATION_CREDENTIALS
from datetime import datetime, timedelta, timezone

def save_dataframe_to_bigquery(df: pd.DataFrame, table_id: str):
    client = bigquery.Client()
    sgt = timezone(timedelta(hours=8))
    df['ingested_at'] = datetime.now(sgt).replace(microsecond=0)
    df['data_source'] = 'data.gov.sg'
    job = client.load_table_from_dataframe(df, table_id, job_config=bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    ))
    
    # Wait for the job to complete 
    job.result()
    print(f"Loaded {job.output_rows} rows into {table_id}")
