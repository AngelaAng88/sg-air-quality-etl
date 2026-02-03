import pandas as pd
from pathlib import Path
from datetime import datetime, timezone, timedelta
from sg_air_quality.common.logger import logger
import time

def save_dataframe_to_csv(df: pd.DataFrame, path: Path):
    logger.info(f"Saving DataFrame to CSV at {path}")
    # Ensure the directory exists
    path.parent.mkdir(parents=True, exist_ok=True)

    sgt = timezone(timedelta(hours=8))
    df['ingested_at'] = datetime.now(sgt).replace(microsecond=0)
    df['data_source'] = 'data.gov.sg'

    start_time = time.perf_counter()
    # Save the DataFrame
    df.to_csv(path, index=False)
    end_time = time.perf_counter()
    logger.info("Saved %d rows to CSV at %s (duration %.2f seconds)",
    len(df),path, end_time - start_time)