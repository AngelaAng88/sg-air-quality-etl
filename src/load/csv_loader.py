import pandas as pd
from pathlib import Path
from datetime import datetime, timezone, timedelta


def save_dataframe_to_csv(df: pd.DataFrame, path: Path):
    # Ensure the directory exists
    path.parent.mkdir(parents=True, exist_ok=True)

    sgt = timezone(timedelta(hours=8))
    df['ingested_at'] = datetime.now(sgt).replace(microsecond=0)
    df['data_source'] = 'data.gov.sg'

    # Save the DataFrame
    df.to_csv(path, index=False)