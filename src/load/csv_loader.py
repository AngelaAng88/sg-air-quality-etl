import pandas as pd
from pathlib import Path

def save_dataframe_to_csv(df: pd.DataFrame, path: Path):
    # Ensure the directory exists
    path.parent.mkdir(parents=True, exist_ok=True)

    # Save the DataFrame
    df.to_csv(path, index=False)