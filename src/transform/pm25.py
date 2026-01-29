import pandas as pd

def flatten_pm25(pm25_readings: dict, metadata: dict) -> pd.DataFrame:
    merged_df = pd.merge(pm25_readings, metadata, on="region", how="left")
    return merged_df