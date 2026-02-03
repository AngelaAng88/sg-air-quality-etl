import pandas as pd
from common.logger import logger

def flatten_psi(psi_readings: dict, metadata: dict) -> pd.DataFrame:
    logger.info("Flattening PSI readings with region metadata")
    # Pivot psi_readings into separate columns
    pivoted_df = psi_readings.pivot_table(
        index=['timestamp', 'region'],
        columns='psi_readings',
        values='psi_value',
        aggfunc='first'
    ).reset_index()
    
    # Merge with metadata
    merged_df = pd.merge(pivoted_df, metadata, on="region", how="left")
    logger.info(f"Flattened PSI readings with region metadata ({len(merged_df)} records)")
    return merged_df

def transform_psi(df: dict) -> dict:
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["date"] = df["timestamp"].dt.date
    df['latitude'] = df['latitude'].astype(float)
    df['longitude'] = df['longitude'].astype(float)
    df['co_eight_hour_max'] = df['co_eight_hour_max'].astype(float)
    df['co_sub_index'] = df['co_sub_index'].astype(float)
    df['no2_one_hour_max'] = df['no2_one_hour_max'].astype(float)
    df['o3_eight_hour_max'] = df['o3_eight_hour_max'].astype(float)
    df['o3_sub_index'] = df['o3_sub_index'].astype(float)
    df['pm10_sub_index'] = df['pm10_sub_index'].astype(float)
    df['pm10_twenty_four_hourly'] = df['pm10_twenty_four_hourly'].astype(float)
    df['pm25_sub_index'] = df['pm25_sub_index'].astype(float)
    df['pm25_twenty_four_hourly'] = df['pm25_twenty_four_hourly'].astype(float)
    df['psi_twenty_four_hourly'] = df['psi_twenty_four_hourly'].astype(float)
    df['so2_sub_index'] = df['so2_sub_index'].astype(float)
    df['so2_twenty_four_hourly'] = df['so2_twenty_four_hourly'].astype(float)
    df = reorder_psi_columns(df)
    return df

def reorder_psi_columns(df: dict) -> dict:
    ordered_cols = [
        # identifiers
        "date",
        "timestamp",
        "region",
        
        #regional coordinates
        "latitude",
        "longitude",

        #PSI readings
        "co_eight_hour_max",
        "co_sub_index",
        "no2_one_hour_max",
        "o3_eight_hour_max",
        "o3_sub_index",
        "pm10_sub_index",
        "pm10_twenty_four_hourly",
        "pm25_sub_index",
        "pm25_twenty_four_hourly",
        "psi_twenty_four_hourly",
        "so2_sub_index",
        "so2_twenty_four_hourly"
    ]

    return df[ordered_cols]

def sort_psi(df: dict) -> dict:
    # Define the desired region order
    region_order = ["north", "south", "east", "west", "central"]

    # Check for unknown regions - raise error if found (to-do: Need to create data validation function in transform pm25 and psi)
    unknown = set(df["region"]) - set(region_order)
    if unknown:
        raise ValueError(f"Unknown regions: {unknown}")

    df = df.copy()
    df["region"] = df["region"].astype(
        pd.CategoricalDtype(categories=region_order, ordered=True)
    )

    return df.sort_values(by=["timestamp", "region"])