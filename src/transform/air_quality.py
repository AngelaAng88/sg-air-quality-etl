
import pandas as pd
from common.logger import logger

def merge_pm25_psi(pm25_df: dict, psi_df: dict) -> dict:
    logger.info("Merging PM2.5 and PSI DataFrames on timestamp and region into Air Quality DataFrame")
    pm25_df = pm25_df.set_index(["timestamp", "region"])
    psi_df = psi_df.set_index(["timestamp", "region"])
    merged_df = pm25_df.join(psi_df.drop(columns=['date', 'latitude', 'longitude','ingested_at','data_source']), how='left').reset_index()
    logger.info(f"Air Quality DataFrame ({len(merged_df)} records)")
    return merged_df

def transform_air_quality(df: dict) -> dict:
    df = reorder_air_quality_columns(df)
    # later: type casting, renaming, validation
    return df

def reorder_air_quality_columns(df: dict) -> dict:
    ordered_cols = [
        # identifiers
        "date",
        "timestamp",
        "region",
        
        #regional coordinates
        "latitude",
        "longitude",
        
        #PM2.5 readings
        "pm25_value",

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

def sort_air_quality(df: dict) -> dict:
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

