import pandas as pd

def flatten_pm25(pm25_readings: dict, metadata: dict) -> pd.DataFrame:
    merged_df = pd.merge(pm25_readings, metadata, on="region", how="left")
    return merged_df

def transform_pm25(df: dict) -> dict:
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["date"] = df["timestamp"].dt.date
    df['pm25_value'] = df['pm25_value'].astype(float)
    df['latitude'] = df['latitude'].astype(float)
    df['longitude'] = df['longitude'].astype(float)
    df = reorder_pm25_columns(df)
    # later: type casting, renaming, validation
    return df

def reorder_pm25_columns(df: dict) -> dict:
    ordered_cols = [
        # identifiers
        "date",
        "timestamp",
        "region",
        
        #regional coordinates
        "latitude",
        "longitude",
        
        #PM2.5 readings
        "pm25_value"
    ]

    return df[ordered_cols]

def sort_pm25(df: dict) -> dict:
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
