import pandas as pd

def merge_pm25_psi(pm25_df: dict, psi_df: dict) -> dict:
    pm25_df = pm25_df.set_index(["timestamp", "region"])
    psi_df = psi_df.set_index(["timestamp", "region"])
    merged_df = pm25_df.join(psi_df.drop(columns=['latitude', 'longitude']), how='left')
    return merged_df