import pandas as pd

def flatten_psi(psi_readings: dict, metadata: dict) -> pd.DataFrame:
    # Pivot psi_readings into separate columns
    pivoted_df = psi_readings.pivot_table(
        index=['timestamp', 'region'],
        columns='psi_readings',
        values='psi_value',
        aggfunc='first'
    ).reset_index()
    
    # Merge with metadata
    merged_df = pd.merge(pivoted_df, metadata, on="region", how="left")
    return merged_df
