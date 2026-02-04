import pandas as pd
from sg_air_quality.config.settings import PSI_API_URI
from sg_air_quality.common.http import fetch_api_data
from sg_air_quality.common.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

def fetch_psi_json(date: str | None = None):
    headers = {}
    params = {}
    if date:
        logger.info(f"PSI processing date: {date}")
        params["date"] = date
    else:
        logger.info(f"No date provided, using default behavior.")

    response = fetch_api_data(headers, params, PSI_API_URI)
    logger.info(f"Received {len(response.json()['data']['items'])} time records")
    logger.info("PSI data fetched successfully")
    return response.json()

def extract_psi_readings(data: dict) -> pd.DataFrame:
    logger.info("Extracting PSI readings into DataFrame")
    rows = []
    for item in data['data']['items']:
        timestamp = item['timestamp']
        readings = item['readings']
        for psi_readings, value_sets in readings.items():
            for region, value in value_sets.items():
                rows.append({
                    'timestamp': timestamp,
                    'psi_readings': psi_readings,
                    'region': region,
                    'psi_value': value
                })
    df = pd.DataFrame(rows)
    logger.info(f"Extracted {len(df)} raw PSI measurement rows (multiple PSI metrics per timestamp; to be pivoted into columns)")
    return df

def extract_psi_region_metadata(data: dict) -> pd.DataFrame:
    logger.info("Extracting PSI region metadata into DataFrame")
    rows = []
    for item in data['data']['regionMetadata']:
        region = item['name']
        latitude =item['labelLocation']['latitude']
        longitude = item['labelLocation']['longitude']
        rows.append({
            'region': region,
            'latitude': latitude,
            'longitude': longitude
            })
    df = pd.DataFrame(rows)
    logger.info(f"Extracted {len(df)} region metadata records for PSI data")
    return df
