import pandas as pd
from sg_air_quality.config.settings import PM25_API_URI
from sg_air_quality.common.http import fetch_api_data
from sg_air_quality.common.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

def fetch_pm25_json(date: str):
    headers = {}
    params = {}
    if date:
        logger.info(f"PM2.5 processing date: {date}")
        params["date"] = date
    else:
        logger.info(f"No date provided, using default behavior.")

    response = fetch_api_data(headers, params, PM25_API_URI)
    logger.info(f"Received {len(response.json()['data']['items'])} time records")
    logger.info("PM2.5 data fetched successfully")
    return response.json()

def extract_pm25_readings(data: dict) -> pd.DataFrame:
    logger.info("Extracting PM2.5 readings into DataFrame")
    rows = []
    for item in data['data']['items']:
        timestamp = item['timestamp']
        readings = item['readings']['pm25_one_hourly']
        for region, value in readings.items():
            rows.append({
                'timestamp': timestamp,
                'region': region,
                'pm25_value': value
                })
    df = pd.DataFrame(rows)
    logger.info(f"Extracted {len(df)} PM2.5 reading records")
    return df

def extract_pm25_region_metadata(data: dict) -> pd.DataFrame:
    logger.info("Extracting PM2.5 region metadata into DataFrame")
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
    logger.info(f"Extracted {len(df)} region metadata records for PM2.5 data")
    return df