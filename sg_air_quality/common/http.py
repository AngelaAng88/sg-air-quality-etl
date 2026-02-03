import requests
from sg_air_quality.config.settings import DATA_GOV_SG_API_KEY, HTTP_TIMEOUT
from sg_air_quality.common.logger import logger
import time

def fetch_api_data(headers: dict, params: dict, api_uri: str):
    logger.info(f"Fetching data from API: {api_uri} with params: {params}")
    if DATA_GOV_SG_API_KEY:
        headers["x-api-key"] = DATA_GOV_SG_API_KEY

    try:
        start_time = time.perf_counter()
        response = requests.get(api_uri, headers=headers, params=params, timeout=HTTP_TIMEOUT)
        response.raise_for_status()
        end_time = time.perf_counter()
        logger.info("Fetched API data (API=%s, duration=%.2fs)", api_uri, end_time - start_time)
        return response
    
    except requests.exceptions.HTTPError as e:
        logger.error("HTTP error calling API | url=%s | status=%s | response=%s",
                     api_uri, response.status_code, response.text, exc_info=True)         
        raise RuntimeError(f"HTTP error {response.status_code}: {response.text}") from e
    except requests.exceptions.RequestException as e:
        logger.error("Network / connection error occurred: %s", e)
        raise RuntimeError("Network / connection error") from e