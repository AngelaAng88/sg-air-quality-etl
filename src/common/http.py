import requests
from config.settings import DATA_GOV_SG_API_KEY, HTTP_TIMEOUT

def fetch_api_data(headers: dict, params: dict, api_uri: str):
    if DATA_GOV_SG_API_KEY:
        headers["x-api-key"] = DATA_GOV_SG_API_KEY

    try:
        response = requests.get(api_uri, headers=headers, params=params, timeout=HTTP_TIMEOUT)
        response.raise_for_status()  
        return response.json()
    #todo: improve error handling and handle specific server code messages
    except requests.exceptions.HTTPError as e:
        raise RuntimeError(f"HTTP error {response.status_code}: {response.text}") from e
    except requests.exceptions.RequestException as e:
        raise RuntimeError("Network / connection error") from e