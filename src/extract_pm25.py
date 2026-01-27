import requests
from config import DATA_GOV_SG_API_KEY, PM25_API_URI

headers = {}

if DATA_GOV_SG_API_KEY:
    headers["x-api-key"] = DATA_GOV_SG_API_KEY

def fetch_pm25_data(date: str | None = None):
    params = {}
    if date:
        params["date"] = date

    response = requests.get(PM25_API_URI, headers=headers, params=params, timeout=10)
    response.raise_for_status()  # fail fast

    return response.json()

if __name__ == "__main__":
    data = fetch_pm25_data()
    print(data)
