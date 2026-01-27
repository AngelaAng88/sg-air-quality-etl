import os
from dotenv import load_dotenv

load_dotenv()
# Load the API key from environment variables (.env file)
DATA_GOV_SG_API_KEY = os.getenv("DATA_GOV_SG_API_KEY")
PM25_API_URI = os.getenv("PM25_API_URI")
PSI_API_URI = os.getenv("PSI_API_URI")

if not DATA_GOV_SG_API_KEY:
    raise ValueError("DATA_GOV_SG_API_KEY is not set")

if not PM25_API_URI:
    raise ValueError("PM25_API_URI is not set")

if not PSI_API_URI:
    raise ValueError("PSI_API_URI is not set")