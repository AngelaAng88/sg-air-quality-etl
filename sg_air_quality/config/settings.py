import os
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

# Load the API key from environment variables (.env file)
DATA_GOV_SG_API_KEY = os.getenv("DATA_GOV_SG_API_KEY")
PM25_API_URI = os.getenv("PM25_API_URI")
PSI_API_URI = os.getenv("PSI_API_URI")
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "10"))

# BigQuery settings
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BQ_PM25_TABLE = os.getenv("BQ_PM25_TABLE")
BQ_PSI_TABLE = os.getenv("BQ_PSI_TABLE")
BQ_AIR_QUALITY_TABLE = os.getenv("BQ_AIR_QUALITY_TABLE")

# Data.gov.sg validations
if not DATA_GOV_SG_API_KEY:
    raise ValueError("DATA_GOV_SG_API_KEY is not set")
if not PM25_API_URI:
    raise ValueError("PM25_API_URI is not set")
if not PSI_API_URI:
    raise ValueError("PSI_API_URI is not set")

#Google Cloud and bigquery validations
if not GOOGLE_APPLICATION_CREDENTIALS:
    raise ValueError("GOOGLE_APPLICATION_CREDENTIALS is not set")
if not BQ_PM25_TABLE:
    raise ValueError("BQ_PM25_TABLE is not set")
if not BQ_PSI_TABLE:
    raise ValueError("BQ_PSI_TABLE is not set")
if not BQ_AIR_QUALITY_TABLE:
    raise ValueError("BQ_AIR_QUALITY_TABLE is not set")
