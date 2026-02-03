import logging
from pathlib import Path
from common.runtime import RUN_ID

# Create logs directory if it doesn't exist
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR /f"air_quality_etl_{RUN_ID}.log"

logger = logging.getLogger("air_quality_etl")

logger.setLevel(logging.INFO)

if not logger.handlers:
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
    formatter = logging.Formatter(
        f"%(asctime)s | run_id={RUN_ID} | %(levelname)s | %(message)s"
    )
    for handler in handlers:
        handler.setFormatter(formatter)
        logger.addHandler(handler)
