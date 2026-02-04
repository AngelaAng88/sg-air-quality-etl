import logging
from pathlib import Path
from sg_air_quality.common.runtime import RUN_ID

# Create logs directory if it doesn't exist
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR /f"air_quality_etl_{RUN_ID}.log"

def setup_logging():
    formatter = logging.Formatter(
        f"%(asctime)s | run_id={RUN_ID} | %(levelname)s | %(name)s | %(etl_module)s | %(funcName)s | %(message)s"
    )

    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
    
    root_logger = logging.getLogger("air_quality_etl")
    root_logger.setLevel(logging.INFO)

    if not root_logger.handlers:
        for handler in handlers:
            handler.setFormatter(formatter)
            root_logger.addHandler(handler)


def get_logger(module_name: str):
    base_logger = logging.getLogger("air_quality_etl")
    return logging.LoggerAdapter(
        base_logger,
        {"etl_module": module_name}
    )