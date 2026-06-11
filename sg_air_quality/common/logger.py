import logging
import os
from pathlib import Path
from sg_air_quality.common.runtime import get_run_id


airflow_logs = os.environ.get("AIRFLOW_HOME")
LOG_DIR = Path(airflow_logs) / "logs" if airflow_logs else Path("logs")

def setup_logging():
    # Move LOG_DIR.mkdir and LOG_FILE creation here
    LOG_DIR.mkdir(exist_ok=True)
    LOG_FILE = LOG_DIR / f"air_quality_etl_{get_run_id()}.log"

    formatter = logging.Formatter(
        f"%(asctime)s | run_id={get_run_id()} | %(levelname)s | %(name)s | %(etl_module)s | %(funcName)s | %(message)s"
    )

    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
    
    root_logger = logging.getLogger("air_quality_etl")
    root_logger.setLevel(logging.INFO)
    root_logger.propagate = False

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