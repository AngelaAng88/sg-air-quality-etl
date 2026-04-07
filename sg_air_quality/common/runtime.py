from datetime import datetime, timedelta

RUN_ID = datetime.now().strftime("%Y%m%dT%H%M%S")
TODAY = datetime.now().strftime("%Y-%m-%d")
YESTERDAY = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

def to_archive(input_date: str) -> bool:
    return input_date != TODAY

def set_input_date(input_date: str) -> str:
    if not input_date:
        return YESTERDAY
    return input_date