from datetime import datetime, timedelta

def get_run_id() -> str:
    return datetime.now().strftime("%Y%m%dT%H%M%S")

def get_today() -> str:
    return datetime.now().strftime("%Y-%m-%d")

def get_yesterday() -> str:
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

def to_archive(input_date: str) -> bool:
    return input_date != get_today()

def set_input_date(input_date: str) -> str:
    if not input_date:
        return get_yesterday()
    return input_date