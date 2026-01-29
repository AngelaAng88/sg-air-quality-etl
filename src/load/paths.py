from pathlib import Path

DATA_DIR = Path("data")

def pm25_csv_path(date: str, toArchive: bool) -> Path:
    if toArchive:
        return Path("data") / "pm25" / f"date={date}" / "data.csv"
    else:
        return Path("data") / "latest" / f"latest_pm25_data.csv"

def psi_csv_path(date: str, toArchive: bool) -> Path:
    if toArchive:
        return Path("data") / "psi" / f"date={date}" / "data.csv"
    else:
        return Path("data") / "latest" / f"latest_psi_data.csv"

def air_quality_csv_path(date: str, toArchive: bool) -> Path:
    if toArchive:
        return Path("data") / "air_quality" / f"date={date}" / "data.csv"
    else:
        return Path("data") / "latest" / f"latest_air_quality_data.csv"