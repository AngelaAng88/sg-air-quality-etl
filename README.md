# sg-air-quality-etl

A real-time ETL pipeline that ingests Singapore air quality data from NEA's public API and loads it into Google BigQuery for analysis.

---

## Overview

This pipeline pulls two air quality metrics published by Singapore's National Environment Agency (NEA) via the [data.gov.sg](https://data.gov.sg) open data API:

- **PM2.5** — Hourly fine particulate matter readings by region
- **PSI** — Pollutant Standards Index readings by region

Data is extracted, transformed, and loaded into three BigQuery tables: a PM2.5 table, a PSI table, and a consolidated air quality table combining both metrics.

## Architecture

```
NEA API (data.gov.sg)
        │
        ▼
  Python ETL Pipeline
        │
        ├── Extract    →  Fetch PM2.5 & PSI readings + region metadata via REST API
        ├── Transform  →  Flatten, type-cast, enrich with date, sort by region/timestamp
        └── Load       →  Write to CSV (local archive) + Google BigQuery (WRITE_APPEND)
```

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3 |
| Data Processing | pandas |
| Data Warehouse | Google BigQuery |
| BQ Connector | google-cloud-bigquery, pandas-gbq, pyarrow |
| Config Management | python-dotenv |
| Source API | NEA via data.gov.sg (REST) |

## Project Structure

```
sg-air-quality-etl/
├── sg_air_quality/
│   ├── common/
│   │   ├── http.py             # Shared HTTP fetch utility
│   │   ├── logger.py           # Logging setup
│   │   └── runtime.py          # Date helpers (get_today, get_yesterday, to_archive)
│   ├── config/
│   │   └── settings.py         # Loads and validates all environment variables
│   ├── extract/
│   │   ├── pm25.py             # Fetches PM2.5 JSON; extracts readings & region metadata
│   │   └── psi.py              # Fetches PSI JSON; extracts readings & region metadata
│   ├── transform/
│   │   ├── pm25.py             # Flatten, type-cast, reorder, sort PM2.5 DataFrame
│   │   ├── psi.py              # Flatten, type-cast, reorder, sort PSI DataFrame
│   │   └── air_quality.py      # Merge PM2.5 + PSI, transform and sort combined DataFrame
│   ├── load/
│   │   ├── bigquery_loader.py  # Appends DataFrame to BigQuery table (adds ingested_at, data_source)
│   │   ├── csv_loader.py       # Saves DataFrame to CSV
│   │   └── paths.py            # Resolves output CSV paths (latest vs. date-partitioned archive)
│   ├── pipeline/
│   │   └── runner.py           # run_single_etl() — generic ETL runner used by PM2.5 and PSI
│   ├── scripts/
│   │   └── run_backfill.ps1    # Run from the project root with .venv activated
│   └── main.py                 # Entry point; orchestrates full ETL run for a given date
├── .env.example                # Environment variable template
├── requirements.txt            # Python dependencies
├── setup.py                    # Package setup
└── README.md
```

## Prerequisites

- Python 3.8+
- Google Cloud project with BigQuery enabled
- GCP service account key (JSON) with BigQuery Data Editor permissions
- API key for [data.gov.sg](https://data.gov.sg) (free registration required)

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/AngelaAng88/sg-air-quality-etl.git
cd sg-air-quality-etl
```

### 2. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```env
# data.gov.sg API key
DATA_GOV_SG_API_KEY=your_api_key_here

# Path to your GCP service account key file
GOOGLE_APPLICATION_CREDENTIALS=your_google_credentials.json

# API endpoints
PM25_API_URI=https://api-open.data.gov.sg/v2/real-time/api/pm25
PSI_API_URI=https://api-open.data.gov.sg/v2/real-time/api/psi

# BigQuery table references (project.dataset.table)
BQ_PM25_TABLE=your_project.your_dataset.pm25
BQ_PSI_TABLE=your_project.your_dataset.psi
BQ_AIR_QUALITY_TABLE=your_project.your_dataset.air_quality
```

### 3. Install Python dependencies (for local development)

```bash
pip install -r requirements.txt
# or install as a package
pip install -e .
```

### 4. Run the pipeline

```bash
# Run for yesterday (default)
python -m sg_air_quality.main

# Run for a specific date
python -m sg_air_quality.main --date 2025-01-15
```

### 5. Backfill historical data
For loading a full historical date range (typically done once, right after initial setup), use the backfill script rather than calling `main.py` for each date manually.
```bash
.\scripts\run_backfill.ps1
```
This loops through each date in the configured range and calls `python -m sg_air_quality.main --date <date>` once per day.

Configuring the date range — edit the `$startDate` and `$endDate` variables at the top of `scripts/run_backfill.ps1`:
```bash
$startDate = Get-Date "2025-07-23"
$endDate = Get-Date "2026-07-22"
```
Note: the script does not stops when encountered on the first failed date (non-zero exit code from main.py). Check the console output for which date failed before re-running.

## How It Works

Each pipeline run processes a single date (defaults to yesterday). For each metric (PM2.5 and PSI), the pipeline:

1. Fetches hourly readings and region metadata from the NEA API
2. Extracts readings into a DataFrame (timestamp, region, value)
3. Merges readings with region metadata (latitude, longitude)
4. Applies type casting, date enrichment, and sorts by timestamp and region
5. Saves output to CSV and appends to the corresponding BigQuery table

After both individual ETLs complete, PM2.5 and PSI data are merged into a consolidated air quality DataFrame, which is saved to CSV and loaded into a third BigQuery table.

Dates other than today are written to date-partitioned archive paths (`data/<metric>/date=YYYY-MM-DD/data.csv`); today's data writes to `data/latest/`.

## Data Sources

| Metric | Endpoint |
|--------|----------|
| PM2.5 | `https://api-open.data.gov.sg/v2/real-time/api/pm25` |
| PSI | `https://api-open.data.gov.sg/v2/real-time/api/psi` |

Data is provided by the National Environment Agency (NEA) under the [Singapore Open Data Licence](https://data.gov.sg/open-data-licence).

## BigQuery Schema

Three tables are populated by this pipeline. Each row includes an `ingested_at` timestamp (SGT) and a `data_source` field set to `data.gov.sg`.

| Table | Key Columns |
|-------|-------------|
| `pm25` | date, timestamp, region, latitude, longitude, pm25_value |
| `psi` | date, timestamp, region, latitude, longitude, *(PSI sub-indexes)* |
| `air_quality` | Combined PM2.5 and PSI columns |

## License

This project is for personal and educational use. Air quality data sourced from NEA via data.gov.sg is subject to the [Singapore Open Data Licence v1.0](https://data.gov.sg/open-data-licence).
