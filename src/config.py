"""
Application Configuration
Handles environment variables, default database URLs, and file paths.
"""
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

# Base directory of the project
BASE_DIR = Path(__file__).resolve().parent.parent

# Data Paths
DATA_DIR = BASE_DIR / "data"
INPUT_CSV_PATH = Path(os.getenv("INPUT_CSV_PATH", DATA_DIR / "input" / "BING_MultiDays.csv"))
STAGING_DIR = DATA_DIR / "staging"
OUTPUT_DIR = DATA_DIR / "output"

STAGING_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Database Configuration
# Supports PostgreSQL (Airflow & Warehouse) as well as DuckDB/SQLite for lightweight local runs/testing
DEFAULT_PG_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_PG_URL)

# Pipeline Configuration
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10000"))
DEFAULT_CURRENCY = os.getenv("DEFAULT_CURRENCY", "USD")
