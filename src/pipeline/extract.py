"""
Extraction & Staging Module
Extracts raw advertising performance records from CSV, standardizes column headers,
and stages raw data to disk/database for lineage and downstream processing.
"""
import logging
from pathlib import Path
import pandas as pd
from src.config import INPUT_CSV_PATH, STAGING_DIR

logger = logging.getLogger(__name__)

COLUMN_RENAME_MAP = {
    "Date": "raw_date",
    "Customer": "customer",
    "Account number": "account_number",
    "Account name": "account_name",
    "Account status": "account_status",
    "Campaign name": "campaign_name",
    "Campaign status": "campaign_status",
    "Ad group ID": "ad_group_id_raw",
    "Ad group": "ad_group",
    "Ad group status": "ad_group_status",
    "Ad ID": "ad_id_raw",
    "Ad description": "ad_description",
    "Ad distribution": "ad_distribution",
    "Ad status": "ad_status",
    "Ad title": "ad_title",
    "Ad type": "ad_type",
    "Tracking Template": "tracking_template",
    "Custom Parameters": "custom_parameters",
    "Final Mobile URL": "final_mobile_url",
    "Final URL": "final_url",
    "Top vs. other": "top_vs_other",
    "Display URL": "display_url",
    "Final App URL": "final_app_url",
    "Destination URL": "destination_url",
    "Device type": "device_type",
    "Device OS": "device_os",
    "Delivered match type": "delivered_match_type",
    "BidMatchType": "bid_match_type",
    "Language": "language",
    "Network": "network",
    "Currency code": "currency_code",
    "Impressions": "impressions",
    "Clicks": "clicks",
    "Spend": "spend",
    "Avg. position": "avg_position",
    "Conversions": "conversions",
    "Assists": "assists",
}


def extract_raw_csv(csv_path: Path | str | None = None) -> pd.DataFrame:
    """
    Reads the raw Bing advertising CSV file, validates presence,
    normalizes column headers to snake_case, and returns a DataFrame.
    """
    file_path = Path(csv_path) if csv_path else INPUT_CSV_PATH
    if not file_path.exists():
        raise FileNotFoundError(f"Input file not found at: {file_path}")

    logger.info("Reading raw dataset from %s", file_path)
    # Using low_memory=False to prevent mixed-type inference warnings
    df = pd.read_csv(file_path, low_memory=False)
    logger.info("Successfully loaded %d raw rows and %d columns", len(df), len(df.columns))

    # Strip whitespace from headers
    df.columns = [col.strip() for col in df.columns]

    # Map headers to standardized naming
    df = df.rename(columns=COLUMN_RENAME_MAP)
    return df


def stage_raw_data(df: pd.DataFrame, staging_filename: str = "staged_raw.parquet") -> Path:
    """
    Persists staged raw data into parquet for fast local I/O between Airflow tasks.
    """
    output_path = STAGING_DIR / staging_filename
    logger.info("Persisting staged raw data to %s", output_path)
    df.to_parquet(output_path, index=False)
    return output_path


def load_staged_data(staging_filename: str = "staged_raw.parquet") -> pd.DataFrame:
    """
    Loads staged parquet file for downstream pipeline tasks.
    """
    input_path = STAGING_DIR / staging_filename
    if not input_path.exists():
        raise FileNotFoundError(f"Staged file not found at: {input_path}")
    return pd.read_parquet(input_path)
