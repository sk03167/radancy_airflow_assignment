"""
End-to-End Pipeline Runner
Allows standalone execution of the ETL pipeline outside of Airflow for local testing,
profiling, and rapid evaluation.

Usage:
    python src/run_pipeline.py [--db-url SQLALCHEMY_URL]
"""
import argparse
import logging
from pathlib import Path
import sys
import time

# Ensure repository root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from sqlalchemy import text

from src.config import DATABASE_URL, INPUT_CSV_PATH
from src.db.connection import get_engine
from src.pipeline.extract import extract_raw_csv, stage_raw_data
from src.pipeline.validate import validate_raw_dataframe
from src.pipeline.transform import transform_all
from src.pipeline.load import load_warehouse, record_pipeline_audit

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("ETLRunner")


def run_pipeline(db_url: str | None = None, input_csv: str | None = None):
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("STARTING BING ADS PERFORMANCE ETL PIPELINE")
    logger.info("=" * 60)

    # 1. Extraction & Staging
    logger.info("--- Step 1: Extraction & Staging ---")
    raw_df = extract_raw_csv(input_csv)
    stage_path = stage_raw_data(raw_df)
    logger.info("Staged %d raw rows to %s", len(raw_df), stage_path)

    # 2. Validation
    logger.info("--- Step 2: Data Quality & Validation ---")
    valid_df, report = validate_raw_dataframe(raw_df)
    logger.info("Validation Status: %s", "PASSED" if report.is_successful else "FAILED")
    logger.info("Valid Records: %d | Rejected: %d | Anomalies: %d", 
                report.valid_records, report.rejected_records, report.anomalies_detected)
    if report.warnings:
        for w in report.warnings:
            logger.warning("  Validation Warning: %s", w)
    if not report.is_successful:
        for e in report.errors:
            logger.error("  Validation Error: %s", e)
        raise ValueError("Pipeline aborted due to critical validation errors.")

    # 3. Transformation
    logger.info("--- Step 3: Dimensional Modeling & Transformation ---")
    data = transform_all(valid_df)

    # 4. Loading
    target_url = db_url or DATABASE_URL
    logger.info("--- Step 4: Loading into Data Warehouse (%s) ---", target_url)
    engine = get_engine(target_url)
    counts = load_warehouse(data, engine=engine)

    # 5. Warehouse Verification & Summary
    logger.info("--- Step 5: Verification & Business Intelligence Check ---")
    with engine.connect() as conn:
        try:
            summary = pd.read_sql(
                text("SELECT campaign_name, spend, impressions, clicks, ctr FROM vw_campaign_leaderboard LIMIT 5;"),
                conn,
            )
            logger.info("Top 5 Campaigns by Spend:\n%s", summary.to_string(index=False))
        except Exception as e:
            logger.warning("Could not query view: %s", e)

    elapsed = round(time.time() - start_time, 2)

    # 6. Audit & Data Quality Logging
    run_id = f"local_run_{int(start_time)}"
    record_pipeline_audit(
        engine=engine,
        run_id=run_id,
        pipeline_name="bing_advertising_performance",
        report=report,
        duration_sec=elapsed,
        status="WARNING" if report.warnings else "SUCCESS",
    )

    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETED SUCCESSFULLY IN %.2f SECONDS", elapsed)
    logger.info("Loaded Entities: %s", counts)
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Bing Ads ETL Pipeline")
    parser.add_argument("--db-url", type=str, default=None, help="Target SQLAlchemy database connection URL")
    parser.add_argument("--input-csv", type=str, default=str(INPUT_CSV_PATH), help="Path to input raw CSV file")
    args = parser.parse_args()

    # If running locally and default Postgres is unreachable, fallback gracefully to local SQLite
    selected_url = args.db_url
    if not selected_url:
        try:
            test_engine = get_engine(DATABASE_URL)
            with test_engine.connect() as conn:
                pass
            selected_url = DATABASE_URL
        except Exception:
            fallback_sqlite = "sqlite:///data/warehouse.db"
            logger.warning("Default database at '%s' is unreachable. Falling back to local warehouse '%s'.", DATABASE_URL, fallback_sqlite)
            selected_url = fallback_sqlite

    run_pipeline(db_url=selected_url, input_csv=args.input_csv)
