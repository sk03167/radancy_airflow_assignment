"""
Airflow DAG: Bing Advertising Performance Data Warehouse Pipeline
Orchestrates: Extraction -> Data Quality Validation -> Dimensional Transformation -> Warehouse Load -> Quality Audit
"""
from datetime import datetime, timedelta
import logging
from pathlib import Path
import sys

# Ensure project modules can be imported inside Airflow workers
sys.path.insert(0, "/opt/airflow")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.config import DATABASE_URL, INPUT_CSV_PATH, STAGING_DIR
from src.pipeline.extract import extract_raw_csv, stage_raw_data, load_staged_data
from src.pipeline.validate import validate_raw_dataframe
from src.pipeline.transform import transform_all
from src.pipeline.load import load_warehouse
from src.db.connection import get_engine
import pandas as pd
from sqlalchemy import text

logger = logging.getLogger("airflow.task")

default_args = {
    "owner": "radancy-data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def task_extract_and_stage(**context):
    """Task 1: Extracts CSV and stages parquet artifact."""
    logger.info("Starting extraction from: %s", INPUT_CSV_PATH)
    raw_df = extract_raw_csv(INPUT_CSV_PATH)
    stage_path = stage_raw_data(raw_df, "staged_raw.parquet")
    context["ti"].xcom_push(key="staged_file", value=str(stage_path))
    context["ti"].xcom_push(key="raw_row_count", value=len(raw_df))
    logger.info("Successfully staged %d rows to %s", len(raw_df), stage_path)


def task_validate_quality(**context):
    """Task 2: Runs data quality assertions and anomaly checks."""
    raw_df = load_staged_data("staged_raw.parquet")
    valid_df, report = validate_raw_dataframe(raw_df)

    if not report.is_successful:
        raise ValueError(f"Data quality validation failed! Errors: {report.errors}")

    logger.info(
        "Data Quality Passed: %d valid records, %d warnings, %d anomalies",
        report.valid_records,
        len(report.warnings),
        report.anomalies_detected,
    )
    # Re-stage sanitized dataset for transformations
    stage_raw_data(valid_df, "validated_data.parquet")
    context["ti"].xcom_push(key="valid_count", value=report.valid_records)


def task_transform(**context):
    """Task 3: Transforms flat data into dimensional Star Schema and calculated KPIs."""
    valid_df = load_staged_data("validated_data.parquet")
    data = transform_all(valid_df)

    # Stage transformed datasets
    data.dim_date.to_parquet(STAGING_DIR / "dim_date.parquet", index=False)
    data.dim_account.to_parquet(STAGING_DIR / "dim_account.parquet", index=False)
    data.dim_campaign.to_parquet(STAGING_DIR / "dim_campaign.parquet", index=False)
    data.dim_ad_group.to_parquet(STAGING_DIR / "dim_ad_group.parquet", index=False)
    data.dim_ad.to_parquet(STAGING_DIR / "dim_ad.parquet", index=False)
    data.fact_performance.to_parquet(STAGING_DIR / "fact_performance.parquet", index=False)

    logger.info("Transformed dimensions and fact table staged successfully.")


def task_load_warehouse(**context):
    """Task 4: Loads dimensions and facts into the data warehouse idempotently."""
    from src.pipeline.transform import TransformedData

    data = TransformedData(
        dim_date=pd.read_parquet(STAGING_DIR / "dim_date.parquet"),
        dim_account=pd.read_parquet(STAGING_DIR / "dim_account.parquet"),
        dim_campaign=pd.read_parquet(STAGING_DIR / "dim_campaign.parquet"),
        dim_ad_group=pd.read_parquet(STAGING_DIR / "dim_ad_group.parquet"),
        dim_ad=pd.read_parquet(STAGING_DIR / "dim_ad.parquet"),
        fact_performance=pd.read_parquet(STAGING_DIR / "fact_performance.parquet"),
    )

    engine = get_engine(DATABASE_URL)
    counts = load_warehouse(data, engine=engine)
    logger.info("Successfully loaded tables: %s", counts)


def task_verify_warehouse(**context):
    """Task 5: Post-load referential integrity check and BI view query."""
    engine = get_engine(DATABASE_URL)
    with engine.connect() as conn:
        fact_count = conn.execute(text("SELECT COUNT(*) FROM fact_ad_performance_daily;")).scalar()
        logger.info("Total rows in fact_ad_performance_daily: %d", fact_count)

        # Integrity check: Ensure no orphaned fact records
        orphans = conn.execute(text("""
            SELECT COUNT(*) FROM fact_ad_performance_daily f
            LEFT JOIN dim_account a ON f.account_id = a.account_id
            WHERE a.account_id IS NULL;
        """)).scalar()
        if orphans > 0:
            raise ValueError(f"Referential integrity failure: Found {orphans} orphaned records in fact table!")

        # Query top campaigns view
        leaderboard = pd.read_sql(
            text("SELECT campaign_name, spend, impressions, clicks, ctr FROM vw_campaign_leaderboard LIMIT 5;"),
            conn,
        )
        logger.info("Top 5 Campaigns by Spend:\n%s", leaderboard.to_string(index=False))


with DAG(
    dag_id="bing_advertising_performance_pipeline",
    default_args=default_args,
    description="Parses Bing Advertising performance data and loads it into a Star Schema data warehouse.",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["radancy", "etl", "advertising", "warehouse"],
) as dag:

    extract_stage = PythonOperator(
        task_id="extract_and_stage_raw",
        python_callable=task_extract_and_stage,
    )

    validate_quality = PythonOperator(
        task_id="validate_data_quality",
        python_callable=task_validate_quality,
    )

    transform = PythonOperator(
        task_id="transform_dimensional_models",
        python_callable=task_transform,
    )

    load = PythonOperator(
        task_id="load_data_warehouse",
        python_callable=task_load_warehouse,
    )

    verify = PythonOperator(
        task_id="verify_warehouse_and_views",
        python_callable=task_verify_warehouse,
    )

    extract_stage >> validate_quality >> transform >> load >> verify
