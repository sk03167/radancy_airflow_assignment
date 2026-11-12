"""
Warehouse Loading Module
Handles idempotent loading of dimensional models and fact tables into the target data warehouse.
Supports PostgreSQL (production/Airflow) and SQLite/DuckDB (lightweight local execution & testing).
"""
import json
import logging
from sqlalchemy import text
from sqlalchemy.engine import Engine
import pandas as pd

from src.db.connection import get_engine, init_db
from src.db.models import Base
from src.pipeline.transform import TransformedData
from src.pipeline.validate import ValidationReport
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert

logger = logging.getLogger(__name__)


def load_dataframe_idempotent(
    df: pd.DataFrame,
    table_name: str,
    engine: Engine,
    conflict_columns: list[str],
) -> int:
    """
    Idempotently loads a DataFrame into a target SQL table using native SQLAlchemy dialect inserts.
    - For PostgreSQL: Uses ON CONFLICT DO NOTHING.
    - For SQLite: Uses INSERT OR IGNORE.
    - Eliminates intermediate temp tables and avoids pandas/sqlalchemy connection cursor mismatches.
    """
    if df.empty:
        logger.info("Table %s: DataFrame is empty, nothing to load.", table_name)
        return 0

    dialect = engine.dialect.name
    logger.info("Loading %d rows into %s (dialect=%s)", len(df), table_name, dialect)

    table = Base.metadata.tables.get(table_name)
    records = df.to_dict(orient="records")

    with engine.begin() as conn:
        if dialect == "sqlite" and table is not None:
            # Batch in chunks of 500 to stay well under SQLite parameter limits
            batch_size = 500
            for i in range(0, len(records), batch_size):
                batch = records[i : i + batch_size]
                stmt = sqlite_insert(table).values(batch).prefix_with("OR IGNORE")
                conn.execute(stmt)
            return len(df)

        elif dialect == "postgresql" and table is not None:
            # Batch in chunks of 2000 for efficient Postgres bulk execution
            batch_size = 2000
            for i in range(0, len(records), batch_size):
                batch = records[i : i + batch_size]
                stmt = pg_insert(table).values(batch).on_conflict_do_nothing(index_elements=conflict_columns)
                conn.execute(stmt)
            return len(df)

        else:
            # Fallback for generic SQL engines
            df.to_sql(table_name, con=engine, if_exists="append", index=False)
            return len(df)


def create_analytics_views(engine: Engine) -> None:
    """
    Creates analytical views optimized for downstream BI tools and automated Insights Agents.
    Supports dialect-specific view creation (PostgreSQL: CREATE OR REPLACE, SQLite: CREATE VIEW IF NOT EXISTS).
    """
    dialect = engine.dialect.name
    view_prefix = "CREATE OR REPLACE VIEW" if dialect == "postgresql" else "CREATE VIEW IF NOT EXISTS"

    vw_ad_performance_summary = f"""
    {view_prefix} vw_ad_performance_summary AS
    SELECT 
        d.full_date,
        acc.account_name,
        c.campaign_name,
        ag.ad_group_name,
        ad.ad_title,
        f.device_type,
        f.device_os,
        f.network,
        SUM(f.impressions) AS total_impressions,
        SUM(f.clicks) AS total_clicks,
        ROUND(CAST(SUM(f.spend) AS NUMERIC), 2) AS total_spend,
        ROUND(CAST(SUM(f.conversions) AS NUMERIC), 2) AS total_conversions,
        CASE WHEN SUM(f.impressions) > 0 
             THEN ROUND(CAST(SUM(f.clicks) * 1.0 / SUM(f.impressions) AS NUMERIC), 4) 
             ELSE 0 END AS ctr,
        CASE WHEN SUM(f.clicks) > 0 
             THEN ROUND(CAST(SUM(f.spend) * 1.0 / SUM(f.clicks) AS NUMERIC), 4) 
             ELSE 0 END AS cpc,
        CASE WHEN SUM(f.conversions) > 0 
             THEN ROUND(CAST(SUM(f.spend) * 1.0 / SUM(f.conversions) AS NUMERIC), 4) 
             ELSE NULL END AS cpa
    FROM fact_ad_performance_daily f
    JOIN dim_date d ON f.date_id = d.date_id
    JOIN dim_account acc ON f.account_id = acc.account_id
    JOIN dim_campaign c ON f.campaign_id = c.campaign_id
    JOIN dim_ad_group ag ON f.ad_group_id = ag.ad_group_id
    JOIN dim_ad ad ON f.ad_id = ad.ad_id
    GROUP BY 
        d.full_date,
        acc.account_name,
        c.campaign_name,
        ag.ad_group_name,
        ad.ad_title,
        f.device_type,
        f.device_os,
        f.network;
    """

    vw_campaign_leaderboard = f"""
    {view_prefix} vw_campaign_leaderboard AS
    SELECT 
        c.campaign_name,
        acc.account_name,
        SUM(f.impressions) AS impressions,
        SUM(f.clicks) AS clicks,
        ROUND(CAST(SUM(f.spend) AS NUMERIC), 2) AS spend,
        ROUND(CAST(SUM(f.conversions) AS NUMERIC), 2) AS conversions,
        CASE WHEN SUM(f.impressions) > 0 
             THEN ROUND(CAST(SUM(f.clicks) * 1.0 / SUM(f.impressions) AS NUMERIC), 4) 
             ELSE 0 END AS ctr,
        CASE WHEN SUM(f.conversions) > 0 
             THEN ROUND(CAST(SUM(f.spend) * 1.0 / SUM(f.conversions) AS NUMERIC), 4) 
             ELSE NULL END AS cpa
    FROM fact_ad_performance_daily f
    JOIN dim_campaign c ON f.campaign_id = c.campaign_id
    JOIN dim_account acc ON f.account_id = acc.account_id
    GROUP BY c.campaign_name, acc.account_name
    ORDER BY spend DESC;
    """

    logger.info("Registering analytical views for Insights Agents and BI...")
    with engine.begin() as conn:
        conn.execute(text(vw_ad_performance_summary))
        conn.execute(text(vw_campaign_leaderboard))
        logger.info("Successfully registered analytical views.")


def load_warehouse(data: TransformedData, engine: Engine | None = None) -> dict[str, int]:
    """
    Loads transformed dimensional and fact datasets in foreign key dependency order.
    Returns record counts for each loaded entity.
    """
    if engine is None:
        engine = get_engine()

    # Ensure tables are created
    init_db(engine)

    counts = {}

    # 1. Load Dimensions (in dependency order)
    counts["dim_date"] = load_dataframe_idempotent(
        data.dim_date, "dim_date", engine, conflict_columns=["date_id"]
    )
    counts["dim_account"] = load_dataframe_idempotent(
        data.dim_account, "dim_account", engine, conflict_columns=["account_id"]
    )
    counts["dim_campaign"] = load_dataframe_idempotent(
        data.dim_campaign, "dim_campaign", engine, conflict_columns=["campaign_id"]
    )
    counts["dim_ad_group"] = load_dataframe_idempotent(
        data.dim_ad_group, "dim_ad_group", engine, conflict_columns=["ad_group_id"]
    )
    counts["dim_ad"] = load_dataframe_idempotent(
        data.dim_ad, "dim_ad", engine, conflict_columns=["ad_id"]
    )

    # 2. Load Fact Table
    counts["fact_ad_performance_daily"] = load_dataframe_idempotent(
        data.fact_performance,
        "fact_ad_performance_daily",
        engine,
        conflict_columns=["performance_id"],
    )

    # 3. Create / Refresh Views
    create_analytics_views(engine)

    logger.info("Warehouse load complete. Loaded records: %s", counts)
    return counts


def record_pipeline_audit(
    engine: Engine,
    run_id: str,
    pipeline_name: str,
    report: ValidationReport,
    duration_sec: float = 0.0,
    status: str = "SUCCESS",
) -> None:
    """
    Persists data quality validation results and execution statistics into audit_pipeline_runs table.
    Enables historical observability and automated monitoring for Insights Agents.
    """
    init_db(engine)
    audit_record = {
        "run_id": str(run_id),
        "pipeline_name": str(pipeline_name),
        "status": str(status),
        "total_records": int(report.total_records),
        "valid_records": int(report.valid_records),
        "rejected_records": int(report.rejected_records),
        "anomalies_detected": int(report.anomalies_detected),
        "warnings": json.dumps(report.warnings) if report.warnings else None,
        "errors": json.dumps(report.errors) if report.errors else None,
        "metrics_summary": json.dumps(report.metrics_summary) if report.metrics_summary else None,
        "execution_duration_sec": float(round(duration_sec, 2)),
        "executed_at": pd.Timestamp.now(tz="UTC"),
    }
    audit_df = pd.DataFrame([audit_record])
    load_dataframe_idempotent(
        audit_df,
        table_name="audit_pipeline_runs",
        engine=engine,
        conflict_columns=["run_id"],
    )
    logger.info("Successfully recorded pipeline audit stats for run '%s' (status=%s)", run_id, status)
