"""
Warehouse Loading Module
Handles idempotent loading of dimensional models and fact tables into the target data warehouse.
Supports PostgreSQL (production/Airflow) and SQLite/DuckDB (lightweight local execution & testing).
"""
import logging
from sqlalchemy import text
from sqlalchemy.engine import Engine
import pandas as pd

from src.db.connection import get_engine, init_db
from src.pipeline.transform import TransformedData

logger = logging.getLogger(__name__)


def load_dataframe_idempotent(
    df: pd.DataFrame,
    table_name: str,
    engine: Engine,
    conflict_columns: list[str],
) -> int:
    """
    Idempotently loads a DataFrame into a target SQL table.
    - For PostgreSQL: Uses ON CONFLICT DO NOTHING.
    - For SQLite: Uses INSERT OR IGNORE.
    - Fallback: Filters out existing records via anti-join.
    """
    if df.empty:
        logger.info("Table %s: DataFrame is empty, nothing to load.", table_name)
        return 0

    dialect = engine.dialect.name
    logger.info("Loading %d rows into %s (dialect=%s)", len(df), table_name, dialect)

    with engine.begin() as conn:
        if dialect == "sqlite":
            # For SQLite, stage to temp table and insert or ignore
            temp_table = f"_tmp_{table_name}"
            df.to_sql(temp_table, conn, if_exists="replace", index=False)
            cols = ", ".join([f'"{col}"' for col in df.columns])
            insert_query = f"""
                INSERT OR IGNORE INTO {table_name} ({cols})
                SELECT {cols} FROM {temp_table};
            """
            conn.execute(text(insert_query))
            conn.execute(text(f"DROP TABLE IF EXISTS {temp_table};"))
            return len(df)

        elif dialect == "postgresql":
            # For PostgreSQL, stage to temporary unlogged table and upsert with ON CONFLICT DO NOTHING
            temp_table = f"_tmp_{table_name}"
            df.to_sql(temp_table, conn, if_exists="replace", index=False)
            cols = ", ".join([f'"{col}"' for col in df.columns])
            conflict_target = ", ".join([f'"{col}"' for col in conflict_columns])
            upsert_query = f"""
                INSERT INTO {table_name} ({cols})
                SELECT {cols} FROM {temp_table}
                ON CONFLICT ({conflict_target}) DO NOTHING;
            """
            conn.execute(text(upsert_query))
            conn.execute(text(f"DROP TABLE IF EXISTS {temp_table};"))
            return len(df)

        else:
            # Generic fallback: filter by primary conflict columns
            try:
                existing_keys = pd.read_sql(
                    f"SELECT {', '.join(conflict_columns)} FROM {table_name}", conn
                )
                if not existing_keys.empty:
                    merged = df.merge(existing_keys, on=conflict_columns, how="left", indicator=True)
                    new_records = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
                else:
                    new_records = df
                if not new_records.empty:
                    new_records.to_sql(table_name, conn, if_exists="append", index=False)
                return len(new_records)
            except Exception:
                df.to_sql(table_name, conn, if_exists="append", index=False)
                return len(df)


def create_analytics_views(engine: Engine) -> None:
    """
    Creates analytical views optimized for downstream BI tools and automated Insights Agents.
    """
    vw_ad_performance_summary = """
    CREATE VIEW IF NOT EXISTS vw_ad_performance_summary AS
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

    vw_campaign_leaderboard = """
    CREATE VIEW IF NOT EXISTS vw_campaign_leaderboard AS
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
        try:
            conn.execute(text(vw_ad_performance_summary))
            conn.execute(text(vw_campaign_leaderboard))
            logger.info("Successfully registered analytical views.")
        except Exception as e:
            logger.warning("Could not create views (dialect may require alternate syntax): %s", e)


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
