"""
Unit & Integration Test Suite
Validates schema transformations, data quality rules, derived KPIs, and referential integrity.
"""
from pathlib import Path
import pytest
import pandas as pd
from sqlalchemy import create_engine, text

from src.pipeline.extract import extract_raw_csv, COLUMN_RENAME_MAP
from src.pipeline.validate import validate_raw_dataframe, ValidationReport
from src.pipeline.transform import (
    clean_id,
    generate_surrogate_key,
    transform_all,
)
from src.pipeline.load import load_warehouse, record_pipeline_audit


def test_clean_id():
    """Verifies that square brackets and dirty chars are properly stripped from IDs."""
    assert clean_id("[2709719083]") == 2709719083
    assert clean_id("6772783690") == 6772783690
    assert clean_id("[ 12345 ]") == 12345
    with pytest.raises(ValueError):
        clean_id(None)
    with pytest.raises(ValueError):
        clean_id("no-digits-here")


def test_generate_surrogate_key():
    """Verifies that surrogate key generation is deterministic and case-insensitive."""
    key1 = generate_surrogate_key("ACC123", "Brand_Campaign")
    key2 = generate_surrogate_key("acc123", "brand_campaign")
    key3 = generate_surrogate_key("ACC123", "Other_Campaign")
    assert key1 == key2
    assert key1 != key3
    assert len(key1) == 16


def test_validation_flags_negative_metrics():
    """Verifies that negative metrics are detected and rejected."""
    sample_df = pd.DataFrame([{
        "raw_date": "1/11/2026",
        "account_number": "ACC1",
        "campaign_name": "Camp1",
        "ad_group_id_raw": "[100]",
        "ad_id_raw": "[200]",
        "impressions": -5,
        "clicks": 1,
        "spend": 10.0,
        "conversions": 0,
    }])
    cleaned_df, report = validate_raw_dataframe(sample_df)
    assert not report.is_successful
    assert any("negative values" in err for err in report.errors)


def test_validation_detects_click_anomalies():
    """Verifies that clicks > impressions is flagged as an ad-tech warning without corrupting data."""
    sample_df = pd.DataFrame([{
        "raw_date": "1/11/2026",
        "account_number": "ACC1",
        "campaign_name": "Camp1",
        "ad_group_id_raw": "[100]",
        "ad_id_raw": "[200]",
        "impressions": 1,
        "clicks": 2,
        "spend": 1.0,
        "conversions": 0,
    }])
    cleaned_df, report = validate_raw_dataframe(sample_df)
    assert report.is_successful
    assert report.anomalies_detected == 1
    assert any("clicks > impressions" in w for w in report.warnings)


def test_kpi_calculations():
    """Verifies accuracy of CTR, CPC, CPA, and conversion rate calculations."""
    sample_df = pd.DataFrame([{
        "raw_date": "1/11/2026",
        "customer": "12345",
        "account_number": "ACC1",
        "account_name": "Account 1",
        "account_status": "Active",
        "campaign_name": "Camp1",
        "campaign_status": "Active",
        "ad_group_id_raw": "[100]",
        "ad_group": "Group 1",
        "ad_group_status": "Active",
        "ad_id_raw": "[200]",
        "ad_description": "Sample",
        "ad_distribution": "Search",
        "ad_status": "Active",
        "ad_title": "Title 1",
        "ad_type": "Text ad",
        "tracking_template": None,
        "custom_parameters": None,
        "final_mobile_url": None,
        "final_url": "http://example.com",
        "top_vs_other": "Top",
        "display_url": "example.com",
        "final_app_url": None,
        "destination_url": None,
        "device_type": "Computer",
        "device_os": "Windows",
        "delivered_match_type": "Exact",
        "bid_match_type": "Exact",
        "language": "English",
        "network": "Search",
        "currency_code": "USD",
        "impressions": 100,
        "clicks": 10,
        "spend": 50.0,
        "avg_position": 1.5,
        "conversions": 2.0,
        "assists": 0.0,
    }])

    data = transform_all(sample_df)
    fact_row = data.fact_performance.iloc[0]

    # CTR = 10 / 100 = 0.1
    assert fact_row["ctr"] == pytest.approx(0.1)
    # CPC = 50.0 / 10 = 5.0
    assert fact_row["cpc"] == pytest.approx(5.0)
    # CPA = 50.0 / 2.0 = 25.0
    assert fact_row["cpa"] == pytest.approx(25.0)
    # Conversion Rate = 2.0 / 10 = 0.2
    assert fact_row["conversion_rate"] == pytest.approx(0.2)


def test_referential_integrity_and_idempotency():
    """
    Integration test verifying:
    1. Full extraction to warehouse loading in an in-memory SQLite database.
    2. Referential integrity: all fact foreign keys exist in dimension tables.
    3. Idempotency: re-running does not produce duplicate rows or primary key violations.
    """
    raw_df = extract_raw_csv()
    valid_df, report = validate_raw_dataframe(raw_df)
    assert report.is_successful

    data = transform_all(valid_df)

    # Use a fresh in-memory SQLite database
    engine = create_engine("sqlite:///:memory:")

    # First load
    counts_1 = load_warehouse(data, engine=engine)
    assert counts_1["fact_ad_performance_daily"] == 12497

    # Test referential integrity
    with engine.connect() as conn:
        # Check orphaned accounts
        orphans = conn.execute(text("""
            SELECT COUNT(*) FROM fact_ad_performance_daily f
            LEFT JOIN dim_account a ON f.account_id = a.account_id
            WHERE a.account_id IS NULL;
        """)).scalar()
        assert orphans == 0

        # Check orphaned campaigns
        orphans = conn.execute(text("""
            SELECT COUNT(*) FROM fact_ad_performance_daily f
            LEFT JOIN dim_campaign c ON f.campaign_id = c.campaign_id
            WHERE c.campaign_id IS NULL;
        """)).scalar()
        assert orphans == 0

        # Check orphaned ad groups
        orphans = conn.execute(text("""
            SELECT COUNT(*) FROM fact_ad_performance_daily f
            LEFT JOIN dim_ad_group ag ON f.ad_group_id = ag.ad_group_id
            WHERE ag.ad_group_id IS NULL;
        """)).scalar()
        assert orphans == 0

        # Check orphaned ads
        orphans = conn.execute(text("""
            SELECT COUNT(*) FROM fact_ad_performance_daily f
            LEFT JOIN dim_ad ad ON f.ad_id = ad.ad_id
            WHERE ad.ad_id IS NULL;
        """)).scalar()
        assert orphans == 0

    # Second load to test idempotency
    counts_2 = load_warehouse(data, engine=engine)
    with engine.connect() as conn:
        total_fact_rows = conn.execute(text("SELECT COUNT(*) FROM fact_ad_performance_daily;")).scalar()
        assert total_fact_rows == 12497, "Fact table should have exactly 12497 rows after idempotent reload."


def test_audit_pipeline_runs_logging():
    """Verifies that pipeline execution statistics and data quality metrics are persisted to audit table."""
    engine = create_engine("sqlite:///:memory:")
    report = ValidationReport(
        total_records=100,
        valid_records=95,
        rejected_records=5,
        anomalies_detected=2,
        warnings=["Test warning"],
        errors=[],
        metrics_summary={"total_spend": 250.0},
    )
    record_pipeline_audit(
        engine=engine,
        run_id="test_run_001",
        pipeline_name="bing_advertising_performance",
        report=report,
        duration_sec=0.45,
        status="WARNING",
    )

    with engine.connect() as conn:
        row = conn.execute(text("SELECT * FROM audit_pipeline_runs WHERE run_id = 'test_run_001';")).mappings().one()
        assert row["status"] == "WARNING"
        assert row["total_records"] == 100
        assert row["valid_records"] == 95
        assert row["rejected_records"] == 5
        assert row["anomalies_detected"] == 2
        assert "Test warning" in row["warnings"]
        assert row["execution_duration_sec"] == 0.45
