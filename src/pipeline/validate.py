"""
Data Quality & Validation Module
Applies structural, domain, and referential integrity validations to advertising performance data.
Emits structured validation reports suitable for logging and automated Insights Agents.
"""
from dataclasses import dataclass, field
import logging
from typing import Any
import pandas as pd

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = [
    "raw_date",
    "account_number",
    "campaign_name",
    "ad_group_id_raw",
    "ad_id_raw",
    "impressions",
    "clicks",
    "spend",
]


@dataclass
class ValidationReport:
    total_records: int = 0
    valid_records: int = 0
    rejected_records: int = 0
    anomalies_detected: int = 0
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    metrics_summary: dict[str, Any] = field(default_factory=dict)

    @property
    def is_successful(self) -> bool:
        return len(self.errors) == 0 and self.valid_records > 0


def validate_raw_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, ValidationReport]:
    """
    Performs comprehensive data quality checks on the extracted DataFrame:
    1. Schema & Column Presence
    2. Null checks on critical identifiers
    3. Non-negative metric validations
    4. Ad-tech domain sanity checks (e.g., clicks vs impressions, null position logic)
    5. Temporal sanity (parseable dates)
    """
    report = ValidationReport(total_records=len(df))

    if df.empty:
        report.errors.append("Validation failed: Input dataset is empty.")
        return df, report

    # 1. Column presence check
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        report.errors.append(f"Missing mandatory columns: {missing_cols}")
        return df, report

    # 2. Critical identifier null check
    id_null_mask = (
        df["account_number"].isna()
        | df["campaign_name"].isna()
        | df["ad_group_id_raw"].isna()
        | df["ad_id_raw"].isna()
    )
    id_null_count = int(id_null_mask.sum())
    if id_null_count > 0:
        report.warnings.append(f"Found {id_null_count} rows with missing core entity IDs. These rows will be dropped.")
        df = df[~id_null_mask].copy()

    # 3. Non-negative metric checks
    negative_mask = (
        (df["impressions"] < 0)
        | (df["clicks"] < 0)
        | (df["spend"] < 0)
    )
    negative_count = int(negative_mask.sum())
    if negative_count > 0:
        report.errors.append(f"Detected {negative_count} rows with negative values in impressions/clicks/spend.")
        df = df[~negative_mask].copy()

    # 4. Domain anomaly checks (Ad-tech specific)
    # Multiple clicks per impression or delayed click attribution
    multi_click_mask = df["clicks"] > df["impressions"]
    multi_click_count = int(multi_click_mask.sum())
    if multi_click_count > 0:
        report.anomalies_detected += multi_click_count
        report.warnings.append(
            f"Ad-Tech Anomaly: Detected {multi_click_count} rows where clicks > impressions "
            "(common in repeat-click actions or cross-day attribution latency). Retaining records for auditability."
        )

    # 5. Temporal validation
    try:
        parsed_dates = pd.to_datetime(df["raw_date"], format="%m/%d/%Y", errors="coerce")
        invalid_dates_count = int(parsed_dates.isna().sum())
        if invalid_dates_count > 0:
            report.warnings.append(f"Found {invalid_dates_count} unparseable dates. Filtering them out.")
            df = df[parsed_dates.notna()].copy()
    except Exception as exc:
        report.errors.append(f"Date validation failed with error: {exc}")

    # Summary calculations
    report.valid_records = len(df)
    report.rejected_records = report.total_records - report.valid_records
    report.metrics_summary = {
        "total_impressions": int(df["impressions"].sum()) if "impressions" in df.columns else 0,
        "total_clicks": int(df["clicks"].sum()) if "clicks" in df.columns else 0,
        "total_spend": float(round(df["spend"].sum(), 2)) if "spend" in df.columns else 0.0,
        "total_conversions": float(df["conversions"].sum()) if "conversions" in df.columns else 0.0,
    }

    logger.info(
        "Validation complete: %d valid records, %d rejected, %d warnings, %d errors",
        report.valid_records,
        report.rejected_records,
        len(report.warnings),
        len(report.errors),
    )
    return df, report
