"""
Transformation Module
Transforms staged advertising performance data into dimensional Star Schema entities:
- dim_date
- dim_account
- dim_campaign
- dim_ad_group
- dim_ad
- fact_ad_performance_daily (with calculated KPIs: CTR, CPC, CPA, Conversion Rate)
"""
import hashlib
import logging
import re
from typing import NamedTuple
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class TransformedData(NamedTuple):
    dim_date: pd.DataFrame
    dim_account: pd.DataFrame
    dim_campaign: pd.DataFrame
    dim_ad_group: pd.DataFrame
    dim_ad: pd.DataFrame
    fact_performance: pd.DataFrame


def clean_id(value: str | int | float | None) -> int:
    """
    Cleans raw IDs containing square brackets or non-digit chars (e.g. '[2709719083]' -> 2709719083).
    """
    if pd.isna(value):
        raise ValueError("Cannot clean empty/null ID")
    digits = re.findall(r"\d+", str(value))
    if not digits:
        raise ValueError(f"No numeric digits found in ID string: {value}")
    return int("".join(digits))


def generate_surrogate_key(*args: str) -> str:
    """
    Generates a deterministic 16-character MD5 hash surrogate key from composite inputs.
    """
    composite = "::".join(str(arg).strip().lower() for arg in args)
    return hashlib.md5(composite.encode("utf-8")).hexdigest()[:16]


def build_date_dimension(df: pd.DataFrame) -> pd.DataFrame:
    """
    Constructs the Date dimension with rich calendar attributes for time-series analysis.
    """
    unique_dates = pd.to_datetime(df["raw_date"].unique(), format="%m/%d/%Y")
    records = []
    for dt in unique_dates:
        records.append({
            "date_id": int(dt.strftime("%Y%m%d")),
            "full_date": dt.date(),
            "day": dt.day,
            "month": dt.month,
            "month_name": dt.strftime("%B"),
            "quarter": dt.quarter,
            "year": dt.year,
            "day_of_week": dt.weekday(),
            "day_name": dt.strftime("%A"),
            "is_weekend": dt.weekday() in (5, 6),
        })
    dim_date_df = pd.DataFrame(records).drop_duplicates(subset=["date_id"]).sort_values("date_id")
    return dim_date_df


def build_account_dimension(df: pd.DataFrame) -> pd.DataFrame:
    """
    Constructs the Account dimension representing client advertiser accounts.
    """
    acc_df = df[[
        "account_number",
        "customer",
        "account_name",
        "account_status",
        "currency_code"
    ]].copy().drop_duplicates(subset=["account_number"])

    acc_df["account_id"] = acc_df["account_number"].astype(str).str.strip()
    acc_df["customer_id"] = acc_df["customer"].astype(str).str.strip()
    acc_df["currency_code"] = acc_df["currency_code"].fillna("USD")

    return acc_df[[
        "account_id",
        "customer_id",
        "account_number",
        "account_name",
        "account_status",
        "currency_code"
    ]]


def build_campaign_dimension(df: pd.DataFrame) -> pd.DataFrame:
    """
    Constructs the Campaign dimension with surrogate keys linking to accounts.
    """
    camp_df = df[["account_number", "campaign_name", "campaign_status"]].copy().drop_duplicates()
    camp_df["account_id"] = camp_df["account_number"].astype(str).str.strip()
    camp_df["campaign_id"] = camp_df.apply(
        lambda r: generate_surrogate_key(r["account_id"], r["campaign_name"]), axis=1
    )

    return camp_df[[
        "campaign_id",
        "account_id",
        "campaign_name",
        "campaign_status"
    ]].drop_duplicates(subset=["campaign_id"])


def build_ad_group_dimension(df: pd.DataFrame) -> pd.DataFrame:
    """
    Constructs the Ad Group dimension with clean bigint IDs and links to campaigns.
    """
    ag_df = df[[
        "account_number",
        "campaign_name",
        "ad_group_id_raw",
        "ad_group",
        "ad_group_status"
    ]].copy().drop_duplicates(subset=["ad_group_id_raw"])

    ag_df["ad_group_id"] = ag_df["ad_group_id_raw"].apply(clean_id)
    ag_df["campaign_id"] = ag_df.apply(
        lambda r: generate_surrogate_key(r["account_number"], r["campaign_name"]), axis=1
    )
    ag_df["ad_group_name"] = ag_df["ad_group"]

    return ag_df[[
        "ad_group_id",
        "campaign_id",
        "ad_group_name",
        "ad_group_status"
    ]].drop_duplicates(subset=["ad_group_id"])


def build_ad_dimension(df: pd.DataFrame) -> pd.DataFrame:
    """
    Constructs the Ad dimension containing creative copy, formats, and URLs.
    """
    ad_cols = [
        "ad_id_raw",
        "ad_group_id_raw",
        "ad_title",
        "ad_description",
        "ad_type",
        "ad_distribution",
        "ad_status",
        "display_url",
        "final_url",
        "destination_url",
        "tracking_template"
    ]
    ad_df = df[ad_cols].copy().drop_duplicates(subset=["ad_id_raw"])

    ad_df["ad_id"] = ad_df["ad_id_raw"].apply(clean_id)
    ad_df["ad_group_id"] = ad_df["ad_group_id_raw"].apply(clean_id)

    return ad_df[[
        "ad_id",
        "ad_group_id",
        "ad_title",
        "ad_description",
        "ad_type",
        "ad_distribution",
        "ad_status",
        "display_url",
        "final_url",
        "destination_url",
        "tracking_template"
    ]].drop_duplicates(subset=["ad_id"])


def build_fact_performance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Constructs the daily ad performance fact table.
    Enriches with foreign keys, targeting dimensions, and derived business KPIs.
    """
    fact = df.copy()

    # Foreign Keys
    parsed_dates = pd.to_datetime(fact["raw_date"], format="%m/%d/%Y")
    fact["date_id"] = parsed_dates.dt.strftime("%Y%m%d").astype(int)
    fact["account_id"] = fact["account_number"].astype(str).str.strip()
    fact["campaign_id"] = fact.apply(
        lambda r: generate_surrogate_key(r["account_id"], r["campaign_name"]), axis=1
    )
    fact["ad_group_id"] = fact["ad_group_id_raw"].apply(clean_id)
    fact["ad_id"] = fact["ad_id_raw"].apply(clean_id)

    # Clean Metrics
    fact["impressions"] = fact["impressions"].fillna(0).astype(int)
    fact["clicks"] = fact["clicks"].fillna(0).astype(int)
    fact["spend"] = fact["spend"].fillna(0.0).astype(float)
    fact["avg_position"] = fact["avg_position"].astype(float)
    fact["conversions"] = fact["conversions"].fillna(0.0).astype(float)
    fact["assists"] = fact["assists"].fillna(0.0).astype(float)

    # Derived KPIs (Safe division to avoid ZeroDivisionError or Inf)
    # CTR = Clicks / Impressions
    fact["ctr"] = np.where(fact["impressions"] > 0, (fact["clicks"] / fact["impressions"]).round(4), 0.0)
    # CPC = Spend / Clicks
    fact["cpc"] = np.where(fact["clicks"] > 0, (fact["spend"] / fact["clicks"]).round(4), 0.0)
    # CPA = Spend / Conversions (None if no conversions)
    fact["cpa"] = np.where(fact["conversions"] > 0, (fact["spend"] / fact["conversions"]).round(4), np.nan)
    # Conversion Rate = Conversions / Clicks
    fact["conversion_rate"] = np.where(fact["clicks"] > 0, (fact["conversions"] / fact["clicks"]).round(4), 0.0)

    # Deterministic Performance ID
    fact["performance_id"] = fact.apply(
        lambda r: generate_surrogate_key(
            r["date_id"],
            r["ad_id"],
            r.get("device_type", ""),
            r.get("device_os", ""),
            r.get("network", ""),
            r.get("delivered_match_type", ""),
            r.get("bid_match_type", ""),
            r.get("top_vs_other", ""),
        ),
        axis=1
    )

    columns = [
        "performance_id",
        "date_id",
        "account_id",
        "campaign_id",
        "ad_group_id",
        "ad_id",
        "device_type",
        "device_os",
        "network",
        "top_vs_other",
        "delivered_match_type",
        "bid_match_type",
        "language",
        "impressions",
        "clicks",
        "spend",
        "avg_position",
        "conversions",
        "assists",
        "ctr",
        "cpc",
        "cpa",
        "conversion_rate",
    ]

    return fact[columns]


def transform_all(df: pd.DataFrame) -> TransformedData:
    """
    Executes end-to-end transformation across all dimensional entities and fact records.
    """
    logger.info("Transforming dimensions...")
    dim_date = build_date_dimension(df)
    dim_account = build_account_dimension(df)
    dim_campaign = build_campaign_dimension(df)
    dim_ad_group = build_ad_group_dimension(df)
    dim_ad = build_ad_dimension(df)

    logger.info("Transforming fact table with derived KPIs...")
    fact_perf = build_fact_performance(df)

    logger.info(
        "Transform summary: %d dates, %d accounts, %d campaigns, %d ad groups, %d ads, %d fact rows",
        len(dim_date),
        len(dim_account),
        len(dim_campaign),
        len(dim_ad_group),
        len(dim_ad),
        len(fact_perf),
    )

    return TransformedData(
        dim_date=dim_date,
        dim_account=dim_account,
        dim_campaign=dim_campaign,
        dim_ad_group=dim_ad_group,
        dim_ad=dim_ad,
        fact_performance=fact_perf,
    )
