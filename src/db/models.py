"""
Database Models for Advertising Data Warehouse
Defines the Star Schema dimensional tables and fact tables using SQLAlchemy.
"""
from datetime import datetime, timezone
from sqlalchemy import (
    Column,
    Integer,
    SmallInteger,
    BigInteger,
    Float,
    String,
    Text,
    Date,
    DateTime,
    Boolean,
    ForeignKey,
    Index,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class StagingBingRaw(Base):
    """
    Staging table for raw ingested data directly from BING_MultiDays.csv.
    Allows auditing raw records and data lineage before dimensional transformation.
    """
    __tablename__ = "stg_bing_performance"

    stage_id = Column(BigInteger, primary_key=True, autoincrement=True)
    raw_date = Column(String(50))
    customer = Column(String(100))
    account_number = Column(String(100))
    account_name = Column(String(255))
    account_status = Column(String(50))
    campaign_name = Column(String(255))
    campaign_status = Column(String(50))
    ad_group_id_raw = Column(String(100))
    ad_group = Column(String(255))
    ad_group_status = Column(String(50))
    ad_id_raw = Column(String(100))
    ad_description = Column(Text)
    ad_distribution = Column(String(100))
    ad_status = Column(String(50))
    ad_title = Column(String(500))
    ad_type = Column(String(100))
    tracking_template = Column(Text)
    custom_parameters = Column(Text)
    final_mobile_url = Column(Text)
    final_url = Column(Text)
    top_vs_other = Column(String(100))
    display_url = Column(String(500))
    final_app_url = Column(Text)
    destination_url = Column(Text)
    device_type = Column(String(100))
    device_os = Column(String(100))
    delivered_match_type = Column(String(50))
    bid_match_type = Column(String(50))
    language = Column(String(50))
    network = Column(String(100))
    currency_code = Column(String(20))
    impressions = Column(BigInteger)
    clicks = Column(BigInteger)
    spend = Column(Float)
    avg_position = Column(Float)
    conversions = Column(Float)
    assists = Column(Float)
    ingested_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class DimDate(Base):
    """
    Date Dimension table for temporal analytics, slicing by day/month/quarter/weekend.
    """
    __tablename__ = "dim_date"

    date_id = Column(Integer, primary_key=True)  # Format: YYYYMMDD
    full_date = Column(Date, unique=True, nullable=False, index=True)
    day = Column(SmallInteger, nullable=False)
    month = Column(SmallInteger, nullable=False)
    month_name = Column(String(20), nullable=False)
    quarter = Column(SmallInteger, nullable=False)
    year = Column(SmallInteger, nullable=False)
    day_of_week = Column(SmallInteger, nullable=False)
    day_name = Column(String(20), nullable=False)
    is_weekend = Column(Boolean, nullable=False)


class DimAccount(Base):
    """
    Account Dimension containing advertiser account and customer attributes.
    """
    __tablename__ = "dim_account"

    account_id = Column(String(64), primary_key=True)  # Cleaned surrogate key
    customer_id = Column(String(64), nullable=True, index=True)
    account_number = Column(String(64), unique=True, nullable=False, index=True)
    account_name = Column(String(255), nullable=False)
    account_status = Column(String(50), nullable=False)
    currency_code = Column(String(10), default="USD")
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    campaigns = relationship("DimCampaign", back_populates="account")


class DimCampaign(Base):
    """
    Campaign Dimension representing marketing campaigns under an account.
    """
    __tablename__ = "dim_campaign"

    campaign_id = Column(String(64), primary_key=True)  # Surrogate hash: account_id + campaign_name
    account_id = Column(String(64), ForeignKey("dim_account.account_id"), nullable=False, index=True)
    campaign_name = Column(String(255), nullable=False, index=True)
    campaign_status = Column(String(50), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    account = relationship("DimAccount", back_populates="campaigns")
    ad_groups = relationship("DimAdGroup", back_populates="campaign")


class DimAdGroup(Base):
    """
    Ad Group Dimension representing ad group entities within a campaign.
    """
    __tablename__ = "dim_ad_group"

    ad_group_id = Column(BigInteger, primary_key=True)  # Cleaned from [12345]
    campaign_id = Column(String(64), ForeignKey("dim_campaign.campaign_id"), nullable=False, index=True)
    ad_group_name = Column(String(255), nullable=False)
    ad_group_status = Column(String(50), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    campaign = relationship("DimCampaign", back_populates="ad_groups")
    ads = relationship("DimAd", back_populates="ad_group")


class DimAd(Base):
    """
    Ad Dimension representing ad copy, titles, creative properties, and destination URLs.
    """
    __tablename__ = "dim_ad"

    ad_id = Column(BigInteger, primary_key=True)  # Cleaned from [67890]
    ad_group_id = Column(BigInteger, ForeignKey("dim_ad_group.ad_group_id"), nullable=False, index=True)
    ad_title = Column(String(500), nullable=True)
    ad_description = Column(Text, nullable=True)
    ad_type = Column(String(100), nullable=True)
    ad_distribution = Column(String(100), nullable=True)
    ad_status = Column(String(50), nullable=True)
    display_url = Column(String(500), nullable=True)
    final_url = Column(Text, nullable=True)
    destination_url = Column(Text, nullable=True)
    tracking_template = Column(Text, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    ad_group = relationship("DimAdGroup", back_populates="ads")


class FactAdPerformanceDaily(Base):
    """
    Daily Ad Performance Fact table.
    Granularity: Date + Ad + Targeting Attributes (Device OS, Match Type, Network, Position slice).
    Stores core ad metrics and pre-computed KPIs for BI dashboards and automated AI Insights Agents.
    """
    __tablename__ = "fact_ad_performance_daily"

    performance_id = Column(String(64), primary_key=True)  # Deterministic hash of grain for idempotency
    date_id = Column(Integer, ForeignKey("dim_date.date_id"), nullable=False, index=True)
    account_id = Column(String(64), ForeignKey("dim_account.account_id"), nullable=False, index=True)
    campaign_id = Column(String(64), ForeignKey("dim_campaign.campaign_id"), nullable=False, index=True)
    ad_group_id = Column(BigInteger, ForeignKey("dim_ad_group.ad_group_id"), nullable=False, index=True)
    ad_id = Column(BigInteger, ForeignKey("dim_ad.ad_id"), nullable=False, index=True)

    # Context / Targeting Attributes (Degenerate dimensions)
    device_type = Column(String(50), nullable=True)
    device_os = Column(String(50), nullable=True)
    network = Column(String(100), nullable=True)
    top_vs_other = Column(String(100), nullable=True)
    delivered_match_type = Column(String(50), nullable=True)
    bid_match_type = Column(String(50), nullable=True)
    language = Column(String(50), nullable=True)

    # Quantitative Metrics
    impressions = Column(BigInteger, nullable=False, default=0)
    clicks = Column(BigInteger, nullable=False, default=0)
    spend = Column(Float, nullable=False, default=0.0)
    avg_position = Column(Float, nullable=True)
    conversions = Column(Float, nullable=False, default=0.0)
    assists = Column(Float, nullable=False, default=0.0)

    # Derived KPIs (Pre-calculated for downstream analytics & Insights Agents)
    ctr = Column(Float, nullable=True)             # Click-Through Rate: clicks / impressions
    cpc = Column(Float, nullable=True)             # Cost Per Click: spend / clicks
    cpa = Column(Float, nullable=True)             # Cost Per Acquisition: spend / conversions
    conversion_rate = Column(Float, nullable=True) # Conversions / clicks

    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index("idx_fact_date_campaign", "date_id", "campaign_id"),
        Index("idx_fact_account_date", "account_id", "date_id"),
    )
