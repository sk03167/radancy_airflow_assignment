# Advertising Performance ETL Pipeline & Data Warehouse

Author: **sk03167**

Production-grade, idempotent ETL pipeline orchestrating advertising performance data (`BING_MultiDays.csv`) into a Star Schema Data Warehouse using **Apache Airflow**, **Python**, **SQLAlchemy**, and **PostgreSQL**.

---

## 🚀 Setup & Execution Instructions

You can evaluate and run this pipeline under two scenarios:

### Scenario A: Running with Docker (Recommended / Full Airflow Orchestration)

This spins up the complete production architecture: **PostgreSQL 15 (Target Warehouse + Airflow DB)**, **Airflow Scheduler**, and **Airflow Webserver**.

#### 1. Start the stack:
```bash
docker compose up -d
```

#### 2. Access the Airflow Web UI:
- Open your browser to [http://localhost:8080](http://localhost:8080)
- **Username**: `airflow`
- **Password**: `airflow`

The DAG `bing_advertising_performance_pipeline` starts unpaused and will automatically execute. You can also trigger it manually from the UI or via CLI:
```bash
docker compose exec airflow-webserver airflow dags trigger bing_advertising_performance_pipeline
```

#### 3. View the Data in PostgreSQL:
```bash
# Query the Campaign Leaderboard View
docker compose exec postgres psql -U airflow -d airflow -c "SELECT * FROM vw_campaign_leaderboard LIMIT 5;"

# Query the Audit & Data Quality log
docker compose exec postgres psql -U airflow -d airflow -c "SELECT run_id, status, total_records, anomalies_detected, executed_at FROM audit_pipeline_runs;"
```

---

### Scenario B: Running without Docker (Local Python 3.11 / 3.12 Setup)

If you do not have Docker installed, you can run the entire pipeline end-to-end directly in Python in **under 1 second**. It automatically falls back to an embedded local SQLite warehouse (`data/warehouse.db`).

#### 1. Create and activate a virtual environment:
**On Windows (PowerShell):**
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

**On Linux / macOS:**
```bash
python3 -m venv .venv
source .venv/bin/activate
```

#### 2. Install dependencies:
```bash
pip install -r requirements.txt
```

#### 3. Run the pipeline:
```bash
python src/run_pipeline.py
```
*Output: Extracts raw CSV, runs data quality checks, transforms dimensional models, loads the warehouse, and records execution audit stats in ~1 second.*

#### 4. Run automated test suite:
```bash
pytest tests/ -v
```

#### 5. Inspect the local warehouse data:
The loaded tables and views are stored in `data/warehouse.db`. You can view them with any SQLite viewer (e.g., the VS Code **SQLite Viewer** extension, DBeaver, or via Python):
```bash
python -c "import sqlite3, pandas as pd; conn = sqlite3.connect('data/warehouse.db'); print(pd.read_sql('SELECT * FROM vw_campaign_leaderboard LIMIT 5', conn))"
```

---

## 🎯 Output Objects & How to View Them

The pipeline produces structured relational tables, analytical views, and audit logs loaded into the target warehouse (**PostgreSQL** in Docker or **SQLite** locally):

### 1. Catalog of Output Warehouse Objects

| Output Object | Type | Rows | Description |
|---|:---:|:---:|---|
| **`fact_ad_performance_daily`** | **Fact Table** | 12,497 | Granular daily ad performance containing core metrics (`impressions`, `clicks`, `spend`, `conversions`, `assists`), targeting context (`device_type`, `os`, `network`, `match_type`), and pre-calculated KPIs (`ctr`, `cpc`, `cpa`, `conversion_rate`). |
| **`dim_account`** | **Dimension** | 9 | Advertiser client metadata, customer IDs, status, and currency codes. |
| **`dim_campaign`** | **Dimension** | 54 | Campaign entities linked to accounts via deterministic surrogate keys. |
| **`dim_ad_group`** | **Dimension** | 234 | Ad group hierarchy with cleaned numeric identifiers. |
| **`dim_ad`** | **Dimension** | 484 | Creative ad copies, titles, distribution formats, and destination URLs. |
| **`dim_date`** | **Dimension** | 7 | Calendar attributes (`full_date`, `day`, `month`, `quarter`, `year`, `day_name`, `is_weekend`). |
| **`audit_pipeline_runs`** | **Audit Table** | 1+ | Observability log recording run durations, valid/rejected counts, anomaly counts, warnings, and reconciliation totals. |
| **`vw_campaign_leaderboard`** | **BI View** | — | Top campaigns ranked by spend, impressions, clicks, CTR, and CPA. |
| **`vw_ad_performance_summary`**| **BI View** | — | Multi-dimensional slice by date, account, campaign, device, and network for automated AI Insights Agents. |

---

### 2. How to View Output Objects in PostgreSQL (Docker)

Run these queries directly in your terminal to inspect the warehouse:

```bash
# 1. View Campaign Leaderboard (BI View)
docker compose exec postgres psql -U airflow -d airflow -c "SELECT campaign_name, account_name, spend, impressions, clicks, ctr FROM vw_campaign_leaderboard LIMIT 5;"

# 2. View Fact Table with Pre-calculated KPIs
docker compose exec postgres psql -U airflow -d airflow -c "SELECT performance_id, date_id, clicks, spend, ctr, cpc, conversion_rate FROM fact_ad_performance_daily WHERE clicks > 0 LIMIT 5;"

# 3. View Pipeline Audit & Data Quality Run Stats
docker compose exec postgres psql -U airflow -d airflow -c "SELECT run_id, status, total_records, valid_records, anomalies_detected, metrics_summary FROM audit_pipeline_runs;"

# 4. View Dimension Tables (e.g. Accounts & Campaigns)
docker compose exec postgres psql -U airflow -d airflow -c "SELECT account_id, account_name, account_status, currency_code FROM dim_account;"

# 5. Open Interactive SQL Shell
docker compose exec -it postgres psql -U airflow -d airflow
# (Type '\dt' to list tables, '\dv' to list views, '\q' to exit)
```

#### Connecting via Desktop GUI (DBeaver, pgAdmin, DataGrip, VS Code SQLTools):
- **Host**: `localhost`
- **Port**: `5432`
- **Database**: `airflow`
- **Username**: `airflow`
- **Password**: `airflow`

---

### 3. How to View Output Objects in SQLite (Local Runner)

When running outside Docker (`python src/run_pipeline.py`), tables are loaded into `data/warehouse.db`:

#### Option A: Quick Python one-liners
```bash
# View Campaign Leaderboard View
python -c "import sqlite3, pandas as pd; conn = sqlite3.connect('data/warehouse.db'); print(pd.read_sql('SELECT * FROM vw_campaign_leaderboard LIMIT 5', conn).to_string(index=False))"

# View Audit Log Record
python -c "import sqlite3, pandas as pd; conn = sqlite3.connect('data/warehouse.db'); print(pd.read_sql('SELECT run_id, status, total_records, anomalies_detected, execution_duration_sec FROM audit_pipeline_runs', conn).to_string(index=False))"
```

#### Option B: VS Code GUI Extension
1. Install the **SQLite Viewer** extension in VS Code.
2. Click on `data/warehouse.db` in the VS Code file explorer to interactively sort, filter, and inspect all tables and views visually.

---

### 4. How to View Output Stats in the Airflow UI

1. Open **[http://localhost:8080](http://localhost:8080)** (login: `airflow` / `airflow`).
2. Click on **`bing_advertising_performance_pipeline`**.
3. **Graph / Grid View**: View real-time green checkmarks for all 5 operational tasks.
4. **XCom Tab** (`Admin` $\to$ `XComs`):
   - View `raw_row_count` (`12497`)
   - View `valid_count` (`12497`)
   - View `validation_stats` (JSON dictionary with anomaly flags and financial metric summaries)
5. **Task Logs**: Click on `verify_warehouse_and_views` $\to$ **Log** to see the formatted leaderboard printed during verification.

---

## 🏗️ Architecture & Pipeline Flow

The pipeline follows a modular **Extract-Validate-Transform-Load (E-V-T-L)** architecture:

```mermaid
flowchart LR
    A[Raw CSV Ingestion<br/>BING_MultiDays.csv] --> B[extract_and_stage_raw<br/>Schema standardizing]
    B --> C[validate_data_quality<br/>Domain sanity & null checks]
    C --> D[transform_dimensional_models<br/>Star Schema & KPI calculation]
    D --> E[load_data_warehouse<br/>Idempotent bulk upsert]
    E --> F[verify_warehouse_and_views<br/>Integrity check & audit logging]
```

---

## 📊 Data Modeling & Star Schema

The warehouse schema is designed around a **Star Schema** optimized for high-performance aggregation, downstream BI tools, and automated **AI Insights Agents** (Text-to-SQL and anomaly detectors).

```
                        ┌──────────────────┐
                        │   dim_account    │
                        ├──────────────────┤
                        │ *account_id (PK) │
                        │  customer_id     │
                        │  account_number  │
                        │  account_name    │
                        │  account_status  │
                        │  currency_code   │
                        └────────┬─────────┘
                                 │
┌──────────────────┐             │             ┌──────────────────┐
│   dim_campaign   │             │             │   dim_ad_group   │
├──────────────────┤             │             ├──────────────────┤
│ *campaign_id(PK) │             │             │ *ad_group_id(PK) │
│  account_id (FK) │             │             │  campaign_id(FK) │
│  campaign_name   │             │             │  ad_group_name   │
│  campaign_status │             │             │  ad_group_status │
└────────┬─────────┘             │             └────────┬─────────┘
         │                       │                      │
         │          ┌────────────┴─────────────┐        │
         └─────────►│fact_ad_performance_daily │◄───────┘
                    ├──────────────────────────┤
                    │ *performance_id (PK Hash)│
                    │  date_id (FK)            │
                    │  account_id (FK)         │
                    │  campaign_id (FK)        │
                    │  ad_group_id (FK)        │
                    │  ad_id (FK)              │
                    │ ──────────────────────── │
                    │  device_type / OS        │
                    │  network / language      │
                    │  match types / placement │
                    │ ──────────────────────── │
                    │  impressions             │
                    │  clicks                  │
                    │  spend                   │
                    │  avg_position            │
                    │  conversions             │
                    │  assists                 │
                    │ ──────────────────────── │
                    │  ctr, cpc, cpa, conv_rate│
                    └────────────┬─────────────┘
                                 │
┌──────────────────┐             │             ┌──────────────────┐
│      dim_ad      │◄────────────┴────────────►│     dim_date     │
├──────────────────┤                           ├──────────────────┤
│ *ad_id (PK)      │                           │ *date_id (PK)    │
│  ad_group_id(FK) │                           │  full_date       │
│  ad_title        │                           │  day, month, year│
│  ad_description  │                           │  quarter         │
│  ad_type         │                           │  day_name        │
│  urls & tracking │                           │  is_weekend      │
└──────────────────┘                           └──────────────────┘
```

### Strategic Modeling Choices:
1. **Surrogate & Clean Keys**:
   - `ad_id` and `ad_group_id`: Stripped raw bracket formatting (e.g., `[6772783690]` $\to$ `6772783690`) into 64-bit BigInts for query performance.
   - `campaign_id`: Generated deterministic composite MD5 hash from `account_id + campaign_name` to prevent collisions across different advertiser accounts.
   - `performance_id`: Deterministic hash of the fact grain `(date, ad_id, device, network, match_type)` ensuring 100% idempotent warehouse loads.
2. **Degenerate Dimensions on Fact**:
   - Targeting attributes (`device_type`, `device_os`, `network`, `top_vs_other`, `delivered_match_type`) are stored directly on the fact table to eliminate expensive joins during multi-dimensional ad slicing.
3. **Automated Insights Agent Enablement**:
   - Pre-computed KPIs (`ctr`, `cpc`, `cpa`, `conversion_rate`) are derived with zero-division protection at load time. This prevents Text-to-SQL or AI agents from generating inaccurate divide-by-zero queries.
   - Includes prepared analytical views (`vw_campaign_leaderboard`, `vw_ad_performance_summary`) for instant consumption by BI dashboards and automated anomaly agents.
4. **Audit & Data Quality Tracking (`audit_pipeline_runs`)**:
   - Every pipeline run records its execution duration, valid records, dropped records, anomaly counts, and metric totals (`total_spend`, `total_clicks`, `total_impressions`) into `audit_pipeline_runs`.

---

## 🛠️ Project Structure

```text
advertising_airflow_pipeline/
├── .github/
│   └── workflows/
│       └── ci.yml                 # GitHub Actions automated test & CI pipeline
├── dags/
│   └── bing_ads_etl_dag.py        # Airflow DAG definition & task orchestration
├── src/
│   ├── config.py                  # Environment settings & file paths
│   ├── run_pipeline.py            # Standalone pipeline CLI runner (<1s execution)
│   ├── db/
│   │   ├── connection.py          # SQLAlchemy engine & session management
│   │   └── models.py              # Star schema dimensional ORM models & DDL
│   └── pipeline/
│       ├── extract.py             # CSV ingestion & column standardization
│       ├── validate.py            # Data quality checks & anomaly detection
│       ├── transform.py           # Star Schema transformations & KPI logic
│       └── load.py                # Idempotent warehouse loading & view creation
├── tests/
│   └── test_pipeline.py           # Automated pytest test suite
├── data/
│   └── input/
│       └── BING_MultiDays.csv     # Raw advertising performance dataset
├── docker-compose.yml             # Full Airflow + PostgreSQL stack
├── Dockerfile                     # Custom Airflow image with Python requirements
├── requirements.txt               # Pinned Python dependencies
└── README.md                      # Project documentation
```

---

## 🧪 Testing Methodology & Expectations

To ensure both rapid developer velocity and production safety on PostgreSQL, the project implements a **two-tier testing strategy**:

```
┌────────────────────────────────────────────────────────────────────────┐
│  Tier 1: Fast Unit & In-Memory Component Tests (pytest)                │
│  • Engine: In-Memory SQLite (sqlite:///:memory:)                       │
│  • Execution Speed: ~1.8 seconds                                       │
│  • Scope: Schema contracts, math safety, data validation, idempotency  │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
┌───────────────────────────────────▼────────────────────────────────────┐
│  Tier 2: Containerized Data Warehouse Audit (verify_warehouse_and_views)│
│  • Engine: Production PostgreSQL 15 in Docker                          │
│  • Scope: Live Postgres DDL, zero-orphan foreign keys, view queries     │
└────────────────────────────────────────────────────────────────────────┘
```

### Tier 1: Unit & Component Test Suite (`pytest tests/ -v`)

Run the automated test suite locally or in CI:

```bash
pytest tests/ -v
```

#### Test Expectations & Coverage:
1. **`test_clean_id`**: Verifies that brackets and dirty characters (e.g., `[2709719083]` $\to$ `2709719083`) are stripped and cast to clean 64-bit BigInts.
2. **`test_generate_surrogate_key`**: Verifies that surrogate key generation is deterministic, case-insensitive, and collision-resistant across composite inputs.
3. **`test_validation_flags_negative_metrics`**: Verifies that records with negative impressions, clicks, or spend are caught and flagged.
4. **`test_validation_detects_click_anomalies`**: Verifies that ad-tech attribution latency anomalies (`clicks > impressions`) are flagged in the validation audit without dropping financial data.
5. **`test_kpi_calculations`**: Verifies that derived advertising KPIs (`CTR`, `CPC`, `CPA`, `conversion_rate`) compute accurately and guard against zero-division errors (`NaN`/`Inf`).
6. **`test_referential_integrity_and_idempotency`**: Integration test using all **12,497 real rows** from `BING_MultiDays.csv`. Proves that:
   - Every fact row foreign key resolves to a valid record in `dim_date`, `dim_account`, `dim_campaign`, `dim_ad_group`, and `dim_ad` (**zero orphaned rows**).
   - Re-running the warehouse load does not duplicate records or cause primary key violations (**100% idempotent**).
7. **`test_audit_pipeline_runs_logging`**: Verifies that run metrics, durations, and JSON summaries are correctly persisted and queryable in `audit_pipeline_runs`.

```text
tests/test_pipeline.py::test_clean_id PASSED                             [ 14%]
tests/test_pipeline.py::test_generate_surrogate_key PASSED               [ 28%]
tests/test_pipeline.py::test_validation_flags_negative_metrics PASSED    [ 42%]
tests/test_pipeline.py::test_validation_detects_click_anomalies PASSED   [ 57%]
tests/test_pipeline.py::test_kpi_calculations PASSED                     [ 71%]
tests/test_pipeline.py::test_referential_integrity_and_idempotency PASSED [ 85%]
tests/test_pipeline.py::test_audit_pipeline_runs_logging PASSED          [100%]
============================== 7 passed in 1.79s ==============================
```

---

### Tier 2: Real Data Warehouse Audit (In Airflow on PostgreSQL 15)

While SQLite catches 90% of logic errors instantly, PostgreSQL enforces strict types, foreign keys, and dialect-specific view syntax.

As the final task in the Airflow DAG ([`verify_warehouse_and_views`](dags/bing_ads_etl_dag.py)), the pipeline automatically runs an **in-engine verification directly against PostgreSQL**:
1. **Row Count Audit**: Asserts `fact_ad_performance_daily` has all 12,497 records.
2. **Referential Integrity Check**:
   ```sql
   SELECT COUNT(*) FROM fact_ad_performance_daily f
   LEFT JOIN dim_account a ON f.account_id = a.account_id
   WHERE a.account_id IS NULL;
   ```
   Fails the pipeline immediately if any orphaned records are found.
3. **Analytical View Query**: Executes `SELECT * FROM vw_campaign_leaderboard LIMIT 5` to confirm PostgreSQL view compilation and query performance.

---

### Continuous Integration (CI) via GitHub Actions
A GitHub Actions workflow is included at [`.github/workflows/ci.yml`](.github/workflows/ci.yml). On every `push` and `pull_request` to `main`:
1. Matrix builds across **Python 3.11** and **Python 3.12**.
2. Compiles and syntax-checks all source, DAG, and test files.
3. Runs the automated `pytest` test suite (including schema validation, KPI formulas, domain anomaly handling, and in-memory referential integrity checks).

---

## 🔍 Edge Cases & Domain Anomalies Handled

1. **Ad-Tech Attribution Latency / Repeat Clicks**:
   - The validation engine detected **11 rows where `clicks > impressions`** in the dataset.
   - In digital advertising, this occurs when users click an ad multiple times from a single impression or when cross-day attribution cutoffs delay impression logging.
   - Rather than dropping valid spend data, the pipeline flags the anomaly in the audit report while preserving financial integrity.
2. **Missing Positions on Zero-Impression Ads**:
   - Handled `avg_position = NaN` gracefully when impressions equal zero.
3. **Idempotent Ingestion**:
   - The warehouse loader employs `ON CONFLICT DO NOTHING` / deterministic hashing. Re-running the DAG or script multiple times will never duplicate rows or cause primary key violations.
