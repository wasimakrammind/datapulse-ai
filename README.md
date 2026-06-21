# DataPulse AI

A production-style data platform that ingests messy operational data, runs it through a 3-layer ETL pipeline (raw, staging, curated), validates quality with 15+ automated checks, exposes curated data through REST APIs, visualizes pipeline health on interactive dashboards, and uses GenAI to explain pipeline failures with actionable root-cause reports.

Built to mirror real-world data engineering workflows at companies like Amazon, Microsoft, Google, and NVIDIA.


## Live Demo

* **Dashboard**: [https://datapulse-ai.streamlit.app](https://datapulse-ai.streamlit.app)
* **API Docs (Swagger)**: [https://datapulse-ai-api.onrender.com/docs](https://datapulse-ai-api.onrender.com/docs)


## What This Project Does

* Generates realistic dirty datasets (orders, customers, payments, inventory, clickstream, subscriptions, API logs) with intentional nulls, duplicates, negative values, and orphan records
* Processes data through a 3-layer ETL pipeline with batch tracking, audit logging, and failed-record quarantine
* Runs 26 automated data quality checks covering schema validation, referential integrity, freshness, anomaly detection, and more
* Exposes all curated data and pipeline metrics through a FastAPI REST API with Swagger docs, pagination, API key auth, and correlation IDs
* Renders a 5-tab Streamlit dashboard showing pipeline health, DQ results, runtime trends, SLA breaches, and business KPIs
* Produces detailed root-cause analysis reports (LLM-powered or rule-based fallback) that explain what failed, why, and who should fix it


## Architecture

```
Raw CSV Files
    |
    v
ETL Pipeline (Python + SQLite)
    |--- Raw Layer (7 source tables)
    |--- Staging Layer (cleaned, deduped, quarantined bad records)
    |--- Curated Layer (fact/dim star schema)
    |
    +---> Audit Log (every operation tracked with batch ID)
    +---> Quarantine Table (rejected records with failure reasons)
    |
    v
Data Quality Engine (15+ rule types)
    |
    +---> DQ Results Table
    +---> GenAI Root-Cause Agent
    |
    v
FastAPI REST API (10+ endpoints)
    |
    v
Streamlit Dashboard (5 tabs, Plotly charts)
```

### Data Flow

```
CSV Sources --> Raw Tables --> Staging (Cleaned) --> Curated (Fact/Dim)
                    |                  |                      |
                    v                  v                      v
              Audit Log         Quarantine Table        REST API
                                                            |
              DQ Engine ----> DQ Results Table          Dashboard
                    |                                       |
                    v                                       v
              GenAI Agent --------------------------> Root-Cause Report
```


## Tech Stack

* **ETL Pipeline**: Python, Pandas, SQL
* **Database**: SQLite (zero-config, portable; swap to PostgreSQL/Redshift for production)
* **Data Quality**: Custom DQ engine with 15+ rule types
* **REST API**: FastAPI, OpenAPI/Swagger, Pydantic
* **Dashboard**: Streamlit, Plotly
* **GenAI**: LangChain + OpenAI (with rule-based fallback when no API key is set)
* **Testing**: Pytest (42 tests), Postman/Newman collection
* **CI/CD**: GitHub Actions
* **Containerization**: Docker, docker-compose
* **Logging**: structlog (structured JSON)


## Features

### ETL Pipeline (Raw to Staging to Curated)

* 7 source datasets: Orders, Customers, Payments, Inventory, Clickstream, Subscriptions, API Logs
* 3-layer architecture with batch ID tracking on every record
* Source-to-target row count reconciliation after each stage
* Failed records quarantined with specific failure reasons
* Full audit trail logging every insert, stage, and load operation
* Idempotent loads using INSERT OR REPLACE logic

### Data Quality Framework (15+ Rule Types)

* Schema validation (verify expected columns exist)
* Null and empty value checks on critical columns
* Duplicate detection on key fields
* Primary key validation (combined null + duplicate check)
* Referential integrity across parent-child tables
* Negative value detection for quantities and amounts
* Status/value validation against allowed lists
* Data freshness checks against SLA thresholds
* Source-to-target row count reconciliation
* Aggregate reconciliation (sum validation across layers)
* Volume anomaly detection (flags unexpected batch size changes)

### Pipeline Observability

* Every pipeline run tracked with correlation ID, runtime, input/output/rejected counts
* SLA breach detection (configurable threshold, default 300 seconds)
* Full audit trail for every database operation
* Quarantine table with failure reasons and rule names

### REST API (FastAPI)

Endpoints:

* `GET /health` - Health check with database connectivity status
* `GET /pipeline-runs` - Paginated pipeline run history with status filter
* `GET /data-quality-results` - DQ check results with severity/passed/table filters
* `GET /orders/summary` - Business KPIs broken down by channel and region
* `GET /inventory/anomalies` - Stock anomalies (negative stock, below reorder, missing region)
* `GET /reports/execution-misses` - Failed runs and SLA breaches
* `GET /quarantine/records` - Quarantined records with source table filter
* `GET /audit-log` - Full audit trail
* `GET /genai/analyze` - On-demand root-cause analysis for any pipeline run
* `GET /genai/latest-analysis` - Root-cause analysis for the most recent run

All endpoints include OpenAPI/Swagger documentation, pagination, API key authentication, request correlation IDs, and structured error handling.

### GenAI Root-Cause Assistant

Reads failed pipeline logs and DQ results, then produces a report covering:

* What specifically failed
* Which data source and column caused the issue
* Which quality rule or pipeline step triggered the failure
* Most likely root cause
* Concrete suggested fix
* Business impact on downstream processes
* Recommended owner team (Data Engineering, Source Team, Platform Team, Data Ops)

Works in two modes: LLM-powered (OpenAI/LangChain) when an API key is configured, or rule-based fallback that still produces useful analysis without any external dependencies.

### Streamlit Dashboard

5 interactive tabs:

* Pipeline Runs: success/failure pie chart, full run history table
* Data Quality: results by severity, top failed rules bar chart, filterable results table
* Runtime Trends: line chart with SLA threshold overlay
* Anomalies and SLA: inventory anomalies, SLA breaches, quarantine breakdown by source and rule
* Orders Summary: order status distribution, revenue by channel and region

### Testing Layer

* 23 API regression tests covering auth, pagination, schemas, idempotency, correlation IDs
* 19 SQL validation tests covering source-to-target counts, duplicate detection, referential integrity, quarantine correctness, audit trail
* Postman/Newman collection with 10 endpoint tests and assertions
* GitHub Actions CI pipeline with automated test execution


## Quick Start

### Prerequisites

* Python 3.10+
* No database setup needed (uses SQLite)

### Setup and Run

```bash
cd datapulse_ai
pip install -r requirements.txt

# Run the full pipeline (generates data, runs ETL, DQ checks, root-cause analysis)
python scripts/run_pipeline.py

# Start the API server
python -m uvicorn src.api.app_with_genai:app --reload
# Open http://localhost:8000/docs for Swagger UI

# Start the dashboard
python -m streamlit run src/dashboard/app.py
# Open http://localhost:8501

# Run all tests
pytest tests/ -v
```

### Docker Setup

```bash
docker-compose up -d
docker-compose --profile pipeline run pipeline

# API:       http://localhost:8000/docs
# Dashboard: http://localhost:8501
```


## Project Structure

```
datapulse_ai/
    config/
        settings.py                 # Environment configuration
    src/
        etl/
            database.py             # SQLite connection and schema
            generate_data.py        # Synthetic data generator (Faker)
            pipeline.py             # ETL orchestrator (raw > staging > curated)
        data_quality/
            dq_engine.py            # DQ framework with 15+ rule types
        api/
            main.py                 # FastAPI application
            app_with_genai.py       # API with GenAI routes
        dashboard/
            app.py                  # Streamlit dashboard
        genai/
            root_cause_agent.py     # LLM and rule-based root-cause analyzer
    tests/
        api_tests/
            test_api.py             # 23 API regression tests
        sql_tests/
            test_sql_validation.py  # 19 database validation tests
        postman/
            DataPulse_AI_API.postman_collection.json
    scripts/
        run_pipeline.py             # Master pipeline runner
    data/raw/                       # Generated CSV datasets
    .github/workflows/ci.yml        # GitHub Actions CI/CD
    Dockerfile
    docker-compose.yml
    requirements.txt
    .env.example
```


## Database Schema

### Raw Layer
raw_orders, raw_customers, raw_payments, raw_inventory, raw_clickstream_events, raw_subscription_events, raw_api_logs

### Staging Layer
stg_orders_cleaned, stg_customer_activity

### Curated Layer
fact_orders, dim_customer, dim_product

### Observability
fact_pipeline_runs, fact_data_quality_results, quarantine_records, audit_log


## Testing Strategy

* **API Regression** (Pytest + FastAPI TestClient): auth enforcement, pagination, response schemas, idempotency, correlation ID propagation
* **SQL Validation** (Pytest + SQLite): source-to-target counts, no duplicates in staging/curated, referential integrity on fact tables, quarantine correctness, audit trail completeness
* **API Collection** (Postman/Newman): all endpoints with status code and response body assertions
* **CI/CD** (GitHub Actions): full pipeline execution + all test suites on every push


## Sample Pipeline Output

When you run the pipeline, you will see output like this:

```
Pipeline SUCCESS
Runtime: 0.37s
Results:
  raw_orders: 2015 ingested
  stg_orders: 1937 inserted, 78 rejected (quarantined)
  dim_customer: 500 loaded
  dim_product: 15 loaded
  fact_orders: 1937 loaded

DQ Summary: 13/26 passed (50.0%), 3 critical failures

Root-Cause Analysis: 14 findings generated
  * 23 null order_ids detected in raw_orders
  * 37 duplicate order_ids in raw_orders
  * 27 orphan payment records (no matching order)
  * 40 negative quantities quarantined
  * Source-to-target mismatch: 2015 raw vs 1937 staged (78 rejected)
```

The intentional dirty data is there by design to demonstrate the DQ framework catching real issues.
