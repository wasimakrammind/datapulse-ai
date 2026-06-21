"""DataPulse AI - FastAPI Data Product API (SQLite)."""
import sys, os, uuid, math
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Depends, Query, Request
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config.settings import API_KEY
from src.etl.database import get_cursor

import structlog
structlog.configure(processors=[
    structlog.processors.TimeStamper(fmt="iso"),
    structlog.processors.JSONRenderer()
])
logger = structlog.get_logger()

app = FastAPI(
    title="DataPulse AI",
    description="Big Data Quality, Observability & GenAI Root-Cause Platform API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(api_key: str = Depends(api_key_header)):
    if api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
    return api_key


@app.middleware("http")
async def add_correlation_id(request: Request, call_next):
    correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
    request.state.correlation_id = correlation_id
    logger.info("request_received", method=request.method, path=request.url.path, correlation_id=correlation_id)
    response = await call_next(request)
    response.headers["X-Correlation-ID"] = correlation_id
    return response


def _rows_to_dicts(cur):
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


class PaginatedResponse(BaseModel):
    data: list
    page: int
    page_size: int
    total_count: int
    total_pages: int


class HealthResponse(BaseModel):
    status: str
    timestamp: str
    version: str
    database: str


@app.get("/health", response_model=HealthResponse, tags=["System"])
async def health_check():
    db_status = "connected"
    try:
        with get_cursor() as (cur, conn):
            cur.execute("SELECT 1")
    except Exception:
        db_status = "disconnected"
    return HealthResponse(
        status="healthy" if db_status == "connected" else "degraded",
        timestamp=datetime.now().isoformat(),
        version="1.0.0",
        database=db_status
    )


@app.get("/pipeline-runs", response_model=PaginatedResponse, tags=["Observability"],
         dependencies=[Depends(verify_api_key)])
async def get_pipeline_runs(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    status: Optional[str] = None
):
    offset = (page - 1) * page_size
    filters = ""
    params = []
    if status:
        filters = "WHERE status = ?"
        params.append(status)

    with get_cursor() as (cur, conn):
        cur.execute(f"SELECT COUNT(*) FROM fact_pipeline_runs {filters}", params)
        total = cur.fetchone()[0]

        cur.execute(f"""
            SELECT pipeline_run_id, pipeline_name, source_name, start_time, end_time,
                   status, input_count, output_count, rejected_count, dq_failed_count,
                   runtime_seconds, error_message, correlation_id, batch_id, created_at
            FROM fact_pipeline_runs {filters}
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
        """, params + [page_size, offset])
        rows = _rows_to_dicts(cur)

    return PaginatedResponse(
        data=rows, page=page, page_size=page_size,
        total_count=total, total_pages=math.ceil(total / page_size) if total > 0 else 0
    )


@app.get("/data-quality-results", response_model=PaginatedResponse, tags=["Data Quality"],
         dependencies=[Depends(verify_api_key)])
async def get_dq_results(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    passed: Optional[bool] = None,
    severity: Optional[str] = None,
    table_name: Optional[str] = None
):
    offset = (page - 1) * page_size
    conditions = []
    params = []
    if passed is not None:
        conditions.append("passed = ?")
        params.append(1 if passed else 0)
    if severity:
        conditions.append("severity = ?")
        params.append(severity)
    if table_name:
        conditions.append("table_name = ?")
        params.append(table_name)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    with get_cursor() as (cur, conn):
        cur.execute(f"SELECT COUNT(*) FROM fact_data_quality_results {where}", params)
        total = cur.fetchone()[0]

        cur.execute(f"""
            SELECT dq_id, pipeline_run_id, table_name, rule_name, rule_type, column_name,
                   severity, passed, failed_count, total_count, failure_percentage, details,
                   checked_at, batch_id
            FROM fact_data_quality_results {where}
            ORDER BY checked_at DESC
            LIMIT ? OFFSET ?
        """, params + [page_size, offset])
        rows = _rows_to_dicts(cur)

    return PaginatedResponse(
        data=rows, page=page, page_size=page_size,
        total_count=total, total_pages=math.ceil(total / page_size) if total > 0 else 0
    )


@app.get("/orders/summary", tags=["Business Data"], dependencies=[Depends(verify_api_key)])
async def get_orders_summary():
    with get_cursor() as (cur, conn):
        cur.execute("""
            SELECT
                COUNT(*) as total_orders,
                COUNT(DISTINCT customer_key) as unique_customers,
                COALESCE(SUM(total_amount), 0) as total_revenue,
                COALESCE(AVG(total_amount), 0) as avg_order_value,
                SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END) as cancelled,
                SUM(CASE WHEN status='returned' THEN 1 ELSE 0 END) as returned,
                SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN status='shipped' THEN 1 ELSE 0 END) as shipped
            FROM fact_orders
        """)
        cols = [d[0] for d in cur.description]
        summary = dict(zip(cols, cur.fetchone()))

        cur.execute("""
            SELECT channel, COUNT(*) as order_count, COALESCE(SUM(total_amount),0) as revenue
            FROM fact_orders GROUP BY channel ORDER BY revenue DESC
        """)
        by_channel = _rows_to_dicts(cur)

        cur.execute("""
            SELECT region, COUNT(*) as order_count, COALESCE(SUM(total_amount),0) as revenue
            FROM fact_orders GROUP BY region ORDER BY revenue DESC
        """)
        by_region = _rows_to_dicts(cur)

    return {"summary": summary, "by_channel": by_channel, "by_region": by_region}


@app.get("/inventory/anomalies", tags=["Business Data"], dependencies=[Depends(verify_api_key)])
async def get_inventory_anomalies():
    with get_cursor() as (cur, conn):
        cur.execute("""
            SELECT product_id, warehouse_id, warehouse_region, quantity_on_hand,
                   reorder_level, last_restock_date, snapshot_date
            FROM raw_inventory
            WHERE quantity_on_hand < 0 OR quantity_on_hand < reorder_level
               OR warehouse_region IS NULL OR warehouse_region = ''
            ORDER BY quantity_on_hand ASC
            LIMIT 50
        """)
        anomalies = _rows_to_dicts(cur)

    return {
        "anomaly_count": len(anomalies),
        "anomalies": anomalies,
        "types": {
            "negative_stock": sum(1 for a in anomalies if (a.get('quantity_on_hand') or 0) < 0),
            "below_reorder": sum(1 for a in anomalies
                                 if 0 <= (a.get('quantity_on_hand') or 0) < (a.get('reorder_level') or 0)),
            "missing_region": sum(1 for a in anomalies if not a.get('warehouse_region'))
        }
    }


@app.get("/reports/execution-misses", tags=["Observability"], dependencies=[Depends(verify_api_key)])
async def get_execution_misses():
    with get_cursor() as (cur, conn):
        cur.execute("""
            SELECT pipeline_run_id, pipeline_name, start_time, end_time, status,
                   runtime_seconds, error_message, batch_id
            FROM fact_pipeline_runs
            WHERE status = 'failed' OR runtime_seconds > 300
            ORDER BY created_at DESC
            LIMIT 50
        """)
        misses = _rows_to_dicts(cur)

    return {
        "total_misses": len(misses),
        "failed_runs": sum(1 for m in misses if m.get('status') == 'failed'),
        "sla_breaches": sum(1 for m in misses if (m.get('runtime_seconds') or 0) > 300),
        "misses": misses
    }


@app.get("/quarantine/records", response_model=PaginatedResponse, tags=["Data Quality"],
         dependencies=[Depends(verify_api_key)])
async def get_quarantine_records(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    source_table: Optional[str] = None
):
    offset = (page - 1) * page_size
    filters = ""
    params = []
    if source_table:
        filters = "WHERE source_table = ?"
        params.append(source_table)

    with get_cursor() as (cur, conn):
        cur.execute(f"SELECT COUNT(*) FROM quarantine_records {filters}", params)
        total = cur.fetchone()[0]

        cur.execute(f"""
            SELECT quarantine_id, source_table, record_data, failure_reason,
                   rule_name, batch_id, quarantined_at
            FROM quarantine_records {filters}
            ORDER BY quarantined_at DESC
            LIMIT ? OFFSET ?
        """, params + [page_size, offset])
        rows = _rows_to_dicts(cur)

    return PaginatedResponse(
        data=rows, page=page, page_size=page_size,
        total_count=total, total_pages=math.ceil(total / page_size) if total > 0 else 0
    )


@app.get("/audit-log", tags=["Observability"], dependencies=[Depends(verify_api_key)])
async def get_audit_log(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100)
):
    offset = (page - 1) * page_size
    with get_cursor() as (cur, conn):
        cur.execute("SELECT COUNT(*) FROM audit_log")
        total = cur.fetchone()[0]

        cur.execute("""
            SELECT audit_id, table_name, operation, batch_id, row_count,
                   source_count, target_count, reconciliation_status, details, created_at
            FROM audit_log ORDER BY created_at DESC LIMIT ? OFFSET ?
        """, (page_size, offset))
        rows = _rows_to_dicts(cur)

    return PaginatedResponse(
        data=rows, page=page, page_size=page_size,
        total_count=total, total_pages=math.ceil(total / page_size) if total > 0 else 0
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    correlation_id = getattr(request.state, 'correlation_id', 'unknown')
    logger.error("unhandled_error", error=str(exc), correlation_id=correlation_id)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error", "correlation_id": correlation_id}
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
