import sqlite3
import os, sys
from contextlib import contextmanager

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config.settings import DB_PATH


def get_connection():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def get_cursor(dict_cursor=False):
    conn = get_connection()
    cur = conn.cursor()
    try:
        yield cur, conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def init_database():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    print(f"Database: {DB_PATH}")


def create_schema():
    schema_sql = """
    -- ===================== RAW LAYER =====================
    CREATE TABLE IF NOT EXISTS raw_orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id TEXT,
        customer_id TEXT,
        product_id TEXT,
        order_date TEXT,
        quantity INTEGER,
        unit_price REAL,
        total_amount REAL,
        status TEXT,
        channel TEXT,
        region TEXT,
        ingested_at TEXT DEFAULT (datetime('now')),
        batch_id TEXT,
        source_file TEXT
    );

    CREATE TABLE IF NOT EXISTS raw_customers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id TEXT,
        name TEXT,
        email TEXT,
        phone TEXT,
        city TEXT,
        state TEXT,
        country TEXT,
        signup_date TEXT,
        customer_type TEXT,
        ingested_at TEXT DEFAULT (datetime('now')),
        batch_id TEXT,
        source_file TEXT
    );

    CREATE TABLE IF NOT EXISTS raw_payments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        payment_id TEXT,
        order_id TEXT,
        payment_date TEXT,
        amount REAL,
        method TEXT,
        status TEXT,
        currency TEXT,
        ingested_at TEXT DEFAULT (datetime('now')),
        batch_id TEXT,
        source_file TEXT
    );

    CREATE TABLE IF NOT EXISTS raw_inventory (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id TEXT,
        warehouse_id TEXT,
        warehouse_region TEXT,
        quantity_on_hand INTEGER,
        reorder_level INTEGER,
        last_restock_date TEXT,
        snapshot_date TEXT,
        ingested_at TEXT DEFAULT (datetime('now')),
        batch_id TEXT,
        source_file TEXT
    );

    CREATE TABLE IF NOT EXISTS raw_clickstream_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT,
        session_id TEXT,
        customer_id TEXT,
        event_type TEXT,
        page_url TEXT,
        referrer TEXT,
        device_type TEXT,
        browser TEXT,
        event_timestamp TEXT,
        duration_seconds INTEGER,
        ingested_at TEXT DEFAULT (datetime('now')),
        batch_id TEXT,
        source_file TEXT
    );

    CREATE TABLE IF NOT EXISTS raw_subscription_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        subscription_id TEXT,
        customer_id TEXT,
        plan TEXT,
        event_type TEXT,
        event_date TEXT,
        monthly_amount REAL,
        billing_cycle TEXT,
        ingested_at TEXT DEFAULT (datetime('now')),
        batch_id TEXT,
        source_file TEXT
    );

    CREATE TABLE IF NOT EXISTS raw_api_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        request_id TEXT,
        endpoint TEXT,
        method TEXT,
        status_code INTEGER,
        response_time_ms INTEGER,
        client_ip TEXT,
        user_agent TEXT,
        request_timestamp TEXT,
        error_message TEXT,
        ingested_at TEXT DEFAULT (datetime('now')),
        batch_id TEXT,
        source_file TEXT
    );

    -- ===================== STAGING LAYER =====================
    CREATE TABLE IF NOT EXISTS stg_orders_cleaned (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id TEXT UNIQUE,
        customer_id TEXT,
        product_id TEXT,
        order_date TEXT,
        quantity INTEGER,
        unit_price REAL,
        total_amount REAL,
        status TEXT,
        channel TEXT,
        region TEXT,
        batch_id TEXT,
        cleaned_at TEXT DEFAULT (datetime('now')),
        dq_passed INTEGER DEFAULT 1
    );

    CREATE TABLE IF NOT EXISTS stg_customer_activity (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id TEXT UNIQUE,
        name TEXT,
        email TEXT,
        city TEXT,
        state TEXT,
        country TEXT,
        signup_date TEXT,
        customer_type TEXT,
        total_orders INTEGER DEFAULT 0,
        total_spent REAL DEFAULT 0,
        last_order_date TEXT,
        batch_id TEXT,
        cleaned_at TEXT DEFAULT (datetime('now'))
    );

    -- ===================== CURATED LAYER =====================
    CREATE TABLE IF NOT EXISTS dim_customer (
        customer_key INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id TEXT UNIQUE,
        name TEXT,
        email TEXT,
        city TEXT,
        state TEXT,
        country TEXT,
        signup_date TEXT,
        customer_type TEXT,
        total_orders INTEGER DEFAULT 0,
        total_spent REAL DEFAULT 0,
        last_order_date TEXT,
        updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS dim_product (
        product_key INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id TEXT UNIQUE,
        product_name TEXT,
        category TEXT,
        subcategory TEXT,
        unit_price REAL,
        updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS fact_orders (
        order_key INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id TEXT UNIQUE,
        customer_key INTEGER REFERENCES dim_customer(customer_key),
        product_key INTEGER REFERENCES dim_product(product_key),
        order_date TEXT,
        quantity INTEGER,
        unit_price REAL,
        total_amount REAL,
        status TEXT,
        channel TEXT,
        region TEXT,
        batch_id TEXT,
        loaded_at TEXT DEFAULT (datetime('now'))
    );

    -- ===================== OBSERVABILITY =====================
    CREATE TABLE IF NOT EXISTS fact_pipeline_runs (
        run_id INTEGER PRIMARY KEY AUTOINCREMENT,
        pipeline_run_id TEXT UNIQUE,
        pipeline_name TEXT,
        source_name TEXT,
        start_time TEXT,
        end_time TEXT,
        status TEXT,
        input_count INTEGER DEFAULT 0,
        output_count INTEGER DEFAULT 0,
        rejected_count INTEGER DEFAULT 0,
        dq_failed_count INTEGER DEFAULT 0,
        runtime_seconds REAL,
        error_message TEXT,
        correlation_id TEXT,
        batch_id TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS fact_data_quality_results (
        dq_id INTEGER PRIMARY KEY AUTOINCREMENT,
        pipeline_run_id TEXT,
        table_name TEXT,
        rule_name TEXT,
        rule_type TEXT,
        column_name TEXT,
        severity TEXT,
        passed INTEGER,
        failed_count INTEGER DEFAULT 0,
        total_count INTEGER DEFAULT 0,
        failure_percentage REAL,
        details TEXT,
        checked_at TEXT DEFAULT (datetime('now')),
        batch_id TEXT
    );

    -- ===================== QUARANTINE =====================
    CREATE TABLE IF NOT EXISTS quarantine_records (
        quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_table TEXT,
        record_data TEXT,
        failure_reason TEXT,
        rule_name TEXT,
        batch_id TEXT,
        quarantined_at TEXT DEFAULT (datetime('now'))
    );

    -- ===================== AUDIT =====================
    CREATE TABLE IF NOT EXISTS audit_log (
        audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
        table_name TEXT,
        operation TEXT,
        batch_id TEXT,
        row_count INTEGER,
        source_count INTEGER,
        target_count INTEGER,
        reconciliation_status TEXT,
        details TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    """

    with get_cursor() as (cur, conn):
        cur.executescript(schema_sql)
        print("Schema created successfully.")
