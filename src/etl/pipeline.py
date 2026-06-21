"""ETL Pipeline: Raw -> Staging -> Curated with full/incremental load support."""
import os, sys, csv, uuid, json
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config.settings import RAW_DIR
from src.etl.database import get_cursor


def _generate_batch_id():
    return f"BATCH-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"


def _read_csv(filename):
    path = os.path.join(RAW_DIR, filename)
    with open(path, 'r', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def _log_audit(cur, table, operation, batch_id, row_count, source_count=0, target_count=0, status="match", details=""):
    cur.execute("""
        INSERT INTO audit_log (table_name, operation, batch_id, row_count, source_count, target_count, reconciliation_status, details)
        VALUES (?,?,?,?,?,?,?,?)
    """, (table, operation, batch_id, row_count, source_count, target_count, status, details))


def _quarantine(cur, source_table, record, reason, rule_name, batch_id):
    cur.execute("""
        INSERT INTO quarantine_records (source_table, record_data, failure_reason, rule_name, batch_id)
        VALUES (?,?,?,?,?)
    """, (source_table, json.dumps(record, default=str), reason, rule_name, batch_id))


# ===================== RAW INGESTION =====================

def ingest_raw_orders(batch_id):
    rows = _read_csv("raw_orders.csv")
    inserted = 0
    with get_cursor() as (cur, conn):
        for r in rows:
            cur.execute("""
                INSERT INTO raw_orders (order_id, customer_id, product_id, order_date, quantity,
                    unit_price, total_amount, status, channel, region, batch_id, source_file)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (r['order_id'], r['customer_id'], r['product_id'], r['order_date'] or None,
                  int(r['quantity']), float(r['unit_price']), float(r['total_amount']),
                  r['status'], r['channel'], r['region'], batch_id, "raw_orders.csv"))
            inserted += 1
        _log_audit(cur, "raw_orders", "ingest", batch_id, inserted, len(rows), inserted)
    return inserted


def ingest_raw_customers(batch_id):
    rows = _read_csv("raw_customers.csv")
    inserted = 0
    with get_cursor() as (cur, conn):
        for r in rows:
            cur.execute("""
                INSERT INTO raw_customers (customer_id, name, email, phone, city, state, country,
                    signup_date, customer_type, batch_id, source_file)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (r['customer_id'], r['name'], r['email'] or None, r.get('phone') or None,
                  r['city'], r['state'], r['country'],
                  r['signup_date'] or None, r['customer_type'], batch_id, "raw_customers.csv"))
            inserted += 1
        _log_audit(cur, "raw_customers", "ingest", batch_id, inserted, len(rows), inserted)
    return inserted


def ingest_raw_payments(batch_id):
    rows = _read_csv("raw_payments.csv")
    inserted = 0
    with get_cursor() as (cur, conn):
        for r in rows:
            cur.execute("""
                INSERT INTO raw_payments (payment_id, order_id, payment_date, amount, method, status, currency, batch_id, source_file)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (r['payment_id'], r['order_id'], r['payment_date'], float(r['amount']),
                  r['method'], r['status'], r['currency'], batch_id, "raw_payments.csv"))
            inserted += 1
        _log_audit(cur, "raw_payments", "ingest", batch_id, inserted, len(rows), inserted)
    return inserted


def ingest_raw_inventory(batch_id):
    rows = _read_csv("raw_inventory.csv")
    inserted = 0
    with get_cursor() as (cur, conn):
        for r in rows:
            cur.execute("""
                INSERT INTO raw_inventory (product_id, warehouse_id, warehouse_region, quantity_on_hand,
                    reorder_level, last_restock_date, snapshot_date, batch_id, source_file)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (r['product_id'], r['warehouse_id'], r['warehouse_region'] or None,
                  int(r['quantity_on_hand']), int(r['reorder_level']),
                  r['last_restock_date'], r['snapshot_date'], batch_id, "raw_inventory.csv"))
            inserted += 1
        _log_audit(cur, "raw_inventory", "ingest", batch_id, inserted, len(rows), inserted)
    return inserted


def ingest_raw_clickstream(batch_id):
    rows = _read_csv("raw_clickstream_events.csv")
    inserted = 0
    with get_cursor() as (cur, conn):
        for r in rows:
            cur.execute("""
                INSERT INTO raw_clickstream_events (event_id, session_id, customer_id, event_type,
                    page_url, referrer, device_type, browser, event_timestamp, duration_seconds, batch_id, source_file)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (r['event_id'], r['session_id'], r['customer_id'] or None, r['event_type'],
                  r['page_url'], r['referrer'], r['device_type'], r['browser'],
                  r['event_timestamp'], int(r['duration_seconds']), batch_id, "raw_clickstream_events.csv"))
            inserted += 1
        _log_audit(cur, "raw_clickstream_events", "ingest", batch_id, inserted, len(rows), inserted)
    return inserted


def ingest_raw_subscriptions(batch_id):
    rows = _read_csv("raw_subscription_events.csv")
    inserted = 0
    with get_cursor() as (cur, conn):
        for r in rows:
            cur.execute("""
                INSERT INTO raw_subscription_events (subscription_id, customer_id, plan, event_type,
                    event_date, monthly_amount, billing_cycle, batch_id, source_file)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (r['subscription_id'], r['customer_id'], r['plan'], r['event_type'],
                  r['event_date'], float(r['monthly_amount']), r['billing_cycle'], batch_id, "raw_subscription_events.csv"))
            inserted += 1
        _log_audit(cur, "raw_subscription_events", "ingest", batch_id, inserted, len(rows), inserted)
    return inserted


def ingest_raw_api_logs(batch_id):
    rows = _read_csv("raw_api_logs.csv")
    inserted = 0
    with get_cursor() as (cur, conn):
        for r in rows:
            cur.execute("""
                INSERT INTO raw_api_logs (request_id, endpoint, method, status_code, response_time_ms,
                    client_ip, user_agent, request_timestamp, error_message, batch_id, source_file)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (r['request_id'], r['endpoint'], r['method'], int(r['status_code']),
                  int(r['response_time_ms']), r['client_ip'], r['user_agent'],
                  r['request_timestamp'], r['error_message'], batch_id, "raw_api_logs.csv"))
            inserted += 1
        _log_audit(cur, "raw_api_logs", "ingest", batch_id, inserted, len(rows), inserted)
    return inserted


# ===================== STAGING TRANSFORMS =====================

def stage_orders(batch_id):
    with get_cursor() as (cur, conn):
        cur.execute("SELECT * FROM raw_orders WHERE batch_id = ?", (batch_id,))
        raw_rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]

        seen_ids = set()
        inserted, rejected = 0, 0

        for row in raw_rows:
            rec = dict(zip(cols, row))
            order_id = rec['order_id']

            if not order_id or str(order_id).strip() == "":
                _quarantine(cur, "raw_orders", rec, "Null or empty order_id", "null_check", batch_id)
                rejected += 1
                continue

            if order_id in seen_ids:
                _quarantine(cur, "raw_orders", rec, f"Duplicate order_id: {order_id}", "duplicate_check", batch_id)
                rejected += 1
                continue

            if rec['quantity'] is not None and rec['quantity'] < 0:
                _quarantine(cur, "raw_orders", rec, f"Negative quantity: {rec['quantity']}", "negative_value_check", batch_id)
                rejected += 1
                continue

            seen_ids.add(order_id)
            cur.execute("""
                INSERT OR REPLACE INTO stg_orders_cleaned (order_id, customer_id, product_id, order_date, quantity,
                    unit_price, total_amount, status, channel, region, batch_id, dq_passed, cleaned_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,1,datetime('now'))
            """, (order_id, rec['customer_id'], rec['product_id'], rec['order_date'],
                  rec['quantity'], rec['unit_price'], rec['total_amount'],
                  rec['status'], rec['channel'], rec['region'], batch_id))
            inserted += 1

        _log_audit(cur, "stg_orders_cleaned", "stage", batch_id, inserted,
                   len(raw_rows), inserted,
                   "match" if rejected == 0 else "mismatch",
                   f"rejected={rejected}")
    return inserted, rejected


def stage_customers(batch_id):
    with get_cursor() as (cur, conn):
        cur.execute("""
            SELECT DISTINCT rc.customer_id, rc.name, rc.email, rc.city, rc.state, rc.country,
                rc.signup_date, rc.customer_type,
                COALESCE(o.order_count, 0) as total_orders,
                COALESCE(o.total_spent, 0) as total_spent,
                o.last_order_date
            FROM raw_customers rc
            LEFT JOIN (
                SELECT customer_id, COUNT(*) as order_count,
                       SUM(total_amount) as total_spent,
                       MAX(order_date) as last_order_date
                FROM stg_orders_cleaned
                WHERE dq_passed = 1
                GROUP BY customer_id
            ) o ON rc.customer_id = o.customer_id
            WHERE rc.batch_id = ? AND rc.customer_id IS NOT NULL AND rc.customer_id != ''
            GROUP BY rc.customer_id
        """, (batch_id,))
        rows = cur.fetchall()

        count = 0
        for row in rows:
            cur.execute("""
                INSERT OR REPLACE INTO stg_customer_activity
                    (customer_id, name, email, city, state, country, signup_date, customer_type,
                     total_orders, total_spent, last_order_date, batch_id, cleaned_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
            """, (row[0], row[1], row[2], row[3], row[4], row[5],
                  row[6], row[7], row[8], row[9], row[10], batch_id))
            count += 1

        _log_audit(cur, "stg_customer_activity", "stage", batch_id, count)
    return count


# ===================== CURATED LAYER =====================

def load_dim_product(batch_id):
    rows = _read_csv("raw_products.csv")
    with get_cursor() as (cur, conn):
        for r in rows:
            cur.execute("""
                INSERT OR REPLACE INTO dim_product (product_id, product_name, category, subcategory, unit_price, updated_at)
                VALUES (?,?,?,?,?,datetime('now'))
            """, (r['product_id'], r['product_name'], r['category'], r['subcategory'], float(r['unit_price'])))
        _log_audit(cur, "dim_product", "load", batch_id, len(rows))
    return len(rows)


def load_dim_customer(batch_id):
    with get_cursor() as (cur, conn):
        cur.execute("SELECT * FROM stg_customer_activity WHERE batch_id = ?", (batch_id,))
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        count = 0
        for row in rows:
            r = dict(zip(cols, row))
            cur.execute("""
                INSERT OR REPLACE INTO dim_customer
                    (customer_id, name, email, city, state, country, signup_date, customer_type,
                     total_orders, total_spent, last_order_date, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
            """, (r['customer_id'], r['name'], r['email'], r['city'], r['state'], r['country'],
                  r['signup_date'], r['customer_type'], r['total_orders'], r['total_spent'], r['last_order_date']))
            count += 1
        _log_audit(cur, "dim_customer", "load", batch_id, count)
    return count


def load_fact_orders(batch_id):
    with get_cursor() as (cur, conn):
        cur.execute("""
            SELECT s.order_id, dc.customer_key, dp.product_key, s.order_date, s.quantity,
                s.unit_price, s.total_amount, s.status, s.channel, s.region
            FROM stg_orders_cleaned s
            LEFT JOIN dim_customer dc ON s.customer_id = dc.customer_id
            LEFT JOIN dim_product dp ON s.product_id = dp.product_id
            WHERE s.batch_id = ? AND s.dq_passed = 1
        """, (batch_id,))
        rows = cur.fetchall()

        count = 0
        for row in rows:
            cur.execute("""
                INSERT OR REPLACE INTO fact_orders
                    (order_id, customer_key, product_key, order_date, quantity,
                     unit_price, total_amount, status, channel, region, batch_id, loaded_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
            """, (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], batch_id))
            count += 1

        _log_audit(cur, "fact_orders", "load", batch_id, count)

        # Source-to-target reconciliation
        cur.execute("SELECT COUNT(*) FROM stg_orders_cleaned WHERE batch_id=? AND dq_passed=1", (batch_id,))
        src = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM fact_orders WHERE batch_id=?", (batch_id,))
        tgt = cur.fetchone()[0]
        recon_status = "match" if src == tgt else "mismatch"
        _log_audit(cur, "fact_orders", "reconciliation", batch_id, tgt, src, tgt, recon_status,
                   f"source={src}, target={tgt}, diff={src-tgt}")
    return count


# ===================== ORCHESTRATOR =====================

def run_full_pipeline():
    batch_id = _generate_batch_id()
    start = datetime.now()
    correlation_id = str(uuid.uuid4())
    results = {}
    error_msg = None
    status = "success"

    print(f"\n{'='*60}")
    print(f"DataPulse ETL Pipeline - Full Load")
    print(f"Batch ID: {batch_id}")
    print(f"Correlation ID: {correlation_id}")
    print(f"Started: {start.isoformat()}")
    print(f"{'='*60}\n")

    try:
        print("[1/8] Ingesting raw orders...")
        results['raw_orders'] = ingest_raw_orders(batch_id)

        print("[2/8] Ingesting raw customers...")
        results['raw_customers'] = ingest_raw_customers(batch_id)

        print("[3/8] Ingesting raw payments...")
        results['raw_payments'] = ingest_raw_payments(batch_id)

        print("[4/8] Ingesting raw inventory...")
        results['raw_inventory'] = ingest_raw_inventory(batch_id)

        print("[5/8] Ingesting clickstream, subscriptions, API logs...")
        results['raw_clickstream'] = ingest_raw_clickstream(batch_id)
        results['raw_subscriptions'] = ingest_raw_subscriptions(batch_id)
        results['raw_api_logs'] = ingest_raw_api_logs(batch_id)

        print("[6/8] Staging orders (clean + deduplicate + quarantine)...")
        inserted, rejected = stage_orders(batch_id)
        results['stg_orders'] = {"inserted": inserted, "rejected": rejected}

        print("[7/8] Staging customers...")
        results['stg_customers'] = stage_customers(batch_id)

        print("[8/8] Loading curated layer...")
        results['dim_product'] = load_dim_product(batch_id)
        results['dim_customer'] = load_dim_customer(batch_id)
        results['fact_orders'] = load_fact_orders(batch_id)

    except Exception as e:
        status = "failed"
        error_msg = str(e)
        print(f"\nPIPELINE FAILED: {e}")
        import traceback
        traceback.print_exc()

    end = datetime.now()
    runtime = (end - start).total_seconds()

    total_input = sum(v if isinstance(v, int) else v.get('inserted', 0) for v in results.values())
    total_rejected = results.get('stg_orders', {}).get('rejected', 0) if isinstance(results.get('stg_orders'), dict) else 0

    with get_cursor() as (cur, conn):
        cur.execute("""
            INSERT INTO fact_pipeline_runs (pipeline_run_id, pipeline_name, source_name,
                start_time, end_time, status, input_count, output_count, rejected_count,
                runtime_seconds, error_message, correlation_id, batch_id)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (f"RUN-{batch_id}", "full_etl_pipeline", "csv_files",
              start.isoformat(), end.isoformat(), status, total_input, total_input - total_rejected,
              total_rejected, runtime, error_msg, correlation_id, batch_id))

    print(f"\n{'='*60}")
    print(f"Pipeline {status.upper()}")
    print(f"Runtime: {runtime:.2f}s")
    print(f"Results: {json.dumps(results, indent=2, default=str)}")
    print(f"{'='*60}\n")

    return batch_id, status, results


if __name__ == "__main__":
    run_full_pipeline()
