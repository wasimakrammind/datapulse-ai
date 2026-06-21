"""Data Quality Engine - comprehensive validation framework (SQLite)."""
import sys, os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from src.etl.database import get_cursor


class DQResult:
    def __init__(self, table, rule, rule_type, column, severity, passed, failed_count, total_count, details=""):
        self.table = table
        self.rule = rule
        self.rule_type = rule_type
        self.column = column
        self.severity = severity
        self.passed = passed
        self.failed_count = failed_count
        self.total_count = total_count
        self.failure_pct = round((failed_count / total_count * 100), 2) if total_count > 0 else 0
        self.details = details


class DataQualityEngine:
    def __init__(self, batch_id, pipeline_run_id=None):
        self.batch_id = batch_id
        self.pipeline_run_id = pipeline_run_id or f"RUN-{batch_id}"
        self.results = []

    def _add_result(self, result: DQResult):
        self.results.append(result)
        status = "PASS" if result.passed else "FAIL"
        print(f"  [{status}] {result.rule} on {result.table}.{result.column or '*'} "
              f"({result.failed_count}/{result.total_count} failed, {result.failure_pct}%)")

    def _run_count_query(self, query, params=None):
        with get_cursor() as (cur, conn):
            cur.execute(query, params or ())
            return cur.fetchone()[0]

    def check_null(self, table, column, severity="high"):
        total = self._run_count_query(f"SELECT COUNT(*) FROM {table} WHERE batch_id=?", (self.batch_id,))
        failed = self._run_count_query(
            f"SELECT COUNT(*) FROM {table} WHERE batch_id=? AND ({column} IS NULL OR CAST({column} AS TEXT) = '')",
            (self.batch_id,))
        self._add_result(DQResult(table, f"null_check_{column}", "null_check", column, severity,
                                  failed == 0, failed, total, f"{failed} null/empty values found"))

    def check_duplicates(self, table, column, severity="high"):
        total = self._run_count_query(f"SELECT COUNT(*) FROM {table} WHERE batch_id=?", (self.batch_id,))
        failed = self._run_count_query(
            f"SELECT COUNT(*) - COUNT(DISTINCT {column}) FROM {table} WHERE batch_id=? AND {column} IS NOT NULL",
            (self.batch_id,))
        self._add_result(DQResult(table, f"duplicate_check_{column}", "duplicate_check", column, severity,
                                  failed == 0, failed, total, f"{failed} duplicate values"))

    def check_primary_key(self, table, column, severity="critical"):
        null_count = self._run_count_query(
            f"SELECT COUNT(*) FROM {table} WHERE batch_id=? AND ({column} IS NULL OR CAST({column} AS TEXT) = '')",
            (self.batch_id,))
        dup_count = self._run_count_query(
            f"SELECT COUNT(*) - COUNT(DISTINCT {column}) FROM {table} WHERE batch_id=? AND {column} IS NOT NULL",
            (self.batch_id,))
        total = self._run_count_query(f"SELECT COUNT(*) FROM {table} WHERE batch_id=?", (self.batch_id,))
        failed = null_count + dup_count
        self._add_result(DQResult(table, f"pk_validation_{column}", "primary_key", column, severity,
                                  failed == 0, failed, total,
                                  f"nulls={null_count}, duplicates={dup_count}"))

    def check_referential_integrity(self, child_table, child_col, parent_table, parent_col, severity="high"):
        failed = self._run_count_query(f"""
            SELECT COUNT(*) FROM {child_table} c
            WHERE c.batch_id=? AND c.{child_col} IS NOT NULL
            AND NOT EXISTS (SELECT 1 FROM {parent_table} p WHERE p.{parent_col} = c.{child_col})
        """, (self.batch_id,))
        total = self._run_count_query(f"SELECT COUNT(*) FROM {child_table} WHERE batch_id=?", (self.batch_id,))
        self._add_result(DQResult(child_table, f"ref_integrity_{child_col}_to_{parent_table}",
                                  "referential_integrity", child_col, severity,
                                  failed == 0, failed, total,
                                  f"{failed} orphan records (no match in {parent_table}.{parent_col})"))

    def check_negative_values(self, table, column, severity="medium"):
        total = self._run_count_query(f"SELECT COUNT(*) FROM {table} WHERE batch_id=?", (self.batch_id,))
        failed = self._run_count_query(
            f"SELECT COUNT(*) FROM {table} WHERE batch_id=? AND {column} < 0",
            (self.batch_id,))
        self._add_result(DQResult(table, f"negative_check_{column}", "negative_value", column, severity,
                                  failed == 0, failed, total, f"{failed} negative values"))

    def check_invalid_status(self, table, column, valid_values, severity="medium"):
        placeholders = ','.join(['?'] * len(valid_values))
        failed = self._run_count_query(
            f"SELECT COUNT(*) FROM {table} WHERE batch_id=? AND {column} NOT IN ({placeholders})",
            (self.batch_id, *valid_values))
        total = self._run_count_query(f"SELECT COUNT(*) FROM {table} WHERE batch_id=?", (self.batch_id,))
        self._add_result(DQResult(table, f"invalid_status_{column}", "value_validation", column, severity,
                                  failed == 0, failed, total,
                                  f"{failed} invalid values (allowed: {valid_values})"))

    def check_freshness(self, table, date_column, max_age_hours=48, severity="high"):
        cutoff = (datetime.now() - timedelta(hours=max_age_hours)).isoformat()
        with get_cursor() as (cur, conn):
            cur.execute(f"SELECT MAX({date_column}) FROM {table} WHERE batch_id=?", (self.batch_id,))
            row = cur.fetchone()
            latest = row[0] if row else None

        if latest is None:
            self._add_result(DQResult(table, f"freshness_{date_column}", "freshness", date_column, severity,
                                      False, 1, 1, "No data found"))
        else:
            is_fresh = latest >= cutoff
            self._add_result(DQResult(table, f"freshness_{date_column}", "freshness", date_column, severity,
                                      is_fresh, 0 if is_fresh else 1, 1,
                                      f"Latest: {latest}, Cutoff: {cutoff}"))

    def check_source_target_count(self, source_table, target_table, severity="critical"):
        src = self._run_count_query(f"SELECT COUNT(*) FROM {source_table} WHERE batch_id=?", (self.batch_id,))
        tgt = self._run_count_query(f"SELECT COUNT(*) FROM {target_table} WHERE batch_id=?", (self.batch_id,))
        diff = abs(src - tgt)
        self._add_result(DQResult(source_table, f"src_tgt_count_{source_table}_vs_{target_table}",
                                  "reconciliation", "*", severity,
                                  diff == 0, diff, src,
                                  f"source={src}, target={tgt}, diff={diff}"))

    def check_aggregate_reconciliation(self, source_table, target_table, agg_column, severity="high"):
        src_sum = self._run_count_query(
            f"SELECT COALESCE(SUM({agg_column}), 0) FROM {source_table} WHERE batch_id=?", (self.batch_id,))
        tgt_sum = self._run_count_query(
            f"SELECT COALESCE(SUM({agg_column}), 0) FROM {target_table} WHERE batch_id=?", (self.batch_id,))
        diff = abs(float(src_sum) - float(tgt_sum))
        self._add_result(DQResult(source_table, f"agg_recon_{agg_column}",
                                  "aggregate_reconciliation", agg_column, severity,
                                  diff < 0.01, 1 if diff >= 0.01 else 0, 1,
                                  f"source_sum={src_sum}, target_sum={tgt_sum}, diff={diff:.2f}"))

    def check_volume_anomaly(self, table, threshold=0.3, severity="high"):
        with get_cursor() as (cur, conn):
            cur.execute(f"""
                SELECT batch_id, COUNT(*) as cnt
                FROM {table}
                GROUP BY batch_id
                ORDER BY MIN(ingested_at) DESC
                LIMIT 5
            """)
            batches = cur.fetchall()

        if len(batches) < 2:
            self._add_result(DQResult(table, "volume_anomaly", "anomaly_detection", "*", severity,
                                      True, 0, 1, "Not enough batches for comparison"))
            return

        current = batches[0][1]
        previous_avg = sum(b[1] for b in batches[1:]) / len(batches[1:])
        if previous_avg == 0:
            return

        change_pct = abs(current - previous_avg) / previous_avg
        is_normal = change_pct <= threshold
        self._add_result(DQResult(table, "volume_anomaly", "anomaly_detection", "*", severity,
                                  is_normal, 0 if is_normal else 1, 1,
                                  f"current={current}, prev_avg={previous_avg:.0f}, change={change_pct*100:.1f}%"))

    def check_schema_columns(self, table, expected_columns, severity="critical"):
        with get_cursor() as (cur, conn):
            cur.execute(f"PRAGMA table_info({table})")
            actual = {r[1] for r in cur.fetchall()}

        missing = set(expected_columns) - actual
        self._add_result(DQResult(table, "schema_validation", "schema", "*", severity,
                                  len(missing) == 0, len(missing), len(expected_columns),
                                  f"Missing columns: {missing}" if missing else "All columns present"))

    def save_results(self):
        with get_cursor() as (cur, conn):
            for r in self.results:
                cur.execute("""
                    INSERT INTO fact_data_quality_results (pipeline_run_id, table_name, rule_name,
                        rule_type, column_name, severity, passed, failed_count, total_count,
                        failure_percentage, details, batch_id)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """, (self.pipeline_run_id, r.table, r.rule, r.rule_type, r.column,
                      r.severity, 1 if r.passed else 0, r.failed_count, r.total_count,
                      r.failure_pct, r.details, self.batch_id))
        print(f"\n  Saved {len(self.results)} DQ results to fact_data_quality_results.")

    def summary(self):
        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        failed = total - passed
        critical_fails = sum(1 for r in self.results if not r.passed and r.severity == "critical")
        return {
            "total_checks": total,
            "passed": passed,
            "failed": failed,
            "critical_failures": critical_fails,
            "pass_rate": round(passed / total * 100, 2) if total > 0 else 0
        }


def run_all_dq_checks(batch_id):
    print(f"\n{'='*60}")
    print(f"Data Quality Checks - Batch: {batch_id}")
    print(f"{'='*60}\n")

    dq = DataQualityEngine(batch_id)

    print("-- Schema Validation --")
    dq.check_schema_columns("raw_orders", ["order_id", "customer_id", "product_id", "order_date",
                                            "quantity", "unit_price", "total_amount", "status", "channel", "region"])
    dq.check_schema_columns("raw_customers", ["customer_id", "name", "email", "city", "state", "country",
                                               "signup_date", "customer_type"])

    print("\n-- Null Checks --")
    dq.check_null("raw_orders", "order_id")
    dq.check_null("raw_orders", "customer_id")
    dq.check_null("raw_customers", "customer_id")
    dq.check_null("raw_customers", "email", severity="medium")
    dq.check_null("raw_payments", "payment_id")
    dq.check_null("raw_inventory", "warehouse_region", severity="medium")

    print("\n-- Duplicate Checks --")
    dq.check_duplicates("raw_orders", "order_id")
    dq.check_duplicates("raw_customers", "customer_id")
    dq.check_duplicates("raw_payments", "payment_id")

    print("\n-- Primary Key Validation --")
    dq.check_primary_key("raw_orders", "order_id")
    dq.check_primary_key("raw_customers", "customer_id")

    print("\n-- Referential Integrity --")
    dq.check_referential_integrity("raw_orders", "customer_id", "raw_customers", "customer_id")
    dq.check_referential_integrity("raw_payments", "order_id", "raw_orders", "order_id")

    print("\n-- Negative Value Checks --")
    dq.check_negative_values("raw_orders", "quantity")
    dq.check_negative_values("raw_orders", "total_amount")
    dq.check_negative_values("raw_payments", "amount")
    dq.check_negative_values("raw_inventory", "quantity_on_hand")

    print("\n-- Status Validation --")
    dq.check_invalid_status("raw_orders", "status",
                            ["completed", "pending", "shipped", "cancelled", "returned", "processing"])
    dq.check_invalid_status("raw_payments", "status",
                            ["success", "failed", "pending", "refunded"])

    print("\n-- Freshness Checks --")
    dq.check_freshness("raw_orders", "ingested_at", max_age_hours=48)
    dq.check_freshness("raw_clickstream_events", "ingested_at", max_age_hours=48)

    print("\n-- Source-to-Target Reconciliation --")
    dq.check_source_target_count("raw_orders", "stg_orders_cleaned")

    print("\n-- Aggregate Reconciliation --")
    dq.check_aggregate_reconciliation("stg_orders_cleaned", "fact_orders", "total_amount")

    print("\n-- Volume Anomaly Detection --")
    dq.check_volume_anomaly("raw_orders")

    dq.save_results()

    summary = dq.summary()
    print(f"\n{'='*60}")
    print(f"DQ Summary: {summary['passed']}/{summary['total_checks']} passed "
          f"({summary['pass_rate']}%), {summary['critical_failures']} critical failures")
    print(f"{'='*60}\n")

    return summary
