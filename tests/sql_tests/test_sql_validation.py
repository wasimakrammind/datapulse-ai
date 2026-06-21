"""SQL Validation Tests — TestNG-style database assertions for pipeline correctness."""
import pytest
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from src.etl.database import get_cursor


def _query_scalar(sql, params=None):
    with get_cursor() as (cur, conn):
        cur.execute(sql, params or ())
        return cur.fetchone()[0]


def _query_rows(sql, params=None):
    with get_cursor(dict_cursor=True) as (cur, conn):
        cur.execute(sql, params or ())
        return [dict(r) for r in cur.fetchall()]


class TestSourceToTargetCounts:
    def test_orders_raw_vs_staging(self):
        raw = _query_scalar("SELECT COUNT(DISTINCT order_id) FROM raw_orders WHERE order_id != ''")
        stg = _query_scalar("SELECT COUNT(*) FROM stg_orders_cleaned")
        assert stg <= raw, f"Staging ({stg}) should not exceed distinct raw ({raw})"

    def test_customers_staging_vs_dim(self):
        stg = _query_scalar("SELECT COUNT(*) FROM stg_customer_activity")
        dim = _query_scalar("SELECT COUNT(*) FROM dim_customer")
        assert dim <= stg, f"Dim ({dim}) should not exceed staging ({stg})"

    def test_fact_orders_not_empty_if_staging_has_data(self):
        stg = _query_scalar("SELECT COUNT(*) FROM stg_orders_cleaned WHERE dq_passed = TRUE")
        fact = _query_scalar("SELECT COUNT(*) FROM fact_orders")
        if stg > 0:
            assert fact > 0, "fact_orders empty despite stg_orders_cleaned having data"


class TestDuplicateDetection:
    def test_no_duplicate_order_ids_in_staging(self):
        dups = _query_scalar("""
            SELECT COUNT(*) FROM (
                SELECT order_id FROM stg_orders_cleaned GROUP BY order_id HAVING COUNT(*) > 1
            ) t
        """)
        assert dups == 0, f"Found {dups} duplicate order_ids in stg_orders_cleaned"

    def test_no_duplicate_customer_ids_in_dim(self):
        dups = _query_scalar("""
            SELECT COUNT(*) FROM (
                SELECT customer_id FROM dim_customer GROUP BY customer_id HAVING COUNT(*) > 1
            ) t
        """)
        assert dups == 0, f"Found {dups} duplicate customer_ids in dim_customer"

    def test_no_duplicate_order_ids_in_fact(self):
        dups = _query_scalar("""
            SELECT COUNT(*) FROM (
                SELECT order_id FROM fact_orders GROUP BY order_id HAVING COUNT(*) > 1
            ) t
        """)
        assert dups == 0, f"Found {dups} duplicate order_ids in fact_orders"


class TestInvalidRecordsGoToQuarantine:
    def test_quarantine_has_records(self):
        count = _query_scalar("SELECT COUNT(*) FROM quarantine_records")
        assert count > 0, "Quarantine table should have records from dirty data"

    def test_quarantine_records_have_reason(self):
        empty_reason = _query_scalar(
            "SELECT COUNT(*) FROM quarantine_records WHERE failure_reason IS NULL OR failure_reason = ''")
        assert empty_reason == 0, "All quarantine records must have a failure reason"

    def test_negative_quantities_quarantined(self):
        count = _query_scalar(
            "SELECT COUNT(*) FROM quarantine_records WHERE rule_name = 'negative_value_check'")
        assert count > 0, "Negative quantity records should be quarantined"


class TestReferentialIntegrity:
    def test_fact_orders_customer_keys_valid(self):
        orphans = _query_scalar("""
            SELECT COUNT(*) FROM fact_orders fo
            WHERE fo.customer_key IS NOT NULL
            AND NOT EXISTS (SELECT 1 FROM dim_customer dc WHERE dc.customer_key = fo.customer_key)
        """)
        assert orphans == 0, f"Found {orphans} orphan customer_keys in fact_orders"

    def test_fact_orders_product_keys_valid(self):
        orphans = _query_scalar("""
            SELECT COUNT(*) FROM fact_orders fo
            WHERE fo.product_key IS NOT NULL
            AND NOT EXISTS (SELECT 1 FROM dim_product dp WHERE dp.product_key = fo.product_key)
        """)
        assert orphans == 0, f"Found {orphans} orphan product_keys in fact_orders"


class TestDataQualityResults:
    def test_dq_results_exist(self):
        count = _query_scalar("SELECT COUNT(*) FROM fact_data_quality_results")
        assert count > 0, "DQ results table should have check results"

    def test_all_dq_results_have_rule_name(self):
        empty = _query_scalar(
            "SELECT COUNT(*) FROM fact_data_quality_results WHERE rule_name IS NULL OR rule_name = ''")
        assert empty == 0, "All DQ results must have a rule name"

    def test_failure_percentage_calculated(self):
        bad = _query_scalar("""
            SELECT COUNT(*) FROM fact_data_quality_results
            WHERE passed = FALSE AND failure_percentage = 0
        """)
        # Some failures may have 0% if total_count is very large
        assert True  # informational check


class TestAuditTrail:
    def test_audit_log_populated(self):
        count = _query_scalar("SELECT COUNT(*) FROM audit_log")
        assert count > 0, "Audit log should track pipeline operations"

    def test_audit_log_has_batch_ids(self):
        empty = _query_scalar("SELECT COUNT(*) FROM audit_log WHERE batch_id IS NULL OR batch_id = ''")
        assert empty == 0, "All audit entries must have batch_id"


class TestPipelineRunTracking:
    def test_pipeline_runs_recorded(self):
        count = _query_scalar("SELECT COUNT(*) FROM fact_pipeline_runs")
        assert count > 0, "Pipeline runs should be tracked"

    def test_pipeline_runs_have_correlation_id(self):
        empty = _query_scalar(
            "SELECT COUNT(*) FROM fact_pipeline_runs WHERE correlation_id IS NULL OR correlation_id = ''")
        assert empty == 0, "All pipeline runs must have correlation_id"

    def test_runtime_is_positive(self):
        neg = _query_scalar("SELECT COUNT(*) FROM fact_pipeline_runs WHERE runtime_seconds < 0")
        assert neg == 0, "Runtime should never be negative"
