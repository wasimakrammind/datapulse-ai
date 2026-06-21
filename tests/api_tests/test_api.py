"""API Regression Tests — validates all DataPulse AI endpoints."""
import pytest
from fastapi.testclient import TestClient
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from src.api.main import app
from config.settings import API_KEY

client = TestClient(app)
HEADERS = {"X-API-Key": API_KEY}


class TestHealthEndpoint:
    def test_health_returns_200(self):
        r = client.get("/health")
        assert r.status_code == 200

    def test_health_response_schema(self):
        r = client.get("/health")
        data = r.json()
        assert "status" in data
        assert "timestamp" in data
        assert "version" in data
        assert "database" in data

    def test_health_version(self):
        r = client.get("/health")
        assert r.json()["version"] == "1.0.0"


class TestAuthSecurity:
    def test_unauthorized_without_key(self):
        r = client.get("/pipeline-runs")
        assert r.status_code == 401

    def test_unauthorized_wrong_key(self):
        r = client.get("/pipeline-runs", headers={"X-API-Key": "wrong-key"})
        assert r.status_code == 401

    def test_authorized_with_valid_key(self):
        r = client.get("/pipeline-runs", headers=HEADERS)
        assert r.status_code == 200


class TestCorrelationID:
    def test_correlation_id_in_response(self):
        r = client.get("/health")
        assert "X-Correlation-ID" in r.headers

    def test_custom_correlation_id_echoed(self):
        r = client.get("/health", headers={"X-Correlation-ID": "test-123"})
        assert r.headers["X-Correlation-ID"] == "test-123"


class TestPipelineRunsEndpoint:
    def test_returns_paginated(self):
        r = client.get("/pipeline-runs", headers=HEADERS)
        data = r.json()
        assert "data" in data
        assert "page" in data
        assert "page_size" in data
        assert "total_count" in data
        assert "total_pages" in data

    def test_pagination_params(self):
        r = client.get("/pipeline-runs?page=1&page_size=5", headers=HEADERS)
        assert r.status_code == 200
        assert r.json()["page_size"] == 5

    def test_filter_by_status(self):
        r = client.get("/pipeline-runs?status=success", headers=HEADERS)
        assert r.status_code == 200


class TestDataQualityEndpoint:
    def test_returns_paginated(self):
        r = client.get("/data-quality-results", headers=HEADERS)
        assert r.status_code == 200
        data = r.json()
        assert "data" in data

    def test_filter_by_severity(self):
        r = client.get("/data-quality-results?severity=critical", headers=HEADERS)
        assert r.status_code == 200

    def test_filter_by_passed(self):
        r = client.get("/data-quality-results?passed=false", headers=HEADERS)
        assert r.status_code == 200


class TestOrdersSummaryEndpoint:
    def test_returns_summary(self):
        r = client.get("/orders/summary", headers=HEADERS)
        assert r.status_code == 200
        data = r.json()
        assert "summary" in data
        assert "by_channel" in data
        assert "by_region" in data


class TestInventoryAnomaliesEndpoint:
    def test_returns_anomalies(self):
        r = client.get("/inventory/anomalies", headers=HEADERS)
        assert r.status_code == 200
        data = r.json()
        assert "anomaly_count" in data
        assert "anomalies" in data
        assert "types" in data


class TestExecutionMissesEndpoint:
    def test_returns_misses(self):
        r = client.get("/reports/execution-misses", headers=HEADERS)
        assert r.status_code == 200
        data = r.json()
        assert "total_misses" in data
        assert "failed_runs" in data
        assert "sla_breaches" in data


class TestQuarantineEndpoint:
    def test_returns_paginated(self):
        r = client.get("/quarantine/records", headers=HEADERS)
        assert r.status_code == 200

    def test_filter_by_source(self):
        r = client.get("/quarantine/records?source_table=raw_orders", headers=HEADERS)
        assert r.status_code == 200


class TestAuditLogEndpoint:
    def test_returns_paginated(self):
        r = client.get("/audit-log", headers=HEADERS)
        assert r.status_code == 200


class TestOpenAPISpec:
    def test_docs_accessible(self):
        r = client.get("/docs")
        assert r.status_code == 200

    def test_openapi_json(self):
        r = client.get("/openapi.json")
        assert r.status_code == 200
        data = r.json()
        assert data["info"]["title"] == "DataPulse AI"


class TestIdempotency:
    def test_repeated_get_same_result(self):
        r1 = client.get("/orders/summary", headers=HEADERS)
        r2 = client.get("/orders/summary", headers=HEADERS)
        assert r1.json() == r2.json()
