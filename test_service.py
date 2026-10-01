import logging
import unittest
from unittest import mock

from fastapi.testclient import TestClient

import verdicts_api
from service import _HideHealthAccessLog


class HealthAccessFilterTests(unittest.TestCase):
    def test_only_health_access_record_is_hidden(self):
        access_filter = _HideHealthAccessLog()
        health = logging.LogRecord(
            "uvicorn.access", logging.INFO, __file__, 1,
            '%s - "%s %s HTTP/%s" %d',
            ("10.0.0.1:123", "GET", "/health", "1.1", 200), None,
        )
        baseline = logging.LogRecord(
            "uvicorn.access", logging.INFO, __file__, 1,
            '%s - "%s %s HTTP/%s" %d',
            ("10.0.0.1:123", "GET", "/baseline/clean", "1.1", 200), None,
        )
        unhealthy = logging.LogRecord(
            "uvicorn.access", logging.INFO, __file__, 1,
            '%s - "%s %s HTTP/%s" %d',
            ("10.0.0.1:123", "GET", "/health", "1.1", 503), None,
        )
        self.assertFalse(access_filter.filter(health))
        self.assertTrue(access_filter.filter(baseline))
        self.assertTrue(access_filter.filter(unhealthy))

    def test_api_only_health_is_always_ready(self):
        with mock.patch.object(verdicts_api, "API_ONLY", True):
            response = TestClient(verdicts_api.app).get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "maintenance", "api_only": True})


if __name__ == "__main__":
    unittest.main()
