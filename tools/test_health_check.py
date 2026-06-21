import sys
import unittest
from unittest.mock import patch

from tools import health_check


class RetryWithBackoffTests(unittest.TestCase):
    def setUp(self):
        health_check.LOGGER.disabled = True

    def tearDown(self):
        health_check.LOGGER.disabled = False

    def test_retries_critical_result_until_success(self):
        results = [
            ("CRITICAL", "temporary failure", 0),
            ("OK", "HTTP 200", 200),
        ]
        sleeps = []

        result = health_check.retry_with_backoff(
            lambda: results.pop(0),
            max_retries=2,
            backoff_factor=2.0,
            sleep_func=sleeps.append,
        )

        self.assertEqual(result, ("OK", "HTTP 200", 200))
        self.assertEqual(sleeps, [1.0])

    def test_exponential_backoff_uses_attempt_number(self):
        sleeps = []

        result = health_check.retry_with_backoff(
            lambda: ("CRITICAL", "still down", 0),
            max_retries=2,
            backoff_factor=3.0,
            sleep_func=sleeps.append,
        )

        self.assertEqual(result[0], "CRITICAL")
        self.assertEqual(sleeps, [1.0, 3.0])

    def test_does_not_retry_warning_result(self):
        attempts = []

        result = health_check.retry_with_backoff(
            lambda: attempts.append(1) or ("WARNING", "HTTP 404", 404),
            max_retries=3,
            sleep_func=lambda _: self.fail("WARNING should not retry"),
        )

        self.assertEqual(result[0], "WARNING")
        self.assertEqual(len(attempts), 1)


class CircuitBreakerTests(unittest.TestCase):
    def setUp(self):
        health_check.LOGGER.disabled = True

    def tearDown(self):
        health_check.LOGGER.disabled = False

    def test_opens_after_threshold_failures(self):
        now = [100.0]
        breaker = health_check.CircuitBreaker(
            failure_threshold=2,
            cooldown_seconds=10.0,
            clock=lambda: now[0],
        )

        breaker.record_failure()
        self.assertEqual(breaker.state, "CLOSED")
        self.assertTrue(breaker.allow_request())

        breaker.record_failure()
        self.assertEqual(breaker.state, "OPEN")
        self.assertFalse(breaker.allow_request())

    def test_half_open_after_cooldown_and_success_closes(self):
        now = [100.0]
        breaker = health_check.CircuitBreaker(
            failure_threshold=1,
            cooldown_seconds=10.0,
            clock=lambda: now[0],
        )

        breaker.record_failure()
        self.assertEqual(breaker.state, "OPEN")

        now[0] = 111.0
        self.assertEqual(breaker.state, "HALF_OPEN")
        self.assertTrue(breaker.allow_request())

        breaker.record_success()
        self.assertEqual(breaker.state, "CLOSED")
        self.assertEqual(breaker.consecutive_failures, 0)

    def test_check_http_service_skips_when_circuit_is_open(self):
        breaker = health_check.CircuitBreaker(failure_threshold=1, clock=lambda: 100.0)
        breaker.record_failure()

        result = health_check.check_http_service(
            "example.test",
            80,
            "/health",
            1,
            circuit_breaker=breaker,
        )

        self.assertEqual(result[0], "CRITICAL")
        self.assertIn("Circuit breaker open", result[1])


class HealthSummaryTests(unittest.TestCase):
    def setUp(self):
        health_check.LOGGER.disabled = True

    def tearDown(self):
        health_check.LOGGER.disabled = False

    def test_build_summary_counts_all_health_sections(self):
        results = {
            "services": {
                "backend": {"status": "OK", "detail": "up"},
                "frontend": {"status": "WARNING", "detail": "slow"},
            },
            "infrastructure": {
                "redis": {"status": "CRITICAL", "detail": "down"},
            },
            "system": {
                "disk": {"status": "OK", "detail": "fine"},
            },
        }

        summary = health_check._build_summary(results)

        self.assertEqual(summary["ok"], 2)
        self.assertEqual(summary["warning"], 1)
        self.assertEqual(summary["critical"], 1)
        self.assertEqual(summary["degraded"], 2)
        self.assertEqual(summary["total"], 4)

    def test_run_health_checks_includes_summary_and_degraded_status(self):
        with patch.object(health_check, "SERVICES", {}), \
             patch.object(health_check, "INFRASTRUCTURE", {}), \
             patch.object(health_check, "check_disk_usage", return_value=("OK", "disk ok", 1.0)), \
             patch.object(health_check, "check_memory_usage", return_value=("WARNING", "memory high", 85.0)), \
             patch.object(health_check, "check_load_average", return_value=("OK", "load ok", 0.1)):
            result = health_check.run_health_checks()

        self.assertEqual(result["overall_status"], "DEGRADED")
        self.assertEqual(result["summary"]["warning"], 1)


class CliTests(unittest.TestCase):
    def test_parse_retry_and_circuit_flags(self):
        argv = [
            "health_check.py",
            "--max-retries",
            "3",
            "--backoff-factor",
            "1.5",
            "--circuit-threshold",
            "2",
            "--circuit-cooldown",
            "7",
        ]

        with patch.object(sys, "argv", argv):
            args = health_check.parse_args()

        self.assertEqual(args.max_retries, 3)
        self.assertEqual(args.backoff_factor, 1.5)
        self.assertEqual(args.circuit_threshold, 2)
        self.assertEqual(args.circuit_cooldown, 7.0)


if __name__ == "__main__":
    unittest.main()
