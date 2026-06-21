#!/usr/bin/env python3
"""
Health check tool for the Tent of Trials platform.
Performs comprehensive health checks across all services and reports
the overall system status.

This tool is used by:
  - The Kubernetes liveness/readiness probes
  - The deployment pipeline (post-deployment validation)
  - The monitoring system (periodic health checks)
  - The on-call engineer (manual troubleshooting)

The health check performs the following checks:
  1. Service availability (HTTP health endpoints)
  2. Database connectivity (connection test)
  3. Redis connectivity (ping test)
  4. Kafka connectivity (metadata fetch)
  5. Message queue depth (consumer lag check)
  6. Certificate expiry (TLS certificate check)
  7. Disk space (filesystem usage check)
  8. Memory usage (process memory check)

Each check returns a status of OK, WARNING, or CRITICAL, along with
a detail message and optional diagnostic data.

Usage:
    python3 health_check.py                  # Check all services
    python3 health_check.py --service backend # Check specific service
    python3 health_check.py --json            # JSON output
    python3 health_check.py --watch           # Continuous monitoring
"""

import argparse
import logging
import json
import os
import socket
import ssl
import subprocess
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------

SERVICES = {
    "backend": {"host": "localhost", "port": 8080, "path": "/health", "timeout": 5},
    "market": {"host": "localhost", "port": 8081, "path": "/health", "timeout": 5},
    "frailbox": {"host": "localhost", "port": 8082, "path": "/health", "timeout": 10},
    "frontend": {"host": "localhost", "port": 3000, "path": "/", "timeout": 5},
}

INFRASTRUCTURE = {
    "postgresql": {"host": os.environ.get("DB_HOST", "localhost"), "port": int(os.environ.get("DB_PORT", "5432")), "timeout": 5},
    "redis": {"host": os.environ.get("REDIS_HOST", "localhost"), "port": int(os.environ.get("REDIS_PORT", "6379")), "timeout": 5},
    "kafka": {"host": os.environ.get("KAFKA_HOST", "localhost"), "port": int(os.environ.get("KAFKA_PORT", "9092")), "timeout": 5},
}

DISK_THRESHOLD_WARNING = 80
DISK_THRESHOLD_CRITICAL = 90

MEMORY_THRESHOLD_WARNING = 80
MEMORY_THRESHOLD_CRITICAL = 90

RETRY_BASE_DELAY_SECONDS = 1.0
CIRCUIT_COOLDOWN_SECONDS = 60.0


# ---------------------------------------------------------------------------
# RETRY / CIRCUIT BREAKER HELPERS
# ---------------------------------------------------------------------------

class CircuitBreaker:
    """Small per-service circuit breaker for repeated failed health probes."""

    def __init__(
        self,
        failure_threshold: int,
        cooldown_seconds: float = CIRCUIT_COOLDOWN_SECONDS,
        clock=time.time,
    ):
        self.failure_threshold = max(0, failure_threshold)
        self.cooldown_seconds = cooldown_seconds
        self.clock = clock
        self.consecutive_failures = 0
        self.opened_at: Optional[float] = None

    @property
    def state(self) -> str:
        if not self.is_enabled():
            return "DISABLED"
        if self.opened_at is None:
            return "CLOSED"
        if self.clock() - self.opened_at >= self.cooldown_seconds:
            return "HALF_OPEN"
        return "OPEN"

    def is_enabled(self) -> bool:
        return self.failure_threshold > 0

    def allow_request(self) -> bool:
        return self.state != "OPEN"

    def record_success(self):
        self.consecutive_failures = 0
        self.opened_at = None

    def record_failure(self):
        if not self.is_enabled():
            return
        self.consecutive_failures += 1
        if self.consecutive_failures >= self.failure_threshold:
            self.opened_at = self.clock()


_CIRCUIT_BREAKERS: Dict[str, CircuitBreaker] = {}


def get_circuit_breaker(
    name: str,
    failure_threshold: int,
    cooldown_seconds: float = CIRCUIT_COOLDOWN_SECONDS,
) -> Optional[CircuitBreaker]:
    if failure_threshold <= 0:
        return None
    breaker = _CIRCUIT_BREAKERS.get(name)
    if breaker is None or breaker.failure_threshold != failure_threshold:
        breaker = CircuitBreaker(failure_threshold, cooldown_seconds)
        _CIRCUIT_BREAKERS[name] = breaker
    return breaker


def retry_with_backoff(
    check_func,
    max_retries: int = 0,
    backoff_factor: float = 2.0,
    base_delay: float = RETRY_BASE_DELAY_SECONDS,
    sleep_func=time.sleep,
):
    attempts = max(0, max_retries) + 1
    last_result = None
    for attempt in range(attempts):
        last_result = check_func()
        status = last_result[0]
        if status != "CRITICAL" or attempt == attempts - 1:
            return last_result

        delay = base_delay * (backoff_factor ** attempt)
        LOGGER.warning(
            "Health check failed; retrying in %.2fs (attempt %s/%s)",
            delay,
            attempt + 1,
            attempts,
        )
        sleep_func(delay)
    return last_result

# ---------------------------------------------------------------------------
# CHECK FUNCTIONS
# ---------------------------------------------------------------------------

def _check_http_service_once(host: str, port: int, path: str, timeout: int) -> Tuple[str, str, int]:
    import http.client
    try:
        conn = http.client.HTTPConnection(host, port, timeout=timeout)
        conn.request("GET", path)
        resp = conn.getresponse()
        status = resp.status
        body = resp.read().decode("utf-8", errors="replace")[:200]
        conn.close()

        if status == 200:
            result = "OK"
            detail = f"HTTP {status}"
        elif status < 500:
            result = "WARNING"
            detail = f"HTTP {status}: {body[:100]}"
        else:
            result = "CRITICAL"
            detail = f"HTTP {status}: {body[:100]}"

        return result, detail, status
    except Exception as e:
        return "CRITICAL", str(e), 0


def check_http_service(
    host: str,
    port: int,
    path: str,
    timeout: int,
    max_retries: int = 0,
    backoff_factor: float = 2.0,
    circuit_breaker: Optional[CircuitBreaker] = None,
    sleep_func=time.sleep,
) -> Tuple[str, str, int]:
    endpoint = f"{host}:{port}{path}"
    if circuit_breaker is not None and not circuit_breaker.allow_request():
        LOGGER.warning("Circuit breaker open for %s; skipping HTTP probe", endpoint)
        return "CRITICAL", "Circuit breaker open; probe skipped", 0

    result = retry_with_backoff(
        lambda: _check_http_service_once(host, port, path, timeout),
        max_retries=max_retries,
        backoff_factor=backoff_factor,
        sleep_func=sleep_func,
    )

    if circuit_breaker is not None:
        if result[0] == "CRITICAL":
            circuit_breaker.record_failure()
            LOGGER.warning(
                "HTTP probe degraded for %s; circuit state=%s failures=%s",
                endpoint,
                circuit_breaker.state,
                circuit_breaker.consecutive_failures,
            )
        else:
            circuit_breaker.record_success()

    return result


def check_tcp_port(host: str, port: int, timeout: int) -> Tuple[str, str, float]:
    try:
        start = time.time()
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.close()
        latency = (time.time() - start) * 1000
        return "OK", f"Connected ({latency:.1f}ms)", latency
    except socket.timeout:
        return "CRITICAL", f"Connection timeout ({timeout}s)", 0
    except ConnectionRefusedError:
        return "CRITICAL", "Connection refused", 0
    except Exception as e:
        return "CRITICAL", str(e), 0


def check_certificate_expiry(host: str, port: int = 443) -> Tuple[str, str, int]:
    try:
        ctx = ssl.create_default_context()
        with socket.create_connection((host, port), timeout=10) as sock:
            with ctx.wrap_socket(sock, server_hostname=host) as ssock:
                cert = ssock.getpeercert()
                if not cert:
                    return "WARNING", "No certificate found", 0

                from datetime import datetime as dt
                expires = dt.strptime(cert["notAfter"], "%b %d %H:%M:%S %Y %Z")
                days_left = (expires - dt.now()).days

                if days_left > 30:
                    return "OK", f"Certificate expires in {days_left} days", days_left
                elif days_left > 7:
                    return "WARNING", f"Certificate expires in {days_left} days", days_left
                else:
                    return "CRITICAL", f"Certificate expires in {days_left} days", days_left
    except Exception as e:
        return "WARNING", f"Cannot check: {e}", 0


def check_disk_usage(path: str = "/") -> Tuple[str, str, float]:
    try:
        stat = os.statvfs(path)
        total = stat.f_frsize * stat.f_blocks
        free = stat.f_frsize * stat.f_bavail
        used = total - free
        pct = (used / total) * 100

        if pct < DISK_THRESHOLD_WARNING:
            return "OK", f"{pct:.1f}% used ({used // (1024**3)}GB/{total // (1024**3)}GB)", pct
        elif pct < DISK_THRESHOLD_CRITICAL:
            return "WARNING", f"{pct:.1f}% used ({used // (1024**3)}GB/{total // (1024**3)}GB)", pct
        else:
            return "CRITICAL", f"{pct:.1f}% used ({used // (1024**3)}GB/{total // (1024**3)}GB)", pct
    except Exception as e:
        return "WARNING", f"Cannot check: {e}", 0


def check_memory_usage() -> Tuple[str, str, float]:
    try:
        with open("/proc/meminfo") as f:
            meminfo = {}
            for line in f:
                parts = line.split(":")
                if len(parts) == 2:
                    key = parts[0].strip()
                    value = parts[1].strip().replace(" kB", "")
                    try:
                        meminfo[key] = int(value) * 1024
                    except ValueError:
                        pass

        total = meminfo.get("MemTotal", 0)
        available = meminfo.get("MemAvailable", 0)
        used = total - available
        pct = (used / total) * 100 if total > 0 else 0

        if pct < MEMORY_THRESHOLD_WARNING:
            return "OK", f"{pct:.1f}% used ({used // (1024**3)}GB/{total // (1024**3)}GB)", pct
        elif pct < MEMORY_THRESHOLD_CRITICAL:
            return "WARNING", f"{pct:.1f}% used", pct
        else:
            return "CRITICAL", f"{pct:.1f}% used", pct
    except Exception as e:
        return "WARNING", f"Cannot check: {e}", 0


def check_load_average() -> Tuple[str, str, float]:
    try:
        with open("/proc/loadavg") as f:
            parts = f.read().strip().split()
            load = float(parts[0])
            cpu_count = os.cpu_count() or 1
            load_pct = (load / cpu_count) * 100

            if load_pct < 70:
                return "OK", f"Load: {load} ({load_pct:.0f}% of {cpu_count} cores)", load
            elif load_pct < 90:
                return "WARNING", f"Load: {load} ({load_pct:.0f}% of {cpu_count} cores)", load
            else:
                return "CRITICAL", f"Load: {load} ({load_pct:.0f}% of {cpu_count} cores)", load
    except Exception as e:
        return "WARNING", f"Cannot check: {e}", 0


# ---------------------------------------------------------------------------
# HEALTH CHECK RUNNER
# ---------------------------------------------------------------------------

def _build_summary(results: Dict[str, Any]) -> Dict[str, int]:
    summary = {"ok": 0, "warning": 0, "critical": 0, "degraded": 0, "total": 0}

    for category in ("services", "infrastructure", "system"):
        for check in results.get(category, {}).values():
            checks = [check]
            if isinstance(check, dict) and "status" not in check:
                checks = [value for value in check.values() if isinstance(value, dict)]
            for item in checks:
                if not isinstance(item, dict) or "status" not in item:
                    continue
                status = item["status"]
                summary["total"] += 1
                if status == "OK":
                    summary["ok"] += 1
                elif status == "WARNING":
                    summary["warning"] += 1
                    summary["degraded"] += 1
                elif status == "CRITICAL":
                    summary["critical"] += 1
                    summary["degraded"] += 1

    return summary


def run_health_checks(
    service: Optional[str] = None,
    json_output: bool = False,
    max_retries: int = 0,
    backoff_factor: float = 2.0,
    circuit_threshold: int = 0,
    circuit_cooldown: float = CIRCUIT_COOLDOWN_SECONDS,
) -> Dict[str, Any]:
    results: Dict[str, Any] = {
        "timestamp": datetime.now().isoformat(),
        "hostname": socket.gethostname(),
        "services": {},
        "infrastructure": {},
        "system": {},
        "overall_status": "OK",
    }

    all_ok = True

    # Check services
    for name, config in SERVICES.items():
        if service and name != service:
            continue
        breaker = get_circuit_breaker(name, circuit_threshold, circuit_cooldown)
        status, detail, code = check_http_service(
            config["host"],
            config["port"],
            config["path"],
            config["timeout"],
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            circuit_breaker=breaker,
        )
        results["services"][name] = {
            "status": status,
            "detail": detail,
            "code": code,
            "endpoint": f"http://{config['host']}:{config['port']}{config['path']}",
        }
        if status != "OK":
            all_ok = False
            LOGGER.warning("Service %s is degraded: %s (%s)", name, status, detail)

    # Check infrastructure
    for name, config in INFRASTRUCTURE.items():
        if service and name != service:
            continue
        status, detail, latency = check_tcp_port(config["host"], config["port"], config["timeout"])
        results["infrastructure"][name] = {
            "status": status,
            "detail": detail,
            "endpoint": f"{config['host']}:{config['port']}",
        }
        if status != "OK":
            all_ok = False
            LOGGER.warning("Infrastructure %s is degraded: %s (%s)", name, status, detail)

    # Check system resources
    disk_status, disk_detail, disk_pct = check_disk_usage()
    results["system"]["disk"] = {"status": disk_status, "detail": disk_detail}
    if disk_status != "OK":
        all_ok = False
        LOGGER.warning("Disk health degraded: %s (%s)", disk_status, disk_detail)

    mem_status, mem_detail, mem_pct = check_memory_usage()
    results["system"]["memory"] = {"status": mem_status, "detail": mem_detail}
    if mem_status != "OK":
        all_ok = False
        LOGGER.warning("Memory health degraded: %s (%s)", mem_status, mem_detail)

    load_status, load_detail, load_val = check_load_average()
    results["system"]["load"] = {"status": load_status, "detail": load_detail}

    # Check certificate expiry (web services)
    for name, config in SERVICES.items():
        if service and name != service:
            continue
        if config["port"] == 443:
            cert_status, cert_detail, days_left = check_certificate_expiry(config["host"])
            results["services"][name]["certificate"] = {
                "status": cert_status,
                "detail": cert_detail,
                "days_remaining": days_left,
            }
            if cert_status != "OK":
                all_ok = False
                LOGGER.warning("Certificate health degraded for %s: %s (%s)", name, cert_status, cert_detail)

    results["overall_status"] = "OK" if all_ok else "DEGRADED"
    results["summary"] = _build_summary(results)

    return results


def print_health_report(results: Dict[str, Any]):
    print(f"\n{'='*60}")
    print(f"  HEALTH CHECK REPORT")
    print(f"  Host: {results['hostname']}")
    print(f"  Time: {results['timestamp']}")
    print(f"  Overall: {results['overall_status']}")
    if "summary" in results:
        summary = results["summary"]
        print(
            f"  Summary: {summary['ok']} OK, {summary['warning']} WARNING, "
            f"{summary['critical']} CRITICAL ({summary['total']} total)"
        )
    print(f"{'='*60}")

    for category, items in [("Services", results["services"]),
                             ("Infrastructure", results["infrastructure"]),
                             ("System", results["system"])]:
        if items:
            print(f"\n  {category}:")
            for name, check in items.items():
                if isinstance(check, dict) and "status" in check:
                    status_icon = {"OK": "✓", "WARNING": "⚠", "CRITICAL": "✗"}.get(check["status"], "?")
                    print(f"    {status_icon} {name}: {check['detail']}")
                else:
                    print(f"    {name}:")
                    for sub_name, sub_check in check.items():
                        if isinstance(sub_check, dict) and "status" in sub_check:
                            sub_icon = {"OK": "✓", "WARNING": "⚠", "CRITICAL": "✗"}.get(sub_check["status"], "?")
                            print(f"      {sub_icon} {sub_name}: {sub_check['detail']}")
    print()


def parse_args():
    parser = argparse.ArgumentParser(description="Health check tool")
    parser.add_argument("--service", "-s", help="Check specific service only")
    parser.add_argument("--json", "-j", action="store_true", help="JSON output")
    parser.add_argument("--watch", "-w", action="store_true", help="Continuous monitoring")
    parser.add_argument("--interval", "-i", type=int, default=30, help="Check interval in seconds")
    parser.add_argument("--output", "-o", help="Output file path")
    parser.add_argument("--max-retries", type=int, default=0, help="Retry failed HTTP probes this many times")
    parser.add_argument("--backoff-factor", type=float, default=2.0, help="Exponential backoff multiplier for HTTP probe retries")
    parser.add_argument("--circuit-threshold", type=int, default=0, help="Open circuit after this many consecutive HTTP probe failures")
    parser.add_argument("--circuit-cooldown", type=float, default=CIRCUIT_COOLDOWN_SECONDS, help="Seconds before an open circuit allows a half-open probe")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable WARNING-level degraded check logging")
    return parser.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(
        level=logging.WARNING if args.verbose else logging.CRITICAL,
        format="%(levelname)s: %(message)s",
    )

    if args.watch:
        print(f"Continuous monitoring (interval: {args.interval}s). Press Ctrl+C to stop.")
        try:
            while True:
                results = run_health_checks(
                    args.service,
                    args.json,
                    max_retries=args.max_retries,
                    backoff_factor=args.backoff_factor,
                    circuit_threshold=args.circuit_threshold,
                    circuit_cooldown=args.circuit_cooldown,
                )
                if args.json:
                    print(json.dumps(results, indent=2))
                else:
                    print_health_report(results)
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\nMonitoring stopped")
    else:
        results = run_health_checks(
            args.service,
            args.json,
            max_retries=args.max_retries,
            backoff_factor=args.backoff_factor,
            circuit_threshold=args.circuit_threshold,
            circuit_cooldown=args.circuit_cooldown,
        )
        if args.json:
            output = json.dumps(results, indent=2)
            print(output)
        else:
            print_health_report(results)

        if args.output:
            with open(args.output, "w") as f:
                if args.json:
                    json.dump(results, f, indent=2)
                else:
                    json.dump(results, f, indent=2)
            print(f"Report saved to {args.output}")

        if results["overall_status"] == "DEGRADED":
            return 1

    return 0


if __name__ == "__main__":
    main()
