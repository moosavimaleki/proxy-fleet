from __future__ import annotations

import time
import unittest
from types import SimpleNamespace

from submanager.core.models import ParsedNode
from submanager.testing.probes import UnexpectedHttpStatusError
from submanager.testing.service import TestService


class _SlowFailingProbe:
    def __init__(self) -> None:
        self.timeouts: list[float] = []

    def measure_latency(self, _port, _url, timeout, **_kwargs):
        self.timeouts.append(timeout)
        time.sleep(timeout)
        raise TimeoutError("probe timed out")


class _InconclusiveDownloadProbe:
    def measure_speed_kbps(self, _port, _url, _timeout):
        raise UnexpectedHttpStatusError("HTTP/1.1 403 Forbidden")


class TestServiceBudgetTests(unittest.TestCase):
    def test_fallback_urls_share_one_relay_timeout_budget(self) -> None:
        service = TestService.__new__(TestService)
        service.settings = SimpleNamespace(
            health=SimpleNamespace(
                relay_timeout_ms=30,
                test_url="https://one.example/",
                fallback_urls=["https://two.example/", "https://three.example/"],
            )
        )
        service.http_probe = _SlowFailingProbe()

        started = time.perf_counter()
        with self.assertRaisesRegex(RuntimeError, "relay probe failed"):
            service._probe_relay_latency(12345)
        elapsed = time.perf_counter() - started

        self.assertLess(elapsed, 0.09)
        self.assertEqual(1, len(service.http_probe.timeouts))

    def test_unmeasured_download_does_not_mark_proxy_healthy(self) -> None:
        service = TestService.__new__(TestService)
        service.settings = SimpleNamespace(
            download_test=SimpleNamespace(
                timeout_seconds=1,
                per_url_timeout_seconds=0.5,
                min_download_kbps=100,
                test_url="https://one.example/file",
                fallback_urls=[],
            )
        )
        service.download_probe = _InconclusiveDownloadProbe()
        service._safe_fetch_exit_info = lambda _port: {}
        parsed = ParsedNode("", "", "", "vless", "example.com", 443, "", {}, {}, "hash")

        result = service._probe_download_result(parsed, 12345, False, 42)

        self.assertFalse(result.ok)
        self.assertEqual(0, result.download_kbps)
        self.assertIn("download mirrors inconclusive", result.error)


if __name__ == "__main__":
    unittest.main()
