from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from submanager.core.scheduling import candidate_retry_at, download_revalidation_due, should_suppress_candidate_failures


class CandidateFailurePolicyTests(unittest.TestCase):
    def test_mass_failure_is_not_suppressed_while_host_network_is_online(self) -> None:
        self.assertFalse(
            should_suppress_candidate_failures(
                guard_enabled=True,
                network_online=True,
                candidate_count=64,
                success_count=0,
                threshold_percent=40,
            )
        )

    def test_mass_failure_is_suppressed_when_guard_confirms_host_is_offline(self) -> None:
        self.assertTrue(
            should_suppress_candidate_failures(
                guard_enabled=True,
                network_online=False,
                candidate_count=64,
                success_count=0,
                threshold_percent=40,
            )
        )

    def test_candidate_retry_uses_progressive_backoff_and_caps_at_last_bucket(self) -> None:
        now = datetime.now(timezone.utc)
        self.assertEqual(
            now + timedelta(seconds=300),
            candidate_retry_at(failure_count=1, backoff_seconds=[300, 1800], now=now),
        )
        self.assertEqual(
            now + timedelta(seconds=1800),
            candidate_retry_at(failure_count=9, backoff_seconds=[300, 1800], now=now),
        )

    def test_download_revalidation_becomes_due_after_interval(self) -> None:
        now = datetime.now(timezone.utc)
        self.assertTrue(
            download_revalidation_due(last_download_test_at=None, interval_seconds=300, now=now)
        )
        self.assertFalse(
            download_revalidation_due(
                last_download_test_at=now - timedelta(seconds=299),
                interval_seconds=300,
                now=now,
            )
        )
        self.assertTrue(
            download_revalidation_due(
                last_download_test_at=now - timedelta(seconds=300),
                interval_seconds=300,
                now=now,
            )
        )


if __name__ == "__main__":
    unittest.main()
