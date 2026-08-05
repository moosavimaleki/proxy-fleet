from __future__ import annotations

from datetime import datetime, timedelta


def should_suppress_candidate_failures(
    *,
    guard_enabled: bool,
    network_online: bool,
    candidate_count: int,
    success_count: int,
    threshold_percent: int,
) -> bool:
    """Protect node state only when the host network is actually offline."""
    if not guard_enabled or network_online or candidate_count < 10 or success_count > 0:
        return False
    failure_percent = ((candidate_count - success_count) * 100.0) / max(1, candidate_count)
    return threshold_percent > 0 and failure_percent >= threshold_percent


def candidate_retry_at(
    *,
    failure_count: int,
    backoff_seconds: list[int],
    now: datetime,
) -> datetime:
    if failure_count <= 0:
        return now
    index = min(failure_count - 1, len(backoff_seconds) - 1)
    return now + timedelta(seconds=backoff_seconds[index])


def download_revalidation_due(
    *,
    last_download_test_at: datetime | None,
    interval_seconds: int,
    now: datetime,
) -> bool:
    if last_download_test_at is None:
        return True
    return last_download_test_at <= now - timedelta(seconds=interval_seconds)
