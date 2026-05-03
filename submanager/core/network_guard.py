from __future__ import annotations

import socket
import ssl
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from submanager.config.models import NetworkGuardSettings, SentinelTarget
from submanager.core.models import NetworkStateSnapshot, NetworkTargetResult, ServiceState
from submanager.utils.logging import get_logger


logger = get_logger(__name__)


class NetworkGuard:
    def __init__(self, settings: NetworkGuardSettings, state: ServiceState) -> None:
        self.settings = settings
        self.state = state
        self.lock = threading.RLock()
        self.state.network = NetworkStateSnapshot(
            enabled=settings.enabled,
            online=not settings.enabled,
            status="checking" if settings.enabled else "disabled",
            failure_streak=0,
            recovery_streak=0,
            total_targets=len(settings.sentinel_targets),
        )

    def snapshot(self) -> NetworkStateSnapshot:
        with self.lock:
            current = self.state.network
            assert current is not None
            return self._copy_snapshot(current)

    def is_online(self) -> bool:
        if not self.settings.enabled:
            return True
        return self.snapshot().online

    def refresh(self) -> NetworkStateSnapshot:
        if not self.settings.enabled:
            return self.snapshot()
        successes = 0
        http_successes = 0
        last_error = ""
        target_results: list[NetworkTargetResult] = []
        total = len(self.settings.sentinel_targets)
        required_successes = min(total or 1, max(1, self.settings.minimum_successful_targets))
        if total == 0:
            successes = 1
            http_successes = 1
            total = 1
            required_successes = 1
            target_results.append(
                NetworkTargetResult(type="implicit", endpoint="no sentinel targets configured", ok=True, duration_ms=0)
            )
        else:
            for target in self.settings.sentinel_targets:
                target_started = time.monotonic()
                try:
                    self._check_target(target)
                    successes += 1
                    if target.type.lower().strip() == "http":
                        http_successes += 1
                    target_results.append(
                        NetworkTargetResult(
                            type=target.type.lower().strip(),
                            endpoint=self._target_endpoint(target),
                            ok=True,
                            duration_ms=int((time.monotonic() - target_started) * 1000),
                        )
                    )
                except Exception as exc:
                    last_error = str(exc)
                    target_results.append(
                        NetworkTargetResult(
                            type=target.type.lower().strip(),
                            endpoint=self._target_endpoint(target),
                            ok=False,
                            duration_ms=int((time.monotonic() - target_started) * 1000),
                            error=str(exc),
                        )
                    )
        has_required_successes = successes >= required_successes
        has_required_http = (not self.settings.require_http_success) or http_successes > 0
        probe_online = has_required_successes and has_required_http
        if not probe_online:
            reasons = []
            if not has_required_successes:
                reasons.append(f"successful targets {successes}/{total}, required {required_successes}")
            if not has_required_http:
                reasons.append("no HTTP sentinel succeeded")
            if last_error:
                reasons.append(f"last error: {last_error}")
            last_error = "; ".join(reasons)
        now = datetime.now(timezone.utc)
        with self.lock:
            current = self.state.network
            assert current is not None
            current.last_checked_at = now
            current.successful_targets = successes
            current.total_targets = total
            current.target_results = target_results
            was_online = current.online
            if probe_online:
                current.failure_streak = 0
                current.recovery_streak += 1
                current.last_success_at = now
                current.last_error = ""
                if was_online or current.recovery_streak >= self.settings.recovery_threshold:
                    current.online = True
                    current.status = "online"
                else:
                    current.status = "recovering"
                if not was_online and current.online:
                    current.last_changed_at = now
                    logger.warning("Network guard recovered: %s/%s targets succeeded", successes, total)
            else:
                current.recovery_streak = 0
                current.failure_streak += 1
                current.last_error = last_error
                if (not was_online) or current.failure_streak >= self.settings.failure_threshold:
                    current.online = False
                    current.status = "offline"
                else:
                    current.status = "degraded"
                if was_online and not current.online:
                    current.last_changed_at = now
                    logger.error("Network guard marked host offline: %s", last_error)
                elif current.last_changed_at is None:
                    current.last_changed_at = now
            return self._copy_snapshot(current)

    def _copy_snapshot(self, snapshot: NetworkStateSnapshot) -> NetworkStateSnapshot:
        return NetworkStateSnapshot(
            enabled=snapshot.enabled,
            online=snapshot.online,
            status=snapshot.status,
            failure_streak=snapshot.failure_streak,
            recovery_streak=snapshot.recovery_streak,
            last_checked_at=snapshot.last_checked_at,
            last_changed_at=snapshot.last_changed_at,
            last_success_at=snapshot.last_success_at,
            last_error=snapshot.last_error,
            successful_targets=snapshot.successful_targets,
            total_targets=snapshot.total_targets,
            target_results=list(snapshot.target_results),
        )

    def _target_endpoint(self, target: SentinelTarget) -> str:
        kind = target.type.lower().strip()
        if kind == "http":
            return target.url
        if kind in {"tcp", "dns"}:
            return f"{target.host}:{target.port or 53}"
        return target.host or target.url or target.type

    def _check_target(self, target: SentinelTarget) -> None:
        kind = target.type.lower().strip()
        if kind == "tcp":
            self._tcp_check(target.host, target.port)
            return
        if kind == "http":
            self._http_check(target.url)
            return
        if kind == "dns":
            self._dns_check(target.host, target.port or 53)
            return
        raise ValueError(f"unsupported sentinel target type: {target.type}")

    def _tcp_check(self, host: str, port: int) -> None:
        if not host or port <= 0:
            raise ValueError("invalid tcp sentinel target")
        with socket.create_connection((host, port), timeout=2.0):
            return

    def _dns_check(self, host: str, port: int) -> None:
        if not host:
            raise ValueError("invalid dns sentinel target")
        query = b"\xaa\xaa\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00\x07example\x03com\x00\x00\x01\x00\x01"
        with socket.create_connection((host, port), timeout=2.0) as sock:
            sock.settimeout(2.0)
            sock.sendall(len(query).to_bytes(2, "big") + query)
            size = sock.recv(2)
            if len(size) != 2:
                raise OSError("dns sentinel short read")
            response = sock.recv(int.from_bytes(size, "big"))
            if not response:
                raise OSError("dns sentinel empty response")

    def _http_check(self, url: str) -> None:
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("invalid http sentinel target")
        request = urllib.request.Request(url, headers={"User-Agent": "submanager/0.1", "Accept": "*/*"})
        context = ssl.create_default_context() if parsed.scheme == "https" else None
        with urllib.request.urlopen(request, timeout=3.0, context=context) as response:
            status = getattr(response, "status", 200)
            if "generate_204" in parsed.path and status != 204:
                raise OSError(f"unexpected HTTP status {status}, expected 204")
            if "generate_204" not in parsed.path and status not in {200, 204, 301, 302}:
                raise OSError(f"unexpected HTTP status {status}")
