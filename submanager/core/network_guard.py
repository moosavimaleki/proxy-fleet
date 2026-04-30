from __future__ import annotations

import socket
import ssl
import threading
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from submanager.config.models import NetworkGuardSettings, SentinelTarget
from submanager.core.models import NetworkStateSnapshot, ServiceState
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
            return NetworkStateSnapshot(**current.__dict__)

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
        total = len(self.settings.sentinel_targets)
        required_successes = min(total or 1, max(1, self.settings.minimum_successful_targets))
        if total == 0:
            successes = 1
            http_successes = 1
            total = 1
            required_successes = 1
        else:
            for target in self.settings.sentinel_targets:
                try:
                    self._check_target(target)
                    successes += 1
                    if target.type.lower().strip() == "http":
                        http_successes += 1
                except Exception as exc:
                    last_error = str(exc)
        has_required_successes = successes >= required_successes
        has_required_http = (not self.settings.require_http_success) or http_successes > 0
        online_now = has_required_successes and has_required_http
        if not online_now:
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
            was_online = current.online
            if online_now:
                current.failure_streak = 0
                current.recovery_streak += 1
                current.last_success_at = now
                current.last_error = ""
                current.online = True
                current.status = "online"
                if not was_online:
                    current.last_changed_at = now
                    logger.warning("Network guard recovered: %s/%s targets succeeded", successes, total)
            else:
                current.recovery_streak = 0
                current.failure_streak += 1
                current.last_error = last_error
                current.online = False
                current.status = "offline"
                if was_online:
                    current.last_changed_at = now
                    logger.error("Network guard marked host offline: %s", last_error)
                elif current.last_changed_at is None:
                    current.last_changed_at = now
            return NetworkStateSnapshot(**current.__dict__)

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
