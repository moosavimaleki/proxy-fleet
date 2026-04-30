from __future__ import annotations

import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from submanager.config.models import AppSettings
from submanager.core.feedback import FeedbackEngine
from submanager.core.network_guard import NetworkGuard
from submanager.core.models import BestNodeDecision, FeedbackInput, NodeRecord, NodeStatus, ParsedNode, ServiceState, VipSelectionDecision
from submanager.core.ports import PortManager
from submanager.core.runtime import RuntimeSupervisor
from submanager.parser import SubscriptionParser
from submanager.selection.engine import SelectionEngine
from submanager.storage.sqlite_store import SqliteStore
from submanager.testing.service import TestService
from submanager.testing.xray import XrayBinaryResolver, XrayRunner
from submanager.utils.logging import get_logger


logger = get_logger(__name__)
PROBATION_FAILURE_THRESHOLD = 2
PROBATION_SUCCESS_THRESHOLD = 2


class OrchestratorApp:
    def __init__(self, settings: AppSettings, work_dir: Path) -> None:
        self.settings = settings
        self.work_dir = work_dir
        self.stop_event = threading.Event()
        self.state = ServiceState()
        self.store = SqliteStore(settings.database.path)
        self.parser = SubscriptionParser()
        self.test_service = TestService(settings, work_dir / ".cache")
        xray_bin = XrayBinaryResolver().ensure(settings.xray_bin, work_dir / ".cache")
        self.runtime_supervisor = RuntimeSupervisor(self.state, XrayRunner(xray_bin))
        self.port_manager = PortManager(settings.ports.main, settings.ports.test)
        self.selection_engine = SelectionEngine(settings, self.store)
        self.feedback_engine = FeedbackEngine(settings, self.store)
        self.network_guard = NetworkGuard(settings.network_guard, self.state)
        self.transition_lock = threading.RLock()
        self.reload_lock = threading.Lock()
        self.threads: list[threading.Thread] = []

    def start(self) -> None:
        reset_count = self.store.reset_testing_nodes()
        if reset_count:
            logger.warning("Reset %s stale TESTING nodes back to CANDIDATE", reset_count)
        active_nodes = self.store.list_nodes_by_status(NodeStatus.ACTIVE)
        self.port_manager.reserve_existing_main([node.main_port for node in active_nodes if node.main_port])
        self._restore_active_runtimes(active_nodes)
        if self.settings.network_guard.enabled:
            self._spawn_thread("network-guard", self._network_guard_loop)
        self._spawn_thread("subscription-worker", self._subscription_loop)
        self._spawn_thread("candidate-worker", self._candidate_loop)
        self._spawn_thread("health-checker", self._health_loop)
        self._spawn_thread("dead-janitor", self._dead_janitor_loop)
        if self.settings.vip_port.enabled:
            self._spawn_thread("vip-manager", self._vip_loop)

    def stop(self) -> None:
        self.stop_event.set()
        for thread in self.threads:
            thread.join(timeout=2)
        for node_id in list(self.state.active_runtimes.keys()):
            self.runtime_supervisor.stop_active_runtime(node_id)
        self.runtime_supervisor.stop_vip_runtime()

    def get_best_node(self, client_id: str) -> BestNodeDecision | None:
        for _ in range(3):
            decision = self.selection_engine.select_best_node(client_id)
            if decision is None:
                return None
            if self.runtime_supervisor.is_running(decision.node_id):
                return decision
            node = self.store.get_node(decision.node_id)
            if node is not None:
                self._move_to_probation(node)
        return None

    def handle_feedback(self, feedback: FeedbackInput) -> None:
        self.feedback_engine.apply(feedback)

    def trigger_manual_test(self, node_id: str) -> dict[str, Any]:
        node = self.store.get_node(node_id)
        if node is None:
            raise KeyError(node_id)
        if not self._network_allows_work(force_refresh=True):
            started = datetime.now(timezone.utc)
            finished = started
            self.store.record_test_history(
                node_id=node.id,
                test_kind="full",
                trigger="manual",
                started_at=started,
                finished_at=finished,
                network_online=False,
                ok=False,
                latency_ms=None,
                download_kbps=None,
                error="host network offline",
                status_before=node.status.value,
                status_after=node.status.value,
                details={"skipped": True},
            )
            return {"ok": False, "error": "host network offline", "network_online": False}

        parsed = self.parser.parse_share_url(node.raw_config, node.source_subs[0] if node.source_subs else "manual")
        test_port = self.port_manager.allocate_test()
        if test_port is None:
            raise RuntimeError("No free test port available")
        started = datetime.now(timezone.utc)
        try:
            result = self.test_service.run_full_test(parsed, test_port)
        finally:
            self.port_manager.release_test(test_port)
        finished = datetime.now(timezone.utc)

        node.last_test_at = finished
        if result.latency_ms >= 0:
            node.relay_delay_ms = result.latency_ms
        if result.download_kbps is not None:
            node.download_kbps = result.download_kbps
        self.store.save_node(node)
        self.store.record_test_history(
            node_id=node.id,
            test_kind="full",
            trigger="manual",
            started_at=started,
            finished_at=finished,
            network_online=True,
            ok=result.ok,
            latency_ms=result.latency_ms if result.latency_ms >= 0 else None,
            download_kbps=result.download_kbps,
            error=result.error,
            status_before=node.status.value,
            status_after=node.status.value,
            details={"remark": parsed.remark, "protocol": parsed.protocol},
        )
        return {
            "ok": result.ok,
            "error": result.error,
            "network_online": True,
            "latency_ms": result.latency_ms if result.latency_ms >= 0 else None,
            "download_kbps": result.download_kbps,
            "finished_at": self._dt(finished),
        }

    def import_manual_configs(self, raw_text: str) -> dict[str, Any]:
        payload = (raw_text or "").strip()
        if not payload:
            return {"imported": 0, "accepted": 0, "warnings": ["empty input"]}
        if not self._network_allows_work(force_refresh=True):
            return {"imported": 0, "accepted": 0, "warnings": ["host network offline"]}

        links = self.parser.subscription_bytes_to_links(payload.encode("utf-8", errors="ignore"))
        if not links:
            links = [line.strip() for line in payload.splitlines() if line.strip()]

        warnings: list[str] = []
        accepted = 0
        for share_url in links:
            try:
                parsed = self.parser.parse_share_url(share_url, "manual://import")
                accepted += self._ingest_subscription_nodes([parsed])
            except Exception as exc:
                warnings.append(f"{share_url[:96]}... ({exc})")
        return {"imported": len(links), "accepted": accepted, "warnings": warnings[:50]}

    def clear_dead_pool(self) -> dict[str, Any]:
        removed = 0
        for node in self.store.list_nodes_by_status(NodeStatus.DEAD):
            with self.transition_lock:
                if self.state.vip_node_id == node.id:
                    self.runtime_supervisor.stop_vip_runtime()
                    self.state.vip_node_id = None
                    self.state.vip_score = None
                self.runtime_supervisor.stop_active_runtime(node.id)
                self.port_manager.release_main(node.main_port)
            self.store.delete_node(node.id)
            removed += 1
        return {"ok": True, "removed": removed}

    def reload_subscriptions_now(self) -> dict[str, Any]:
        with self.reload_lock:
            if self.state.subscription_reload_in_progress:
                return {
                    "ok": True,
                    "scheduled": False,
                    "in_progress": True,
                    "started_at": self._dt(self.state.subscription_reload_started_at),
                    "last_result": self.state.subscription_reload_last_result,
                }
            self.state.subscription_reload_in_progress = True
            self.state.subscription_reload_started_at = datetime.now(timezone.utc)
            self.state.subscription_reload_finished_at = None
        self._spawn_thread("subscription-reload", self._reload_subscriptions_worker)
        return {
            "ok": True,
            "scheduled": True,
            "in_progress": True,
            "started_at": self._dt(self.state.subscription_reload_started_at),
            "message": "Subscription reload scheduled in background.",
        }

    def cleanup_database(self) -> dict[str, Any]:
        result = self.store.cleanup_database()
        return {"ok": True, **result}

    def get_system_logs_payload(self, limit: int = 200, component: str = "", level: str = "") -> dict[str, Any]:
        rows = self.store.list_system_events(limit=limit, component=component, level=level)
        return {
            "events": [
                {
                    "id": row.id,
                    "created_at": self._dt(row.created_at),
                    "level": row.level,
                    "component": row.component,
                    "event": row.event,
                    "message": row.message,
                    "details": row.details,
                }
                for row in rows
            ]
        }

    def get_node_test_history_payload(self, node_id: str, limit: int = 50) -> dict[str, Any]:
        node = self.store.get_node(node_id)
        if node is None:
            raise KeyError(node_id)
        parsed = None
        try:
            parsed = self.parser.parse_share_url(node.raw_config, node.source_subs[0] if node.source_subs else "history")
        except Exception:
            parsed = None
        rows = self.store.list_test_history(node_id, limit=limit)
        return {
            "node": {
                "id": node.id,
                "status": node.status.value,
                "remark": parsed.remark if parsed is not None else "",
                "protocol": parsed.protocol if parsed is not None else node.normalized_config.get("protocol", ""),
                "server": parsed.address if parsed is not None else node.normalized_config.get("server", ""),
                "remote_port": parsed.port if parsed is not None else node.normalized_config.get("port", ""),
            },
            "history": [
                {
                    "id": row.id,
                    "test_kind": row.test_kind,
                    "trigger": row.trigger,
                    "started_at": self._dt(row.started_at),
                    "finished_at": self._dt(row.finished_at),
                    "network_online": row.network_online,
                    "ok": row.ok,
                    "latency_ms": row.latency_ms,
                    "download_kbps": row.download_kbps,
                    "error": row.error,
                    "status_before": row.status_before,
                    "status_after": row.status_after,
                    "details": row.details,
                }
                for row in rows
            ],
        }

    def get_dashboard_payload(self) -> dict[str, Any]:
        nodes = self.store.list_nodes()
        stats_by_node = self.store.list_dashboard_stats(self.settings.assignment_ttl_seconds)
        status_counts = Counter(node.status.value for node in nodes)
        payload_nodes = [self._serialize_dashboard_node(node, stats_by_node.get(node.id, {})) for node in nodes]
        return {
            "service": self.settings.service.name,
            "environment": self.settings.service.environment,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_nodes": len(payload_nodes),
            "status_counts": dict(status_counts),
            "network": self.get_network_status_payload(),
            "vip": self.get_vip_status_payload(),
            "nodes": payload_nodes,
        }

    def get_client_dashboard_payload(self, client_id: str | None = None) -> dict[str, Any]:
        nodes = self.store.list_nodes()
        known_clients = self.store.list_client_ids()
        selected_client = (client_id or "").strip() or (known_clients[0] if known_clients else "")
        client_stats_by_node = self.store.list_client_dashboard_stats(selected_client) if selected_client else {}
        payload_nodes = [self._serialize_client_dashboard_node(node, client_stats_by_node.get(node.id, {})) for node in nodes]
        return {
            "service": self.settings.service.name,
            "environment": self.settings.service.environment,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "selected_client": selected_client,
            "known_clients": known_clients,
            "total_nodes": len(payload_nodes),
            "network": self.get_network_status_payload(),
            "vip": self.get_vip_status_payload(),
            "nodes": payload_nodes,
        }

    def get_vip_status_payload(self) -> dict[str, Any]:
        current_node = self.store.get_node(self.state.vip_node_id) if self.state.vip_node_id else None
        return {
            "enabled": self.settings.vip_port.enabled,
            "port": self.settings.vip_port.port,
            "running": self.runtime_supervisor.is_vip_running(),
            "node_id": self.state.vip_node_id,
            "score": round(self.state.vip_score, 4) if self.state.vip_score is not None else None,
            "last_switched_at": self._dt(self.state.vip_last_switched_at),
            "node_status": current_node.status.value if current_node is not None else None,
            "relay_delay_ms": current_node.relay_delay_ms if current_node is not None else None,
            "download_kbps": current_node.download_kbps if current_node is not None else None,
        }

    def get_network_status_payload(self) -> dict[str, Any]:
        snapshot = self.network_guard.snapshot()
        return {
            "enabled": snapshot.enabled,
            "online": snapshot.online,
            "status": snapshot.status,
            "failure_streak": snapshot.failure_streak,
            "recovery_streak": snapshot.recovery_streak,
            "last_checked_at": self._dt(snapshot.last_checked_at),
            "last_changed_at": self._dt(snapshot.last_changed_at),
            "last_success_at": self._dt(snapshot.last_success_at),
            "last_error": snapshot.last_error,
            "successful_targets": snapshot.successful_targets,
            "total_targets": snapshot.total_targets,
            "minimum_successful_targets": self.settings.network_guard.minimum_successful_targets,
            "require_http_success": self.settings.network_guard.require_http_success,
        }

    def get_reload_status_payload(self) -> dict[str, Any]:
        return {
            "in_progress": self.state.subscription_reload_in_progress,
            "started_at": self._dt(self.state.subscription_reload_started_at),
            "finished_at": self._dt(self.state.subscription_reload_finished_at),
            "last_result": self.state.subscription_reload_last_result,
        }

    def _spawn_thread(self, name: str, target) -> None:
        thread = threading.Thread(name=name, target=target, daemon=True)
        thread.start()
        self.threads.append(thread)

    def _event(self, level: str, component: str, event: str, message: str, details: dict[str, object] | None = None) -> None:
        try:
            self.store.record_system_event(level, component, event, message, details)
        except Exception:
            logger.exception("Failed to record system event")

    def _reload_subscriptions_worker(self) -> None:
        self._event("info", "subscription", "reload_started", "Manual subscription reload started")
        result = {"ok": False, "reloaded_sources": 0, "accepted": 0, "warnings": []}
        try:
            if not self._network_allows_work(force_refresh=True):
                result = {"ok": False, "reloaded_sources": 0, "accepted": 0, "warnings": ["host network offline"]}
                self._event("warning", "subscription", "reload_skipped", "Manual subscription reload skipped because host network is offline")
                return
            accepted = 0
            warnings: list[str] = []
            reloaded_sources = 0
            for source in self.settings.subscriptions.urls:
                try:
                    parsed_nodes, source_warnings = self.parser.load_nodes(source.url)
                    warnings.extend(source_warnings)
                    reloaded_sources += 1
                    accepted_from_source = self._ingest_subscription_nodes(parsed_nodes)
                    accepted += accepted_from_source
                    if source_warnings:
                        self._event(
                            "warning",
                            "subscription",
                            "reload_source_warnings",
                            "Manual subscription reload parsed with warnings",
                            {"source": source.url, "accepted": accepted_from_source, "warnings": len(source_warnings)},
                        )
                except Exception:
                    if not self._network_allows_work(force_refresh=True):
                        warnings.append(f"{source.url}: host network offline during reload")
                        break
                    logger.exception("Failed to reload subscription %s", source.url)
                    self._event(
                        "error",
                        "subscription",
                        "reload_source_failed",
                        "Manual subscription source reload failed",
                        {"source": source.url},
                    )
                    warnings.append(f"{source.url}: reload failed")
            result = {
                "ok": True,
                "reloaded_sources": reloaded_sources,
                "accepted": accepted,
                "warnings": warnings[:100],
            }
        finally:
            with self.reload_lock:
                self.state.subscription_reload_in_progress = False
                self.state.subscription_reload_finished_at = datetime.now(timezone.utc)
                self.state.subscription_reload_last_result = result
            self._event(
                "info" if result.get("ok") else "warning",
                "subscription",
                "reload_finished",
                "Manual subscription reload finished",
                result,
            )

    def _serialize_dashboard_node(self, node: NodeRecord, stats: dict[str, Any]) -> dict[str, Any]:
        parsed = None
        try:
            parsed = self.parser.parse_share_url(node.raw_config, node.source_subs[0] if node.source_subs else "dashboard")
        except Exception:
            parsed = None
        normalized = node.normalized_config or {}
        return {
            "id": node.id,
            "config_hash": node.config_hash,
            "status": node.status.value,
            "protocol": parsed.protocol if parsed is not None else normalized.get("protocol", ""),
            "remark": parsed.remark if parsed is not None else "",
            "server": parsed.address if parsed is not None else normalized.get("server", ""),
            "remote_port": parsed.port if parsed is not None else normalized.get("port", ""),
            "runtime_running": self.runtime_supervisor.is_running(node.id),
            "runtime_port": self.runtime_supervisor.get_port(node.id),
            "is_vip": self.state.vip_node_id == node.id and self.runtime_supervisor.is_vip_running(),
            "main_port": node.main_port,
            "relay_delay_ms": node.relay_delay_ms,
            "download_kbps": node.download_kbps,
            "health_success_ewma": round(node.health_success_ewma, 4),
            "consecutive_relay_failures": node.consecutive_relay_failures,
            "open_assignments": stats.get("open_assignments", 0),
            "total_assignments": stats.get("total_assignments", 0),
            "used_count": stats.get("used_count", 0),
            "broken_count": stats.get("broken_count", 0),
            "rate_limited_count": stats.get("rate_limited_count", 0),
            "total_clients": stats.get("total_clients", 0),
            "open_clients": stats.get("open_clients", 0),
            "half_open_clients": stats.get("half_open_clients", 0),
            "closed_clients": stats.get("closed_clients", 0),
            "source_subs": node.source_subs,
            "created_at": self._dt(node.created_at),
            "updated_at": self._dt(node.updated_at),
            "last_health_check_at": self._dt(node.last_health_check_at),
            "last_test_at": self._dt(node.last_test_at),
            "dead_until": self._dt(node.dead_until),
            "last_assigned_at": stats.get("last_assigned_at"),
            "last_feedback_at": stats.get("last_feedback_at"),
            "last_client_assigned_at": stats.get("last_client_assigned_at"),
            "last_client_feedback_at": stats.get("last_client_feedback_at"),
            "normalized_config": normalized,
            "raw_config": node.raw_config,
        }

    def _serialize_client_dashboard_node(self, node: NodeRecord, stats: dict[str, Any]) -> dict[str, Any]:
        payload = self._serialize_dashboard_node(node, {})
        payload.update(
            {
                "client_state": stats.get("client_state", "UNSEEN"),
                "client_fail_streak": stats.get("fail_streak", 0),
                "client_rate_limit_streak": stats.get("rate_limit_streak", 0),
                "cooldown_until": stats.get("cooldown_until"),
                "usage_count": stats.get("usage_count", 0),
                "success_count": stats.get("success_count", 0),
                "broken_count": stats.get("broken_count", 0),
                "rate_limited_count": stats.get("rate_limited_count", 0),
                "recent_usage_score": round(float(stats.get("recent_usage_score", 0.0) or 0.0), 4),
                "success_rate_ewma": round(float(stats.get("success_rate_ewma", 0.0) or 0.0), 4),
                "client_total_assignments": stats.get("client_total_assignments", 0),
                "client_open_assignments": stats.get("client_open_assignments", 0),
                "client_used_feedback_count": stats.get("client_used_feedback_count", 0),
                "client_broken_feedback_count": stats.get("client_broken_feedback_count", 0),
                "client_rate_limited_feedback_count": stats.get("client_rate_limited_feedback_count", 0),
                "last_assigned_at": stats.get("latest_assigned_at") or stats.get("last_assigned_at"),
                "last_feedback_at": stats.get("latest_feedback_at") or stats.get("last_feedback_at"),
                "last_failure_at": stats.get("last_failure_at"),
                "last_success_at": stats.get("last_success_at"),
            }
        )
        return payload

    def _restore_active_runtimes(self, active_nodes: list[NodeRecord]) -> None:
        for node in active_nodes:
            if node.main_port is None:
                continue
            try:
                parsed = self.parser.parse_share_url(node.raw_config, node.source_subs[0] if node.source_subs else "restored")
                self.runtime_supervisor.start_active_runtime(node.id, parsed, node.main_port)
            except Exception:
                logger.exception("Failed to restore runtime for node %s", node.id)
                self._move_to_probation(node)

    def _network_guard_loop(self) -> None:
        while not self.stop_event.is_set():
            started = time.monotonic()
            try:
                snapshot = self.network_guard.refresh()
                self._event(
                    "info" if snapshot.online else "warning",
                    "network",
                    "sentinel_check",
                    f"Network sentinel check: {snapshot.status}",
                    {
                        "online": snapshot.online,
                        "successful_targets": snapshot.successful_targets,
                        "total_targets": snapshot.total_targets,
                        "minimum_successful_targets": self.settings.network_guard.minimum_successful_targets,
                        "require_http_success": self.settings.network_guard.require_http_success,
                        "failure_streak": snapshot.failure_streak,
                        "recovery_streak": snapshot.recovery_streak,
                        "duration_ms": int((time.monotonic() - started) * 1000),
                        "last_error": snapshot.last_error,
                    },
                )
            except Exception:
                logger.exception("Network guard check failed unexpectedly")
                self._event("error", "network", "sentinel_error", "Network guard check failed unexpectedly")
            self.stop_event.wait(self.settings.network_guard.check_interval_seconds)

    def _subscription_loop(self) -> None:
        probation_every = self.settings.health.probation_recheck_interval_seconds
        last_probation = datetime.min.replace(tzinfo=timezone.utc)
        while not self.stop_event.is_set():
            started = time.monotonic()
            if not self._network_allows_work():
                self._event("warning", "subscription", "cycle_skipped", "Subscription cycle skipped because host network is offline")
                self.stop_event.wait(self.settings.network_guard.check_interval_seconds if self.settings.network_guard.enabled else 1.0)
                continue
            reloaded_sources = 0
            accepted_total = 0
            warning_total = 0
            failed_sources = 0
            for source in self.settings.subscriptions.urls:
                try:
                    parsed_nodes, warnings = self.parser.load_nodes(source.url)
                    for warning in warnings:
                        logger.warning(warning)
                    accepted_from_source = self._ingest_subscription_nodes(parsed_nodes)
                    accepted_total += accepted_from_source
                    reloaded_sources += 1
                    warning_total += len(warnings)
                    if warnings:
                        self._event(
                            "warning",
                            "subscription",
                            "source_warnings",
                            "Subscription source parsed with warnings",
                            {"source": source.url, "accepted": accepted_from_source, "warnings": len(warnings)},
                        )
                except Exception:
                    if not self._network_allows_work(force_refresh=True):
                        logger.warning("Skipping subscription refresh while host is offline: %s", source.url)
                        self._event("warning", "subscription", "cycle_interrupted", "Subscription cycle interrupted because host network went offline", {"source": source.url})
                        break
                    logger.exception("Failed to process subscription %s", source.url)
                    self._event(
                        "error",
                        "subscription",
                        "source_failed",
                        "Subscription source refresh failed",
                        {"source": source.url},
                    )
                    failed_sources += 1
            now = datetime.now(timezone.utc)
            if (now - last_probation).total_seconds() >= probation_every:
                self._recheck_probation_nodes()
                self._promote_waiting_nodes()
                last_probation = now
            elapsed = time.monotonic() - started
            self._event(
                "info" if failed_sources == 0 else "warning",
                "subscription",
                "cycle_finished",
                "Subscription cycle finished",
                {
                    "reloaded_sources": reloaded_sources,
                    "accepted": accepted_total,
                    "warnings": warning_total,
                    "failed_sources": failed_sources,
                    "duration_ms": int(elapsed * 1000),
                    "next_run_seconds": max(0.0, self.settings.subscriptions.refresh_interval_seconds - elapsed),
                },
            )
            sleep_for = max(0.0, self.settings.subscriptions.refresh_interval_seconds - elapsed)
            self.stop_event.wait(sleep_for)

    def _candidate_loop(self) -> None:
        interval = self.settings.health.candidate_recheck_interval_seconds
        while not self.stop_event.is_set():
            started = time.monotonic()
            try:
                if self._network_allows_work():
                    result = self._recheck_candidate_nodes()
                    self._event(
                        "info",
                        "candidate",
                        "cycle_finished",
                        "Candidate worker cycle finished",
                        {**result, "duration_ms": int((time.monotonic() - started) * 1000), "next_run_seconds": interval},
                    )
                else:
                    self._event("warning", "candidate", "cycle_skipped", "Candidate worker skipped because host network is offline")
            except Exception:
                logger.exception("Candidate worker iteration failed")
                self._event("error", "candidate", "cycle_error", "Candidate worker iteration failed")
            elapsed = time.monotonic() - started
            self.stop_event.wait(max(0.0, interval - elapsed))

    def _health_loop(self) -> None:
        interval = self.settings.health.active_pool_relay_check_interval_seconds
        while not self.stop_event.is_set():
            started = time.monotonic()
            try:
                if self._network_allows_work():
                    result = self._check_active_nodes_batch()
                    self._event(
                        "info" if not result.get("failures") else "warning",
                        "health",
                        "active_check_finished",
                        "Active pool health check finished",
                        {**result, "duration_ms": int((time.monotonic() - started) * 1000), "next_run_seconds": interval},
                    )
                else:
                    self._event("warning", "health", "active_check_skipped", "Active pool health check skipped because host network is offline")
            except Exception:
                logger.exception("Active health checker iteration failed")
                self._event("error", "health", "active_check_error", "Active health checker iteration failed")
            self.stop_event.wait(interval)

    def _dead_janitor_loop(self) -> None:
        while not self.stop_event.is_set():
            removed = self.store.delete_expired_dead_nodes()
            if removed:
                logger.info("Removed %s expired dead nodes", removed)
            self._event("info", "dead-janitor", "cycle_finished", "Dead pool janitor cycle finished", {"removed": removed, "next_run_seconds": 300})
            self.stop_event.wait(300)

    def _vip_loop(self) -> None:
        interval = self.settings.vip_port.check_interval_seconds
        while not self.stop_event.is_set():
            started = time.monotonic()
            try:
                if self._network_allows_work():
                    self._maintain_vip_runtime()
                    self._event(
                        "info",
                        "vip",
                        "cycle_finished",
                        "VIP manager cycle finished",
                        {
                            "node_id": self.state.vip_node_id,
                            "score": self.state.vip_score,
                            "running": self.runtime_supervisor.is_vip_running(),
                            "duration_ms": int((time.monotonic() - started) * 1000),
                            "next_run_seconds": interval,
                        },
                    )
                else:
                    self._event("warning", "vip", "cycle_skipped", "VIP manager skipped because host network is offline")
            except Exception:
                logger.exception("VIP manager iteration failed")
                self._event("error", "vip", "cycle_error", "VIP manager iteration failed")
            self.stop_event.wait(interval)

    def _ingest_subscription_nodes(self, parsed_nodes: list[ParsedNode]) -> int:
        accepted = 0
        for parsed in parsed_nodes:
            existing = self.store.get_node_by_hash(parsed.config_hash) or self.store.get_node_by_raw_config(parsed.raw_config)
            if existing is not None:
                if existing.status == NodeStatus.DEAD and existing.dead_until and existing.dead_until <= datetime.now(timezone.utc):
                    self.store.delete_node(existing.id)
                else:
                    if parsed.source_url and parsed.source_url not in existing.source_subs:
                        existing.source_subs = sorted({*existing.source_subs, parsed.source_url})
                        self.store.save_node(existing)
                    continue
            record = self.store.create_or_merge_candidate(parsed)
            record.status = NodeStatus.CANDIDATE
            record.dead_until = None
            record.consecutive_relay_failures = 0
            record.consecutive_relay_successes = 0
            self.store.save_node(record)
            accepted += 1
        return accepted

    def _test_and_register_candidate(self, parsed: ParsedNode) -> None:
        if not self._network_allows_work():
            return
        with self.transition_lock:
            record = self.store.create_or_merge_candidate(parsed)
            record.status = NodeStatus.TESTING
            self.store.save_node(record)

        test_port = self.port_manager.allocate_test()
        if test_port is None:
            logger.warning("No free test port for candidate %s", parsed.config_hash)
            record.status = NodeStatus.CANDIDATE
            self.store.save_node(record)
            return

        started_at = datetime.now(timezone.utc)
        try:
            try:
                result = self.test_service.run_full_test(parsed, test_port)
            except Exception as exc:
                result = None
                logger.exception("Candidate test crashed for node %s", record.id)
                record.last_test_at = datetime.now(timezone.utc)
                record.status = NodeStatus.CANDIDATE if not self._network_allows_work(force_refresh=True) else NodeStatus.DEAD
                if record.status == NodeStatus.DEAD:
                    record.dead_until = datetime.now(timezone.utc) + timedelta(hours=self.settings.dead_pool.ttl_hours)
                self.store.save_node(record)
                self.store.record_test_history(
                    node_id=record.id,
                    test_kind="full",
                    trigger="candidate",
                    started_at=started_at,
                    finished_at=datetime.now(timezone.utc),
                    network_online=self._network_allows_work(),
                    ok=False,
                    latency_ms=None,
                    download_kbps=None,
                    error=str(exc),
                    status_before=NodeStatus.TESTING.value,
                    status_after=record.status.value,
                    details={"protocol": parsed.protocol, "remark": parsed.remark, "crashed": True},
                )
                return
        finally:
            self.port_manager.release_test(test_port)

        status_before = record.status.value
        record.last_test_at = datetime.now(timezone.utc)
        record.relay_delay_ms = result.latency_ms if result.latency_ms >= 0 else None
        record.download_kbps = result.download_kbps
        if result.ok:
            self._activate_node(record, parsed)
            self._record_test_result(record.id, "full", "candidate", started_at=started_at, result=result, status_before=status_before, status_after=record.status.value, details={"protocol": parsed.protocol, "remark": parsed.remark})
        else:
            if not self._network_allows_work(force_refresh=True):
                record.status = NodeStatus.CANDIDATE
                self.store.save_node(record)
                self._record_test_result(record.id, "full", "candidate", started_at=started_at, result=result, status_before=status_before, status_after=record.status.value, details={"suppressed_by_network_guard": True, "protocol": parsed.protocol, "remark": parsed.remark})
                return
            self._move_to_dead(record)
            self._record_test_result(record.id, "full", "candidate", started_at=started_at, result=result, status_before=status_before, status_after=record.status.value, details={"protocol": parsed.protocol, "remark": parsed.remark})

    def _activate_node(self, record: NodeRecord, parsed: ParsedNode) -> None:
        with self.transition_lock:
            main_port = self.port_manager.allocate_main()
            if main_port is None:
                record.status = NodeStatus.WAITING_FOR_PORT
                record.main_port = None
                self.store.save_node(record)
                return

            self.runtime_supervisor.start_active_runtime(record.id, parsed, main_port)
            record.status = NodeStatus.ACTIVE
            record.main_port = main_port
            record.consecutive_relay_failures = 0
            record.consecutive_relay_successes = 0
            record.health_success_ewma = self._ewma(record.health_success_ewma, 1.0)
            self.store.save_node(record)

    def _check_active_node(self, node: NodeRecord) -> None:
        if node.main_port is None:
            self._move_to_probation(node)
            return
        if not self.runtime_supervisor.is_running(node.id):
            self._move_to_probation(node)
            return
        try:
            latency = self.test_service.probe_running_port(node.main_port)
            node.relay_delay_ms = latency
            node.last_health_check_at = datetime.now(timezone.utc)
            node.consecutive_relay_failures = 0
            node.health_success_ewma = self._ewma(node.health_success_ewma, 1.0)
            self.store.save_node(node)
        except Exception:
            logger.exception("Active relay check failed for node %s", node.id)
            self._move_to_probation(node)

    def _recheck_probation_nodes(self) -> None:
        if not self._network_allows_work():
            return
        for node in self.store.list_nodes_by_status(NodeStatus.PROBATION):
            try:
                parsed = self.parser.parse_share_url(node.raw_config, node.source_subs[0] if node.source_subs else "probation")
            except Exception:
                logger.exception("Failed to parse probation node %s", node.id)
                self._mark_probation_failure(node)
                continue

            test_port = self.port_manager.allocate_test()
            if test_port is None:
                logger.warning("No free test port for probation node %s", node.id)
                return
            try:
                result = self.test_service.run_full_test(parsed, test_port)
            finally:
                self.port_manager.release_test(test_port)

            status_before = node.status.value
            finished_at = datetime.now(timezone.utc)
            node.last_test_at = finished_at
            node.relay_delay_ms = result.latency_ms if result.latency_ms >= 0 else None
            node.download_kbps = result.download_kbps
            if result.ok:
                self._mark_probation_success(node, parsed)
                self._record_test_result(node.id, "full", "probation", started_at=finished_at, result=result, status_before=status_before, status_after=node.status.value, details={"protocol": parsed.protocol, "remark": parsed.remark, "probation_successes": node.consecutive_relay_successes})
            else:
                if not self._network_allows_work(force_refresh=True):
                    self.store.save_node(node)
                    self._record_test_result(node.id, "full", "probation", started_at=finished_at, result=result, status_before=status_before, status_after=node.status.value, details={"suppressed_by_network_guard": True, "protocol": parsed.protocol, "remark": parsed.remark})
                    return
                self._mark_probation_failure(node)
                self._record_test_result(node.id, "full", "probation", started_at=finished_at, result=result, status_before=status_before, status_after=node.status.value, details={"protocol": parsed.protocol, "remark": parsed.remark, "probation_failures": node.consecutive_relay_failures})

    def _recheck_candidate_nodes(self) -> dict[str, Any]:
        summary: dict[str, Any] = {
            "skipped": False,
            "stale_testing_reset": 0,
            "candidates": 0,
            "batches": 0,
            "batch_size": self.settings.health.candidate_batch_size,
            "parallel_batches": self.settings.health.candidate_parallel_batches,
            "probe_concurrency": self.settings.health.candidate_batch_concurrency,
            "batch_failures": 0,
        }
        if not self._network_allows_work():
            summary.update({"skipped": True, "reason": "network_offline"})
            return summary
        stale_testing = self.store.reset_testing_nodes()
        summary["stale_testing_reset"] = stale_testing
        if stale_testing:
            logger.warning("Recovered %s stale TESTING nodes before candidate batch", stale_testing)
        candidates: list[tuple[NodeRecord, ParsedNode]] = []
        for node in self.store.list_nodes_by_status(NodeStatus.CANDIDATE):
            try:
                parsed = self.parser.parse_share_url(node.raw_config, node.source_subs[0] if node.source_subs else "candidate")
                candidates.append((node, parsed))
            except Exception:
                logger.exception("Failed to parse candidate node %s", node.id)
                self._move_to_dead(node)
        summary["candidates"] = len(candidates)
        if not candidates:
            return summary
        batch_size = max(1, self.settings.health.candidate_batch_size)
        parallel_batches = max(1, self.settings.health.candidate_parallel_batches)
        batches = [candidates[start : start + batch_size] for start in range(0, len(candidates), batch_size)]
        summary.update(
            {
                "batches": len(batches),
                "batch_size": batch_size,
                "parallel_batches": parallel_batches,
                "probe_concurrency": self.settings.health.candidate_batch_concurrency,
            }
        )
        logger.info(
            "Testing %s candidate nodes in %s batches (batch_size=%s, parallel_batches=%s, probe_concurrency=%s)",
            len(candidates),
            len(batches),
            batch_size,
            parallel_batches,
            self.settings.health.candidate_batch_concurrency,
        )
        with ThreadPoolExecutor(max_workers=min(parallel_batches, len(batches))) as pool:
            futures = [pool.submit(self._process_candidate_batch, batch) for batch in batches]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception:
                    summary["batch_failures"] = int(summary["batch_failures"]) + 1
                    logger.exception("Candidate batch worker failed unexpectedly")
        return summary

    def _process_candidate_batch(self, candidates: list[tuple[NodeRecord, ParsedNode]]) -> None:
        parsed_nodes: list[ParsedNode] = []
        by_hash: dict[str, tuple[NodeRecord, ParsedNode, str]] = {}
        started_at = datetime.now(timezone.utc)
        for node, parsed in candidates:
            status_before = node.status.value
            node.status = NodeStatus.TESTING
            self.store.save_node(node)
            parsed_nodes.append(parsed)
            by_hash[parsed.config_hash] = (node, parsed, status_before)

        ports: list[int] = []
        for _ in parsed_nodes:
            port = self.port_manager.allocate_test()
            if port is None:
                for allocated in ports:
                    self.port_manager.release_test(allocated)
                logger.warning("No free test ports for candidate batch of %s nodes", len(parsed_nodes))
                for node, parsed in candidates:
                    node.status = NodeStatus.CANDIDATE
                    self.store.save_node(node)
                    self.store.record_test_history(
                        node_id=node.id,
                        test_kind="realping",
                        trigger="candidate",
                        started_at=started_at,
                        finished_at=datetime.now(timezone.utc),
                        network_online=self._network_allows_work(),
                        ok=False,
                        latency_ms=None,
                        download_kbps=None,
                        error="no free test ports for candidate batch",
                        status_before=NodeStatus.CANDIDATE.value,
                        status_after=node.status.value,
                        details={"protocol": parsed.protocol, "remark": parsed.remark, "batched": True},
                    )
                return
            ports.append(port)

        try:
            results = self.test_service.run_realping_batch(
                parsed_nodes,
                concurrency=self.settings.health.candidate_batch_concurrency,
                ports=ports,
            )
        except Exception:
            logger.exception("Candidate realping batch crashed")
            network_online = self._network_allows_work(force_refresh=True)
            for node, parsed in candidates:
                node.status = NodeStatus.CANDIDATE if not network_online else NodeStatus.DEAD
                node.last_test_at = datetime.now(timezone.utc)
                if node.status == NodeStatus.DEAD:
                    node.dead_until = datetime.now(timezone.utc) + timedelta(hours=self.settings.dead_pool.ttl_hours)
                self.store.save_node(node)
                self.store.record_test_history(
                    node_id=node.id,
                    test_kind="realping",
                    trigger="candidate",
                    started_at=started_at,
                    finished_at=datetime.now(timezone.utc),
                    network_online=self._network_allows_work(),
                    ok=False,
                    latency_ms=None,
                    download_kbps=None,
                    error="candidate realping batch crashed",
                    status_before=NodeStatus.CANDIDATE.value,
                    status_after=node.status.value,
                    details={"protocol": parsed.protocol, "remark": parsed.remark, "crashed": True, "batched": True},
                )
            return
        finally:
            for port in ports:
                self.port_manager.release_test(port)

        result_map = {result.parsed_node.config_hash: result for result in results}
        has_failed_result = len(result_map) < len(candidates) or any(
            (not result.ok) or result.latency_ms > self.settings.health.max_relay_delay_ms for result in results
        )
        network_online_after_batch = self._network_allows_work(force_refresh=True) if has_failed_result else True
        finished_at = datetime.now(timezone.utc)
        for node, parsed in candidates:
            entry = by_hash.get(parsed.config_hash)
            result = result_map.get(parsed.config_hash)
            if entry is None or result is None:
                node.status = NodeStatus.CANDIDATE
                node.last_test_at = finished_at
                self.store.save_node(node)
                self.store.record_test_history(
                    node_id=node.id,
                    test_kind="realping",
                    trigger="candidate",
                    started_at=started_at,
                    finished_at=finished_at,
                    network_online=self._network_allows_work(),
                    ok=False,
                    latency_ms=None,
                    download_kbps=None,
                    error="candidate result missing from batch",
                    status_before=NodeStatus.CANDIDATE.value,
                    status_after=node.status.value,
                    details={"protocol": parsed.protocol, "remark": parsed.remark, "batched": True},
                )
                continue
            _, _, status_before = entry
            node.last_test_at = finished_at
            if result.latency_ms >= 0:
                node.relay_delay_ms = result.latency_ms
            if result.ok and result.latency_ms <= self.settings.health.max_relay_delay_ms:
                self._activate_node(node, parsed)
                self._record_test_result(
                    node.id,
                    "realping",
                    "candidate",
                    started_at=started_at,
                    result=result,
                    status_before=status_before,
                    status_after=node.status.value,
                    details={"protocol": parsed.protocol, "remark": parsed.remark, "batched": True},
                )
                continue
            if result.ok and result.latency_ms > self.settings.health.max_relay_delay_ms:
                result.ok = False
                result.error = "relay delay too high"
            if not network_online_after_batch:
                node.status = NodeStatus.CANDIDATE
                self.store.save_node(node)
                self._record_test_result(
                    node.id,
                    "realping",
                    "candidate",
                    started_at=started_at,
                    result=result,
                    status_before=status_before,
                    status_after=node.status.value,
                    details={"protocol": parsed.protocol, "remark": parsed.remark, "suppressed_by_network_guard": True, "batched": True},
                )
                continue
            self._move_to_dead(node)
            self._record_test_result(
                node.id,
                "realping",
                "candidate",
                started_at=started_at,
                result=result,
                status_before=status_before,
                status_after=node.status.value,
                details={"protocol": parsed.protocol, "remark": parsed.remark, "batched": True},
            )

    def _promote_waiting_nodes(self) -> None:
        if not self._network_allows_work():
            return
        for node in self.store.list_nodes_by_status(NodeStatus.WAITING_FOR_PORT):
            try:
                parsed = self.parser.parse_share_url(node.raw_config, node.source_subs[0] if node.source_subs else "waiting")
                self._activate_node(node, parsed)
            except Exception:
                logger.exception("Failed to promote waiting node %s", node.id)

    def _move_to_probation(self, node: NodeRecord) -> None:
        with self.transition_lock:
            if self.state.vip_node_id == node.id:
                self.runtime_supervisor.stop_vip_runtime()
            self.runtime_supervisor.stop_active_runtime(node.id)
            self.port_manager.release_main(node.main_port)
            node.main_port = None
            node.status = NodeStatus.PROBATION
            node.consecutive_relay_failures += 1
            node.consecutive_relay_successes = 0
            node.health_success_ewma = self._ewma(node.health_success_ewma, 0.0)
            self.store.save_node(node)

    def _move_to_dead(self, node: NodeRecord) -> None:
        with self.transition_lock:
            if self.state.vip_node_id == node.id:
                self.runtime_supervisor.stop_vip_runtime()
            self.runtime_supervisor.stop_active_runtime(node.id)
            self.port_manager.release_main(node.main_port)
            node.main_port = None
            node.status = NodeStatus.DEAD
            node.dead_until = datetime.now(timezone.utc) + timedelta(hours=self.settings.dead_pool.ttl_hours)
            node.consecutive_relay_successes = 0
            node.health_success_ewma = self._ewma(node.health_success_ewma, 0.0)
            self.store.save_node(node)

    def _mark_probation_failure(self, node: NodeRecord) -> None:
        node.consecutive_relay_failures += 1
        node.consecutive_relay_successes = 0
        if node.consecutive_relay_failures >= PROBATION_FAILURE_THRESHOLD:
            self._move_to_dead(node)
            return
        node.status = NodeStatus.PROBATION
        node.health_success_ewma = self._ewma(node.health_success_ewma, 0.0)
        self.store.save_node(node)

    def _mark_probation_success(self, node: NodeRecord, parsed: ParsedNode) -> None:
        node.consecutive_relay_successes += 1
        node.consecutive_relay_failures = 0
        node.health_success_ewma = self._ewma(node.health_success_ewma, 1.0)
        if node.consecutive_relay_successes >= PROBATION_SUCCESS_THRESHOLD:
            self._activate_node(node, parsed)
            return
        node.status = NodeStatus.PROBATION
        self.store.save_node(node)

    def _ewma(self, current: float, value: float, alpha: float = 0.3) -> float:
        return current * (1 - alpha) + value * alpha

    def _dt(self, value: datetime | None) -> str | None:
        return value.isoformat() if value else None

    def _network_allows_work(self, force_refresh: bool = False) -> bool:
        if not self.settings.network_guard.enabled:
            return True
        if force_refresh:
            return self.network_guard.refresh().online
        return self.network_guard.is_online()

    def _maintain_vip_runtime(self) -> None:
        decision = self.selection_engine.select_best_vip_node()
        if decision is None:
            if self.runtime_supervisor.is_vip_running():
                self.runtime_supervisor.stop_vip_runtime()
            return

        current_node = self.store.get_node(self.state.vip_node_id) if self.state.vip_node_id else None
        current_running = self.runtime_supervisor.is_vip_running()
        if current_node is None or current_node.status != NodeStatus.ACTIVE or not current_running:
            self._switch_vip_runtime(decision)
            return

        if current_node.id == decision.node_id:
            self.state.vip_score = decision.score
            return

        if not self._vip_switch_allowed(decision):
            return
        self._switch_vip_runtime(decision)

    def _vip_switch_allowed(self, candidate: VipSelectionDecision) -> bool:
        if self.state.vip_last_switched_at is None:
            return True
        elapsed = (datetime.now(timezone.utc) - self.state.vip_last_switched_at).total_seconds()
        if elapsed < self.settings.vip_port.min_switch_interval_seconds:
            return False
        current_score = self.state.vip_score if self.state.vip_score is not None else 0.0
        return (candidate.score - current_score) >= self.settings.vip_port.switch_threshold_score_diff

    def _switch_vip_runtime(self, decision: VipSelectionDecision) -> None:
        node = self.store.get_node(decision.node_id)
        if node is None:
            return
        parsed = self.parser.parse_share_url(node.raw_config, node.source_subs[0] if node.source_subs else "vip")
        with self.transition_lock:
            self.runtime_supervisor.start_vip_runtime(node.id, parsed, self.settings.vip_port.port)
            self.state.vip_node_id = node.id
            self.state.vip_score = decision.score
            self.state.vip_last_switched_at = datetime.now(timezone.utc)
            logger.info(
                "VIP port switched to node %s on port %s with score %.4f",
                node.id,
                self.settings.vip_port.port,
                decision.score,
            )

    def _check_active_nodes_batch(self) -> dict[str, Any]:
        summary: dict[str, Any] = {
            "active_nodes": 0,
            "checked": 0,
            "failures": 0,
            "suppressed": 0,
            "moved_to_probation": 0,
        }
        active_nodes = self.store.list_nodes_by_status(NodeStatus.ACTIVE)
        summary["active_nodes"] = len(active_nodes)
        probe_failures: list[NodeRecord] = []
        checked = 0
        for node in active_nodes:
            if node.main_port is None:
                self._move_to_probation(node)
                summary["moved_to_probation"] = int(summary["moved_to_probation"]) + 1
                continue
            if not self.runtime_supervisor.is_running(node.id):
                self._move_to_probation(node)
                summary["moved_to_probation"] = int(summary["moved_to_probation"]) + 1
                continue
            checked += 1
            summary["checked"] = checked
            started = datetime.now(timezone.utc)
            try:
                latency = self.test_service.probe_running_port(node.main_port)
                node.relay_delay_ms = latency
                finished = datetime.now(timezone.utc)
                node.last_health_check_at = finished
                node.consecutive_relay_failures = 0
                node.health_success_ewma = self._ewma(node.health_success_ewma, 1.0)
                self.store.save_node(node)
                self.store.record_test_history(
                    node_id=node.id,
                    test_kind="realping",
                    trigger="health",
                    started_at=started,
                    finished_at=finished,
                    network_online=True,
                    ok=True,
                    latency_ms=latency,
                    download_kbps=None,
                    error="",
                    status_before=node.status.value,
                    status_after=node.status.value,
                    details={"main_port": node.main_port},
                )
            except Exception:
                probe_failures.append(node)

        if not probe_failures:
            return summary

        threshold_percent = self.settings.network_guard.mass_failure_threshold_percent
        failure_percent = (len(probe_failures) * 100.0 / max(1, checked))
        summary["failures"] = len(probe_failures)
        if self.settings.network_guard.enabled and failure_percent >= threshold_percent:
            if not self._network_allows_work(force_refresh=True):
                logger.warning(
                    "Suppressed %s active-node failures because host network is offline (%.1f%% failures in batch)",
                    len(probe_failures),
                    failure_percent,
                )
                summary["suppressed"] = len(probe_failures)
                return summary

        for node in probe_failures:
            logger.warning("Active relay check failed for node %s", node.id)
            finished = datetime.now(timezone.utc)
            self.store.record_test_history(
                node_id=node.id,
                test_kind="realping",
                trigger="health",
                started_at=finished,
                finished_at=finished,
                network_online=self._network_allows_work(),
                ok=False,
                latency_ms=None,
                download_kbps=None,
                error="health probe failed",
                status_before=node.status.value,
                status_after=NodeStatus.PROBATION.value,
                details={"main_port": node.main_port},
            )
            self._move_to_probation(node)
            summary["moved_to_probation"] = int(summary["moved_to_probation"]) + 1
        return summary

    def _record_test_result(
        self,
        node_id: str,
        test_kind: str,
        trigger: str,
        started_at: datetime | None,
        result,
        status_before: str,
        status_after: str,
        details: dict[str, object] | None = None,
    ) -> None:
        finished = datetime.now(timezone.utc)
        begun = started_at or finished
        self.store.record_test_history(
            node_id=node_id,
            test_kind=test_kind,
            trigger=trigger,
            started_at=begun,
            finished_at=finished,
            network_online=self._network_allows_work(),
            ok=result.ok,
            latency_ms=result.latency_ms if result.latency_ms >= 0 else None,
            download_kbps=result.download_kbps,
            error=result.error,
            status_before=status_before,
            status_after=status_after,
            details=details,
        )
