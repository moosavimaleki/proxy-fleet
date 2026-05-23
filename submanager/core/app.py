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
from submanager.core.models import BestNodeDecision, FeedbackInput, NodeRecord, NodeStatus, ParsedNode, ServiceState, TestResult, VipSelectionDecision
from submanager.core.ports import PortManager
from submanager.core.runtime import RuntimeSupervisor
from submanager.parser import SubscriptionParser
from submanager.selection.engine import SelectionEngine
from submanager.storage.sqlite_store import SqliteStore
from submanager.testing.service import TestService
from submanager.testing.xray import XrayBinaryResolver, XrayRunner
from submanager.utils.logging import get_logger


logger = get_logger(__name__)
CANDIDATE_FAILURE_THRESHOLD = 2
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

    def _has_exit_info(self, node: NodeRecord) -> bool:
        return bool(node.exit_ip or node.exit_country or node.exit_info)

    def _apply_exit_info(self, node: NodeRecord, exit_info: dict[str, Any]) -> None:
        normalized = {str(key): value for key, value in exit_info.items()}
        node.exit_ip = str(normalized.get("ip", "") or "")
        node.exit_hostname = str(normalized.get("hostname", "") or "")
        node.exit_city = str(normalized.get("city", "") or "")
        node.exit_region = str(normalized.get("region", "") or "")
        node.exit_country = str(normalized.get("country", "") or "")
        node.exit_loc = str(normalized.get("loc", "") or "")
        node.exit_org = str(normalized.get("org", "") or "")
        node.exit_postal = str(normalized.get("postal", "") or "")
        node.exit_timezone = str(normalized.get("timezone", "") or "")
        node.exit_info = normalized
        node.exit_info_fetched_at = datetime.now(timezone.utc)

    def _status_priority(self, status: NodeStatus) -> int:
        priorities = {
            NodeStatus.ACTIVE: 0,
            NodeStatus.PROBATION: 1,
            NodeStatus.CANDIDATE: 2,
            NodeStatus.WAITING_FOR_PORT: 3,
            NodeStatus.TESTING: 4,
            NodeStatus.DEAD: 5,
            NodeStatus.REMOVED: 6,
        }
        return priorities.get(status, 99)

    def _duplicate_keep_key(self, node: NodeRecord) -> tuple[Any, ...]:
        relay = node.relay_delay_ms if node.relay_delay_ms is not None else 10**9
        download = -(node.download_kbps if node.download_kbps is not None else -1)
        created = node.created_at or datetime.max.replace(tzinfo=timezone.utc)
        return (self._status_priority(node.status), relay, download, created, node.id)

    def _delete_node_runtime_and_record(self, node: NodeRecord) -> None:
        with self.transition_lock:
            if self.state.vip_node_id == node.id:
                self.runtime_supervisor.stop_vip_runtime()
                self.state.vip_node_id = None
                self.state.vip_score = None
            self.runtime_supervisor.stop_active_runtime(node.id)
            self.port_manager.release_main(node.main_port)
        self.store.delete_node(node.id)

    def _save_and_dedupe_by_exit_ip(self, node: NodeRecord) -> NodeRecord:
        self.store.save_node(node)
        if not node.exit_ip:
            return node
        peers = [item for item in self.store.list_nodes_by_exit_ip(node.exit_ip) if item.id != node.id]
        if not peers:
            return node
        group = [node, *peers]
        canonical = min(group, key=self._duplicate_keep_key)
        merged_sources = sorted({source for item in group for source in item.source_subs})
        if merged_sources != canonical.source_subs:
            canonical.source_subs = merged_sources
            self.store.save_node(canonical)
        removed_ids: list[str] = []
        for item in group:
            if item.id == canonical.id:
                continue
            removed_ids.append(item.id)
            self._delete_node_runtime_and_record(item)
        if removed_ids:
            self._event(
                "warning",
                "candidate",
                "duplicate_exit_ip_merged",
                "Merged duplicate proxy nodes by exit IP",
                {"exit_ip": node.exit_ip, "kept_node_id": canonical.id, "removed_node_ids": removed_ids},
            )
        return canonical

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
        status_before = node.status.value
        if not self._network_allows_work(force_refresh=True):
            started = datetime.now(timezone.utc)
            finished = started
            self.store.record_test_history(
                node_id=node.id,
                test_kind="fast",
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
            result = self.test_service.run_fast_test(parsed, test_port, fetch_exit_info=not self._has_exit_info(node))
        finally:
            self.port_manager.release_test(test_port)
        finished = datetime.now(timezone.utc)

        node.last_test_at = finished
        if result.latency_ms >= 0:
            node.relay_delay_ms = result.latency_ms
        if result.download_kbps is not None:
            node.download_kbps = result.download_kbps
        exit_info_details: dict[str, Any] = {}
        if result.exit_info:
            self._apply_exit_info(node, result.exit_info)
            exit_info_details = {
                "exit_info": result.exit_info,
                "exit_country": node.exit_country,
                "exit_city": node.exit_city,
                "exit_org": node.exit_org,
                "exit_ip": node.exit_ip,
            }
        if result.ok and node.status in {NodeStatus.DEAD, NodeStatus.PROBATION}:
            if node.status == NodeStatus.DEAD:
                node.dead_until = None
                node.status = NodeStatus.PROBATION
                node.consecutive_relay_failures = 0
                node.consecutive_relay_successes = 0
            self._mark_probation_success(node, parsed)
        kept = self._save_and_dedupe_by_exit_ip(node)
        self.store.record_test_history(
            node_id=node.id,
            test_kind="fast",
            trigger="manual",
            started_at=started,
            finished_at=finished,
            network_online=True,
            ok=result.ok,
            latency_ms=result.latency_ms if result.latency_ms >= 0 else None,
            download_kbps=result.download_kbps,
            error=result.error,
            status_before=status_before,
            status_after=node.status.value,
            details={"remark": parsed.remark, "protocol": parsed.protocol, "realping_first": True, **exit_info_details},
        )
        return {
            "ok": result.ok,
            "error": result.error,
            "network_online": True,
            "latency_ms": result.latency_ms if result.latency_ms >= 0 else None,
            "download_kbps": result.download_kbps,
            "status_before": status_before,
            "status_after": kept.status.value,
            "exit_info": kept.exit_info,
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
            "targets": [
                {
                    "type": target.type,
                    "endpoint": target.endpoint,
                    "ok": target.ok,
                    "duration_ms": target.duration_ms,
                    "error": target.error,
                }
                for target in snapshot.target_results
            ],
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
            "exit_ip": node.exit_ip,
            "exit_country": node.exit_country,
            "exit_city": node.exit_city,
            "exit_region": node.exit_region,
            "exit_org": node.exit_org,
            "exit_timezone": node.exit_timezone,
            "exit_info": node.exit_info,
            "exit_info_fetched_at": self._dt(node.exit_info_fetched_at),
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
        last_online = self.network_guard.snapshot().online
        while not self.stop_event.is_set():
            started = time.monotonic()
            try:
                snapshot = self.network_guard.refresh()
                details = {
                    "online": snapshot.online,
                    "successful_targets": snapshot.successful_targets,
                    "total_targets": snapshot.total_targets,
                    "minimum_successful_targets": self.settings.network_guard.minimum_successful_targets,
                    "require_http_success": self.settings.network_guard.require_http_success,
                    "failure_streak": snapshot.failure_streak,
                    "recovery_streak": snapshot.recovery_streak,
                    "duration_ms": int((time.monotonic() - started) * 1000),
                    "last_error": snapshot.last_error,
                    "targets": [
                        {
                            "type": target.type,
                            "endpoint": target.endpoint,
                            "ok": target.ok,
                            "duration_ms": target.duration_ms,
                            "error": target.error,
                        }
                        for target in snapshot.target_results
                    ],
                }
                if snapshot.online != last_online:
                    transition_event = "sentinel_recovered" if snapshot.online else "sentinel_offline"
                    transition_level = "info" if snapshot.online else "error"
                    transition_message = (
                        "Network guard recovered; workers may resume"
                        if snapshot.online
                        else "Network guard marked host offline; workers will pause"
                    )
                    self._event(transition_level, "network", transition_event, transition_message, details)
                    last_online = snapshot.online
                self._event(
                    "info" if snapshot.online else "warning",
                    "network",
                    "sentinel_check",
                    f"Network sentinel check: {snapshot.status}",
                    details,
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
            next_sleep = 0.0
            try:
                if self._network_allows_work():
                    result = self._check_active_nodes_batch()
                    active_nodes = int(result.get("active_nodes") or 0)
                    next_sleep = 0.0 if active_nodes else interval
                    self._event(
                        "info" if not result.get("failures") else "warning",
                        "health",
                        "active_check_finished",
                        "Active pool health check finished",
                        {**result, "duration_ms": int((time.monotonic() - started) * 1000), "next_run_seconds": next_sleep},
                    )
                else:
                    self._event("warning", "health", "active_check_skipped", "Active pool health check skipped because host network is offline")
                    next_sleep = self.settings.network_guard.check_interval_seconds if self.settings.network_guard.enabled else interval
            except Exception:
                logger.exception("Active health checker iteration failed")
                self._event("error", "health", "active_check_error", "Active health checker iteration failed")
                next_sleep = min(1.0, interval)
            if next_sleep > 0:
                self.stop_event.wait(next_sleep)

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
                result = self.test_service.run_fast_test(parsed, test_port, fetch_exit_info=not self._has_exit_info(node))
            finally:
                self.port_manager.release_test(test_port)

            status_before = node.status.value
            finished_at = datetime.now(timezone.utc)
            node.last_test_at = finished_at
            node.relay_delay_ms = result.latency_ms if result.latency_ms >= 0 else None
            node.download_kbps = result.download_kbps
            exit_info_details: dict[str, Any] = {}
            if result.exit_info:
                self._apply_exit_info(node, result.exit_info)
                exit_info_details = {
                    "exit_info": result.exit_info,
                    "exit_country": node.exit_country,
                    "exit_city": node.exit_city,
                    "exit_org": node.exit_org,
                    "exit_ip": node.exit_ip,
                }
            kept = self._save_and_dedupe_by_exit_ip(node)
            if kept.id != node.id:
                continue
            if result.ok:
                self._mark_probation_success(node, parsed)
                self._record_test_result(node.id, "fast", "probation", started_at=finished_at, result=result, status_before=status_before, status_after=node.status.value, details={"protocol": parsed.protocol, "remark": parsed.remark, "probation_successes": node.consecutive_relay_successes, "realping_first": True, **exit_info_details})
            else:
                if not self._network_allows_work(force_refresh=True):
                    self.store.save_node(node)
                    self._record_test_result(node.id, "fast", "probation", started_at=finished_at, result=result, status_before=status_before, status_after=node.status.value, details={"suppressed_by_network_guard": True, "protocol": parsed.protocol, "remark": parsed.remark, "realping_first": True, **exit_info_details})
                    return
                self._mark_probation_failure(node)
                self._record_test_result(node.id, "fast", "probation", started_at=finished_at, result=result, status_before=status_before, status_after=node.status.value, details={"protocol": parsed.protocol, "remark": parsed.remark, "probation_failures": node.consecutive_relay_failures, "realping_first": True, **exit_info_details})

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
                        test_kind="fast",
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
                        details={"protocol": parsed.protocol, "remark": parsed.remark, "batched": True, "realping_first": True},
                    )
                return
            ports.append(port)

        try:
            try:
                results = self.test_service.run_fast_test_batch(
                    parsed_nodes,
                    concurrency=self.settings.health.candidate_batch_concurrency,
                    ports=ports,
                    fetch_exit_info=any(not self._has_exit_info(node) for node, _ in candidates),
                )
            except Exception:
                logger.exception("Candidate fast test batch crashed")
                for node, parsed in candidates:
                    node.status = NodeStatus.CANDIDATE
                    node.last_test_at = datetime.now(timezone.utc)
                    self.store.save_node(node)
                    self.store.record_test_history(
                        node_id=node.id,
                        test_kind="fast",
                        trigger="candidate",
                        started_at=started_at,
                        finished_at=datetime.now(timezone.utc),
                        network_online=self._network_allows_work(),
                        ok=False,
                        latency_ms=None,
                        download_kbps=None,
                        error="candidate fast test batch crashed",
                        status_before=NodeStatus.CANDIDATE.value,
                        status_after=node.status.value,
                        details={"protocol": parsed.protocol, "remark": parsed.remark, "crashed": True, "batched": True, "realping_first": True},
                    )
                return

            result_map = {result.parsed_node.config_hash: result for result in results}
            has_failures = len(result_map) < len(candidates) or any(not result.ok for result in results)
            success_count = sum(1 for result in results if result.ok)
            failure_percent = ((len(candidates) - success_count) * 100.0 / max(1, len(candidates)))
            threshold_percent = self.settings.network_guard.mass_failure_threshold_percent
            suppress_mass_failure = (
                self.settings.network_guard.enabled
                and has_failures
                and success_count == 0
                and len(candidates) >= 10
                and threshold_percent > 0
                and failure_percent >= threshold_percent
            )
            if suppress_mass_failure:
                logger.warning(
                    "Suppressed candidate batch failures because every probe failed (%.1f%% failures in batch)",
                    failure_percent,
                )
            if suppress_mass_failure:
                network_online_after_test = False
            elif has_failures:
                network_online_after_test = self._network_allows_work(force_refresh=True)
            else:
                network_online_after_test = True
            finished_at = datetime.now(timezone.utc)

            for node, parsed in candidates:
                entry = by_hash.get(parsed.config_hash)
                result = result_map.get(parsed.config_hash)
                if entry is None:
                    logger.warning("Candidate batch bookkeeping missing for node %s", node.id)
                    continue
                if result is None:
                    result = TestResult(
                        parsed_node=parsed,
                        ok=False,
                        latency_ms=-1,
                        download_kbps=0,
                        error="candidate result missing from batch",
                    )

                _, _, status_before = entry
                node.last_test_at = finished_at
                if result.latency_ms >= 0:
                    node.relay_delay_ms = result.latency_ms
                if result.download_kbps is not None:
                    node.download_kbps = result.download_kbps

                exit_info_details: dict[str, Any] = {}
                if result.exit_info:
                    self._apply_exit_info(node, result.exit_info)
                    exit_info_details = {
                        "exit_info": result.exit_info,
                        "exit_country": node.exit_country,
                        "exit_city": node.exit_city,
                        "exit_org": node.exit_org,
                        "exit_ip": node.exit_ip,
                    }
                if result.ok:
                    self._activate_node(node, parsed)
                    kept = self._save_and_dedupe_by_exit_ip(node)
                    if kept.id != node.id:
                        continue
                    self._record_test_result(
                        node.id,
                        "fast",
                        "candidate",
                        started_at=started_at,
                        result=result,
                        status_before=status_before,
                        status_after=node.status.value,
                        details={"protocol": parsed.protocol, "remark": parsed.remark, "batched": True, "realping_first": True, **exit_info_details},
                    )
                    continue

                if not network_online_after_test:
                    node.status = NodeStatus.CANDIDATE
                    self.store.save_node(node)
                    self._record_test_result(
                        node.id,
                        "fast",
                        "candidate",
                        started_at=started_at,
                        result=result,
                        status_before=status_before,
                        status_after=node.status.value,
                        details={
                            "protocol": parsed.protocol,
                            "remark": parsed.remark,
                            "suppressed_by_network_guard": not suppress_mass_failure,
                            "suppressed_by_mass_failure": suppress_mass_failure,
                            "batched": True,
                            "realping_first": True,
                            **exit_info_details,
                        },
                    )
                    continue

                self._mark_candidate_failure(node)
                self._record_test_result(
                    node.id,
                    "fast",
                    "candidate",
                    started_at=started_at,
                    result=result,
                    status_before=status_before,
                    status_after=node.status.value,
                    details={
                        "protocol": parsed.protocol,
                        "remark": parsed.remark,
                        "candidate_failures": node.consecutive_relay_failures,
                        "batched": True,
                        "realping_first": True,
                        **exit_info_details,
                    },
                )
        finally:
            for port in ports:
                self.port_manager.release_test(port)

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
            # The first failure moves the node into probation; retries inside probation decide death.
            node.consecutive_relay_failures = 0
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

    def _mark_candidate_failure(self, node: NodeRecord) -> None:
        with self.transition_lock:
            node.consecutive_relay_failures += 1
            node.consecutive_relay_successes = 0
            node.health_success_ewma = self._ewma(node.health_success_ewma, 0.0)
            if node.consecutive_relay_failures >= CANDIDATE_FAILURE_THRESHOLD:
                self._move_to_dead(node)
                return
            node.status = NodeStatus.CANDIDATE
            node.dead_until = None
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
            "skipped_no_ports": 0,
            "vip_checked": 0,
            "vip_failures": 0,
        }
        active_nodes = self.store.list_nodes_by_status(NodeStatus.ACTIVE)
        summary["active_nodes"] = len(active_nodes)
        test_items: list[tuple[NodeRecord, ParsedNode, str]] = []
        for node in active_nodes:
            if node.main_port is None:
                self._move_to_probation(node)
                summary["moved_to_probation"] = int(summary["moved_to_probation"]) + 1
                continue
            if not self.runtime_supervisor.is_running(node.id):
                self._move_to_probation(node)
                summary["moved_to_probation"] = int(summary["moved_to_probation"]) + 1
                continue
            try:
                parsed = self.parser.parse_share_url(node.raw_config, node.source_subs[0] if node.source_subs else "health")
            except Exception:
                logger.exception("Failed to parse active node %s", node.id)
                result = TestResult(
                    parsed_node=ParsedNode("", node.raw_config, node.raw_config, "", "", 0, "", {}, {}, node.config_hash),
                    ok=False,
                    latency_ms=-1,
                    download_kbps=0,
                    error="failed to parse active node",
                )
                self._record_test_result(
                    node.id,
                    "fast",
                    "health",
                    started_at=datetime.now(timezone.utc),
                    result=result,
                    status_before=node.status.value,
                    status_after=NodeStatus.PROBATION.value,
                    details={"main_port": node.main_port, "parse_failed": True},
                )
                self._move_to_probation(node)
                summary["moved_to_probation"] = int(summary["moved_to_probation"]) + 1
                continue
            test_items.append((node, parsed, node.status.value))

        summary["checked"] = len(test_items)
        if not test_items:
            return summary

        probe_failures: list[tuple[NodeRecord, ParsedNode, str, TestResult, datetime]] = []
        batch_size = max(1, self.settings.health.candidate_batch_size)
        batches = [test_items[start : start + batch_size] for start in range(0, len(test_items), batch_size)]

        for batch in batches:
            started = datetime.now(timezone.utc)
            port_items = [(parsed, int(node.main_port or 0)) for node, parsed, _ in batch]
            vip_index: int | None = None
            vip_probe: tuple[NodeRecord, ParsedNode, str] | None = None
            if self.settings.vip_port.enabled and self.state.vip_node_id and self.runtime_supervisor.is_vip_running():
                for node, parsed, status_before in batch:
                    if node.id == self.state.vip_node_id:
                        vip_index = len(port_items)
                        vip_probe = (node, parsed, status_before)
                        port_items.append((parsed, self.settings.vip_port.port))
                        break
            try:
                results = self.test_service.run_fast_test_existing_ports(
                    port_items,
                    concurrency=self.settings.health.candidate_batch_concurrency,
                    fetch_exit_info=any(not self._has_exit_info(node) for node, _, _ in batch),
                    include_download=False,
                )
            except Exception:
                logger.exception("Active fast test batch crashed")
                results = [
                    TestResult(
                        parsed_node=parsed,
                        ok=False,
                        latency_ms=-1,
                        download_kbps=0,
                        error="active fast test batch crashed",
                    )
                    for _, parsed, _ in batch
                ]
                if vip_probe is not None:
                    _, parsed, _ = vip_probe
                    results.append(
                        TestResult(
                            parsed_node=parsed,
                            ok=False,
                            latency_ms=-1,
                            download_kbps=0,
                            error="active fast test batch crashed",
                        )
                    )

            while len(results) < len(port_items):
                parsed, _ = port_items[len(results)]
                results.append(
                    TestResult(
                        parsed_node=parsed,
                        ok=False,
                        latency_ms=-1,
                        download_kbps=0,
                        error="active health result missing from batch",
                    )
                )

            for idx, (node, parsed, status_before) in enumerate(batch):
                if self.store.get_node(node.id) is None:
                    continue
                result = results[idx]

                if not result.ok:
                    probe_failures.append((node, parsed, status_before, result, started))
                    continue

                if result.latency_ms >= 0:
                    node.relay_delay_ms = result.latency_ms
                if result.download_kbps is not None:
                    node.download_kbps = result.download_kbps
                finished = datetime.now(timezone.utc)
                node.last_health_check_at = finished
                node.last_test_at = finished
                node.consecutive_relay_failures = 0
                node.health_success_ewma = self._ewma(node.health_success_ewma, 1.0)
                exit_info_details: dict[str, Any] = {}
                if result.exit_info:
                    self._apply_exit_info(node, result.exit_info)
                    exit_info_details = {
                        "exit_info": result.exit_info,
                        "exit_country": node.exit_country,
                        "exit_city": node.exit_city,
                        "exit_org": node.exit_org,
                        "exit_ip": node.exit_ip,
                    }
                kept = self._save_and_dedupe_by_exit_ip(node)
                if kept.id != node.id:
                    continue
                self._record_test_result(
                    node.id,
                    "fast",
                    "health",
                    started_at=started,
                    result=result,
                    status_before=status_before,
                    status_after=node.status.value,
                    details={"main_port": node.main_port, "batched": True, "realping_first": True, **exit_info_details},
                )

            if vip_index is not None and vip_probe is not None:
                node, parsed, status_before = vip_probe
                if self.store.get_node(node.id) is None:
                    continue
                vip_result = results[vip_index]
                summary["vip_checked"] = int(summary["vip_checked"]) + 1
                self._record_test_result(
                    node.id,
                    "fast",
                    "vip-health",
                    started_at=started,
                    result=vip_result,
                    status_before=status_before,
                    status_after=node.status.value,
                    details={"vip_port": self.settings.vip_port.port, "protocol": parsed.protocol, "remark": parsed.remark, "batched": True, "realping_first": True},
                )
                if vip_result.ok:
                    continue
                summary["vip_failures"] = int(summary["vip_failures"]) + 1
                logger.warning("VIP hot port check failed for node %s: %s", node.id, vip_result.error)
                with self.transition_lock:
                    self.runtime_supervisor.stop_vip_runtime()
                    if self.state.vip_node_id == node.id:
                        self.state.vip_node_id = None
                        self.state.vip_score = None
                self._event(
                    "warning",
                    "vip",
                    "vip_port_failed",
                    "VIP hot port failed active health check and was stopped",
                    {
                        "node_id": node.id,
                        "vip_port": self.settings.vip_port.port,
                        "latency_ms": vip_result.latency_ms if vip_result.latency_ms >= 0 else None,
                        "download_kbps": vip_result.download_kbps,
                        "error": vip_result.error,
                    },
                )

        if not probe_failures:
            return summary

        threshold_percent = self.settings.network_guard.mass_failure_threshold_percent
        failure_percent = (len(probe_failures) * 100.0 / max(1, len(test_items)))
        summary["failures"] = len(probe_failures)
        if self.settings.network_guard.enabled and failure_percent >= threshold_percent:
            if not self._network_allows_work(force_refresh=True):
                logger.warning(
                    "Suppressed %s active-node failures because host network is offline (%.1f%% failures in batch)",
                    len(probe_failures),
                    failure_percent,
                )
                summary["suppressed"] = len(probe_failures)
                for node, parsed, status_before, result, started in probe_failures:
                    if self.store.get_node(node.id) is None:
                        continue
                    self._record_test_result(
                        node.id,
                        "fast",
                        "health",
                        started_at=started,
                        result=result,
                        status_before=status_before,
                        status_after=node.status.value,
                        details={"main_port": node.main_port, "protocol": parsed.protocol, "remark": parsed.remark, "suppressed_by_network_guard": True, "batched": True, "realping_first": True},
                    )
                return summary

        for node, parsed, status_before, result, started in probe_failures:
            if self.store.get_node(node.id) is None:
                continue
            logger.warning("Active relay check failed for node %s", node.id)
            if result.latency_ms >= 0:
                node.relay_delay_ms = result.latency_ms
            if result.download_kbps is not None:
                node.download_kbps = result.download_kbps
            node.last_health_check_at = datetime.now(timezone.utc)
            node.last_test_at = node.last_health_check_at
            self.store.save_node(node)
            self._record_test_result(
                node.id,
                "fast",
                "health",
                started_at=started,
                result=result,
                status_before=status_before,
                status_after=NodeStatus.PROBATION.value,
                details={"main_port": node.main_port, "protocol": parsed.protocol, "remark": parsed.remark, "batched": True, "realping_first": True},
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
