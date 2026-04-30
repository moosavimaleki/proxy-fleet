from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone

from submanager.config.models import AppSettings
from submanager.core.models import ActiveNodeSnapshot, BestNodeDecision, ClientCircuitState, NodeStatus, VipSelectionDecision
from submanager.storage.sqlite_store import SqliteStore


class SelectionEngine:
    def __init__(self, settings: AppSettings, store: SqliteStore) -> None:
        self.settings = settings
        self.store = store

    def select_best_node(self, client_id: str) -> BestNodeDecision | None:
        candidates = self._build_candidates(client_id)
        if not candidates:
            return None

        sample_size = self.settings.selection.sample_size
        sampled = candidates if len(candidates) <= sample_size else random.sample(candidates, sample_size)
        for snapshot in sampled:
            snapshot.score = self._score(client_id, snapshot)
        best = max(sampled, key=lambda item: item.score or -999.0)
        assignment = self.store.record_assignment(client_id, best.node.id, best.node.main_port or 0)
        self.store.append_usage_event(client_id, best.node.id, "assigned")
        state = best.client_state
        if state is not None:
            state.last_assigned_at = datetime.now(timezone.utc)
            self.store.save_client_node_state(state)
        return BestNodeDecision(
            node_id=best.node.id,
            port=best.node.main_port or 0,
            assignment_id=assignment.id,
            relay_delay_ms=best.node.relay_delay_ms,
            expires_in_seconds=self.settings.assignment_ttl_seconds,
        )

    def select_best_vip_node(self) -> VipSelectionDecision | None:
        candidates = self._build_vip_candidates()
        if not candidates:
            return None
        for snapshot in candidates:
            snapshot.score = self._vip_score(snapshot)
        best = max(candidates, key=lambda item: item.score or -999.0)
        return VipSelectionDecision(
            node_id=best.node.id,
            port=best.node.main_port or 0,
            score=best.score or 0.0,
            relay_delay_ms=best.node.relay_delay_ms,
            download_kbps=best.node.download_kbps,
        )

    def _build_candidates(self, client_id: str) -> list[ActiveNodeSnapshot]:
        now = datetime.now(timezone.utc)
        snapshots: list[ActiveNodeSnapshot] = []
        for node in self._list_eligible_active_nodes():
            relation = self.store.get_client_node_state(client_id, node.id)
            if relation.state == ClientCircuitState.OPEN and relation.cooldown_until and relation.cooldown_until > now:
                continue
            if relation.state == ClientCircuitState.OPEN and relation.cooldown_until and relation.cooldown_until <= now:
                relation.state = ClientCircuitState.HALF_OPEN
                self.store.save_client_node_state(relation)
            if self.store.has_open_assignment(client_id, node.id, self.settings.assignment_ttl_seconds):
                continue
            snapshots.append(
                ActiveNodeSnapshot(
                    node=node,
                    active_assignments=self.store.count_active_assignments(node.id, self.settings.assignment_ttl_seconds),
                    recent_global_usage=self.store.count_recent_usage(node.id, 300),
                    client_state=relation,
                    recent_client_usage=self.store.count_recent_client_usage(client_id, node.id, 1800),
                )
            )
        return snapshots

    def _build_vip_candidates(self) -> list[ActiveNodeSnapshot]:
        snapshots: list[ActiveNodeSnapshot] = []
        for node in self._list_eligible_active_nodes():
            snapshots.append(
                ActiveNodeSnapshot(
                    node=node,
                    active_assignments=self.store.count_active_assignments(node.id, self.settings.assignment_ttl_seconds),
                    recent_global_usage=self.store.count_recent_usage(node.id, 300),
                )
            )
        return snapshots

    def _list_eligible_active_nodes(self):
        for node in self.store.list_nodes_by_status(NodeStatus.ACTIVE):
            if node.main_port is None:
                continue
            if node.relay_delay_ms is None or node.relay_delay_ms > self.settings.health.max_relay_delay_ms:
                continue
            if self.settings.download_test.enabled:
                if node.download_kbps is not None and node.download_kbps < self.settings.download_test.min_download_kbps:
                    continue
            yield node

    def _score(self, client_id: str, snapshot: ActiveNodeSnapshot) -> float:
        relation = snapshot.client_state
        weights = self.settings.selection.weights
        latency_score = max(0.0, min(1.0, 1 - (snapshot.node.relay_delay_ms or 0) / self.settings.health.max_relay_delay_ms))
        target_download = max(1, self.settings.download_test.target_download_kbps)
        download_score = max(0.0, min(1.0, (snapshot.node.download_kbps or 0) / target_download))
        availability_score = snapshot.node.health_success_ewma
        fairness_score = 1 / (1 + snapshot.recent_global_usage + snapshot.recent_client_usage + snapshot.active_assignments)
        client_history_score = relation.success_rate_ewma if relation else 0.5
        fail_streak = relation.fail_streak if relation else 0
        rate_limit_streak = relation.rate_limit_streak if relation else 0
        penalty_score = min(0.5, fail_streak * 0.15 + rate_limit_streak * 0.25)
        noise = random.uniform(-0.02, 0.02)
        return (
            weights.latency * latency_score
            + weights.download * download_score
            + weights.availability * availability_score
            + weights.fairness * fairness_score
            + weights.client_history * client_history_score
            - penalty_score
            + noise
        )

    def _vip_score(self, snapshot: ActiveNodeSnapshot) -> float:
        latency_score = max(0.0, min(1.0, 1 - (snapshot.node.relay_delay_ms or 0) / self.settings.health.max_relay_delay_ms))
        target_download = max(1, self.settings.download_test.target_download_kbps)
        download_score = max(0.0, min(1.0, (snapshot.node.download_kbps or 0) / target_download))
        availability_score = snapshot.node.health_success_ewma
        low_usage_score = 1 / (1 + snapshot.recent_global_usage + snapshot.active_assignments)
        return (
            0.45 * latency_score
            + 0.25 * download_score
            + 0.20 * availability_score
            + 0.10 * low_usage_score
        )
