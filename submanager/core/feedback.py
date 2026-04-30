from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

from submanager.config.models import AppSettings, PenaltyRule
from submanager.core.models import ClientCircuitState, ClientNodeStateRecord, FeedbackInput
from submanager.storage.sqlite_store import SqliteStore


class FeedbackEngine:
    def __init__(self, settings: AppSettings, store: SqliteStore) -> None:
        self.settings = settings
        self.store = store

    def apply(self, feedback: FeedbackInput) -> None:
        node = self.store.get_node(feedback.node_id)
        if node is None:
            raise KeyError("NODE_NOT_FOUND")

        state = self.store.get_client_node_state(feedback.client, feedback.node_id)
        now = datetime.now(timezone.utc)
        state.last_feedback_at = now

        if feedback.status == "used":
            state.state = ClientCircuitState.CLOSED
            state.fail_streak = 0
            state.rate_limit_streak = 0
            state.cooldown_until = None
            state.usage_count += 1
            state.success_count += 1
            state.success_rate_ewma = self._ewma(state.success_rate_ewma, 1.0)
            state.last_success_at = now
        elif feedback.status == "broken":
            state.fail_streak += 1
            state.broken_count += 1
            state.state = ClientCircuitState.OPEN
            state.cooldown_until = now + self._cooldown(self.settings.client_penalty.broken, state.fail_streak)
            state.success_rate_ewma = self._ewma(state.success_rate_ewma, 0.0)
            state.last_failure_at = now
        elif feedback.status == "rate_limited":
            state.rate_limit_streak += 1
            state.rate_limited_count += 1
            state.state = ClientCircuitState.OPEN
            state.cooldown_until = now + self._cooldown(self.settings.client_penalty.rate_limited, state.rate_limit_streak)
            state.success_rate_ewma = self._ewma(state.success_rate_ewma, 0.0)
            state.last_failure_at = now
        else:
            raise ValueError("invalid feedback status")

        self.store.save_client_node_state(state)
        self.store.mark_assignment_feedback(feedback.client, feedback.node_id, feedback.status)
        self.store.append_usage_event(feedback.client, feedback.node_id, feedback.status)

    def _cooldown(self, rule: PenaltyRule, streak: int) -> timedelta:
        raw = min(rule.max_cooldown_seconds, int(rule.base_cooldown_seconds * (2 ** max(streak - 1, 0))))
        jitter = random.uniform(0, raw * rule.jitter_ratio)
        return timedelta(seconds=raw + jitter)

    def _ewma(self, current: float, value: float, alpha: float = 0.3) -> float:
        return current * (1 - alpha) + value * alpha
