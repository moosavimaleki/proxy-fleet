from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


class NodeStatus(StrEnum):
    CANDIDATE = "CANDIDATE"
    TESTING = "TESTING"
    ACTIVE = "ACTIVE"
    PROBATION = "PROBATION"
    DEAD = "DEAD"
    WAITING_FOR_PORT = "WAITING_FOR_PORT"
    REMOVED = "REMOVED"


class ClientCircuitState(StrEnum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


@dataclass
class ParsedNode:
    source_url: str
    raw_config: str
    share_url: str
    protocol: str
    address: str
    port: int
    remark: str
    outbound: dict[str, Any]
    normalized_config: dict[str, Any]
    config_hash: str


@dataclass
class NodeRecord:
    id: str
    config_hash: str
    raw_config: str
    normalized_config: dict[str, Any]
    source_subs: list[str]
    status: NodeStatus
    main_port: int | None = None
    relay_delay_ms: int | None = None
    download_kbps: int | None = None
    health_success_ewma: float = 1.0
    consecutive_relay_failures: int = 0
    consecutive_relay_successes: int = 0
    created_at: datetime | None = None
    updated_at: datetime | None = None
    last_health_check_at: datetime | None = None
    last_test_at: datetime | None = None
    dead_until: datetime | None = None


@dataclass
class ClientNodeStateRecord:
    client_id: str
    node_id: str
    state: ClientCircuitState = ClientCircuitState.CLOSED
    fail_streak: int = 0
    rate_limit_streak: int = 0
    cooldown_until: datetime | None = None
    usage_count: int = 0
    success_count: int = 0
    broken_count: int = 0
    rate_limited_count: int = 0
    recent_usage_score: float = 0.0
    success_rate_ewma: float = 0.5
    last_assigned_at: datetime | None = None
    last_feedback_at: datetime | None = None
    last_failure_at: datetime | None = None
    last_success_at: datetime | None = None


@dataclass
class AssignmentRecord:
    id: str
    client_id: str
    node_id: str
    port: int
    assigned_at: datetime
    feedback_status: str | None = None
    feedback_at: datetime | None = None


@dataclass
class RuntimeHandle:
    node_id: str
    port: int
    process: Any
    started_at: datetime


@dataclass
class BestNodeDecision:
    node_id: str
    port: int
    assignment_id: str
    relay_delay_ms: int | None
    expires_in_seconds: int


@dataclass
class VipSelectionDecision:
    node_id: str
    port: int
    score: float
    relay_delay_ms: int | None
    download_kbps: int | None


@dataclass
class NetworkStateSnapshot:
    enabled: bool
    online: bool
    status: str
    failure_streak: int
    recovery_streak: int
    last_checked_at: datetime | None = None
    last_changed_at: datetime | None = None
    last_success_at: datetime | None = None
    last_error: str = ""
    successful_targets: int = 0
    total_targets: int = 0


@dataclass
class TestResult:
    parsed_node: ParsedNode
    ok: bool
    latency_ms: int
    download_kbps: int | None = None
    error: str = ""


@dataclass
class TestHistoryRecord:
    id: str
    node_id: str
    test_kind: str
    trigger: str
    started_at: datetime
    finished_at: datetime
    network_online: bool
    ok: bool
    latency_ms: int | None = None
    download_kbps: int | None = None
    error: str = ""
    status_before: str = ""
    status_after: str = ""
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class SystemEventRecord:
    id: str
    created_at: datetime
    level: str
    component: str
    event: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class BatchNode:
    node: ParsedNode
    local_port: int
    inbound_tag: str
    outbound_tag: str


@dataclass
class ActiveNodeSnapshot:
    node: NodeRecord
    active_assignments: int = 0
    recent_global_usage: int = 0
    client_state: ClientNodeStateRecord | None = None
    recent_client_usage: int = 0
    score: float | None = None


@dataclass
class FeedbackInput:
    client: str
    node_id: str
    status: str


@dataclass
class BestRequest:
    client: str


@dataclass
class ServiceState:
    active_runtimes: dict[str, RuntimeHandle] = field(default_factory=dict)
    vip_runtime: RuntimeHandle | None = None
    vip_node_id: str | None = None
    vip_score: float | None = None
    vip_last_switched_at: datetime | None = None
    network: NetworkStateSnapshot | None = None
    subscription_reload_in_progress: bool = False
    subscription_reload_started_at: datetime | None = None
    subscription_reload_finished_at: datetime | None = None
    subscription_reload_last_result: dict[str, Any] = field(default_factory=dict)
