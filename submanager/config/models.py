from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SubscriptionSource:
    name: str
    url: str


@dataclass
class SubscriptionSettings:
    refresh_interval_seconds: int = 60
    urls: list[SubscriptionSource] = field(default_factory=list)


@dataclass
class PortRange:
    start: int
    end: int

    def validate(self) -> None:
        if self.start <= 0 or self.end <= 0 or self.start > self.end:
            raise ValueError(f"invalid port range: {self.start}-{self.end}")


@dataclass
class PortSettings:
    main: PortRange
    test: PortRange

    def validate(self) -> None:
        self.main.validate()
        self.test.validate()
        if self.main.start <= self.test.end and self.test.start <= self.main.end:
            raise ValueError("main and test port ranges must not overlap")


@dataclass
class HealthSettings:
    active_pool_relay_check_interval_seconds: int = 10
    probation_recheck_interval_seconds: int = 120
    candidate_recheck_interval_seconds: int = 60
    candidate_batch_size: int = 256
    candidate_batch_concurrency: int = 128
    candidate_parallel_batches: int = 4
    candidate_batch_timeout_seconds: int = 12
    relay_timeout_ms: int = 3000
    max_relay_delay_ms: int = 1500
    test_url: str = "http://www.msftconnecttest.com/connecttest.txt"
    fallback_urls: list[str] = field(default_factory=list)


@dataclass
class DownloadTestSettings:
    enabled: bool = True
    timeout_seconds: int = 8
    per_url_timeout_seconds: float = 1.5
    min_download_kbps: int = 100
    target_download_kbps: int = 1000
    test_url: str = "https://speed.cloudflare.com/__down?bytes=10000000"
    fallback_urls: list[str] = field(default_factory=list)


@dataclass
class DeadPoolSettings:
    ttl_hours: int = 8


@dataclass
class PenaltyRule:
    base_cooldown_seconds: int
    max_cooldown_seconds: int
    jitter_ratio: float


@dataclass
class ClientPenaltySettings:
    broken: PenaltyRule
    rate_limited: PenaltyRule


@dataclass
class SelectionWeights:
    latency: float = 0.35
    download: float = 0.20
    availability: float = 0.20
    fairness: float = 0.15
    client_history: float = 0.10


@dataclass
class SelectionSettings:
    strategy: str = "weighted_power_of_choices"
    sample_size: int = 5
    weights: SelectionWeights = field(default_factory=SelectionWeights)


@dataclass
class ApiSettings:
    host: str = "0.0.0.0"
    port: int = 8080
    auth_enabled: bool = False


@dataclass
class VipPortSettings:
    enabled: bool = True
    port: int = 5050
    check_interval_seconds: int = 10
    min_switch_interval_seconds: int = 60
    switch_threshold_score_diff: float = 0.15


@dataclass
class SentinelTarget:
    type: str
    host: str = ""
    port: int = 0
    url: str = ""


@dataclass
class NetworkGuardSettings:
    enabled: bool = True
    check_interval_seconds: int = 5
    failure_threshold: int = 1
    recovery_threshold: int = 1
    minimum_successful_targets: int = 1
    require_http_success: bool = True
    mass_failure_threshold_percent: int = 40
    sentinel_targets: list[SentinelTarget] = field(default_factory=list)


@dataclass
class DatabaseSettings:
    type: str = "sqlite"
    path: str = "data/app.db"


@dataclass
class ServiceSettings:
    name: str = "config-orchestrator"
    environment: str = "production"


@dataclass
class AppSettings:
    service: ServiceSettings
    subscriptions: SubscriptionSettings
    ports: PortSettings
    database: DatabaseSettings
    health: HealthSettings
    download_test: DownloadTestSettings
    dead_pool: DeadPoolSettings
    client_penalty: ClientPenaltySettings
    selection: SelectionSettings
    api: ApiSettings
    vip_port: VipPortSettings = field(default_factory=VipPortSettings)
    network_guard: NetworkGuardSettings = field(default_factory=NetworkGuardSettings)
    assignment_ttl_seconds: int = 60
    xray_bin: str = ""
