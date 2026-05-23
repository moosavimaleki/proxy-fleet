from __future__ import annotations

from pathlib import Path
from typing import Any

from submanager.config.models import (
    ApiSettings,
    AppSettings,
    ClientPenaltySettings,
    DatabaseSettings,
    DeadPoolSettings,
    DownloadTestSettings,
    HealthSettings,
    NetworkGuardSettings,
    PenaltyRule,
    PortRange,
    PortSettings,
    SentinelTarget,
    SelectionSettings,
    SelectionWeights,
    ServiceSettings,
    SubscriptionSettings,
    SubscriptionSource,
    VipPortSettings,
)


class ConfigLoader:
    def load(self, path: str | Path) -> AppSettings:
        try:
            import yaml
        except ImportError as exc:
            raise RuntimeError("PyYAML is required to load the YAML config file") from exc

        with open(path, "r", encoding="utf-8") as fh:
            payload = yaml.safe_load(fh) or {}

        settings = AppSettings(
            service=ServiceSettings(**payload.get("service", {})),
            subscriptions=self._load_subscriptions(payload.get("subscriptions", {})),
            ports=self._load_ports(payload.get("ports", {})),
            database=DatabaseSettings(**payload.get("database", {})),
            health=HealthSettings(**payload.get("health", {})),
            download_test=DownloadTestSettings(**payload.get("download_test", {})),
            dead_pool=DeadPoolSettings(**payload.get("dead_pool", {})),
            client_penalty=self._load_penalties(payload.get("client_penalty", {})),
            selection=self._load_selection(payload.get("selection", {})),
            api=ApiSettings(**payload.get("api", {})),
            vip_port=VipPortSettings(**payload.get("vip_port", {})),
            network_guard=self._load_network_guard(payload.get("network_guard", {})),
            assignment_ttl_seconds=int(payload.get("assignment_ttl_seconds", 60)),
            xray_bin=payload.get("xray_bin", ""),
        )
        self._validate(settings)
        return settings

    def _load_subscriptions(self, payload: dict[str, Any]) -> SubscriptionSettings:
        urls = [SubscriptionSource(**item) for item in payload.get("urls", [])]
        return SubscriptionSettings(
            refresh_interval_seconds=int(payload.get("refresh_interval_seconds", 60)),
            urls=urls,
        )

    def _load_ports(self, payload: dict[str, Any]) -> PortSettings:
        return PortSettings(
            main=PortRange(**payload.get("main", {})),
            test=PortRange(**payload.get("test", {})),
        )

    def _load_penalties(self, payload: dict[str, Any]) -> ClientPenaltySettings:
        return ClientPenaltySettings(
            broken=PenaltyRule(**payload.get("broken", {})),
            rate_limited=PenaltyRule(**payload.get("rate_limited", {})),
        )

    def _load_selection(self, payload: dict[str, Any]) -> SelectionSettings:
        weights_payload = payload.get("weights", {})
        return SelectionSettings(
            strategy=payload.get("strategy", "weighted_power_of_choices"),
            sample_size=int(payload.get("sample_size", 5)),
            weights=SelectionWeights(**weights_payload),
        )

    def _load_network_guard(self, payload: dict[str, Any]) -> NetworkGuardSettings:
        targets = [SentinelTarget(**item) for item in payload.get("sentinel_targets", [])]
        return NetworkGuardSettings(
            enabled=bool(payload.get("enabled", True)),
            check_interval_seconds=int(payload.get("check_interval_seconds", 5)),
            failure_threshold=int(payload.get("failure_threshold", 1)),
            recovery_threshold=int(payload.get("recovery_threshold", 1)),
            minimum_successful_targets=int(payload.get("minimum_successful_targets", 1)),
            require_http_success=bool(payload.get("require_http_success", True)),
            mass_failure_threshold_percent=int(payload.get("mass_failure_threshold_percent", 40)),
            sentinel_targets=targets,
        )

    def _validate(self, settings: AppSettings) -> None:
        if not settings.subscriptions.urls:
            raise ValueError("subscriptions.urls must not be empty")
        settings.ports.validate()
        if settings.subscriptions.refresh_interval_seconds <= 0:
            raise ValueError("refresh interval must be greater than zero")
        if settings.health.active_pool_relay_check_interval_seconds <= 0:
            raise ValueError("active pool relay check interval must be greater than zero")
        if settings.health.candidate_recheck_interval_seconds <= 0:
            raise ValueError("candidate recheck interval must be greater than zero")
        if settings.health.candidate_batch_size <= 0:
            raise ValueError("candidate batch size must be greater than zero")
        if settings.health.candidate_batch_concurrency <= 0:
            raise ValueError("candidate batch concurrency must be greater than zero")
        if settings.health.candidate_parallel_batches <= 0:
            raise ValueError("candidate parallel batches must be greater than zero")
        if settings.health.candidate_batch_timeout_seconds <= 0:
            raise ValueError("candidate batch timeout must be greater than zero")
        if settings.dead_pool.ttl_hours <= 0:
            raise ValueError("dead pool TTL must be greater than zero")
        if settings.download_test.timeout_seconds <= 0:
            raise ValueError("download_test.timeout_seconds must be greater than zero")
        if settings.download_test.per_url_timeout_seconds <= 0:
            raise ValueError("download_test.per_url_timeout_seconds must be greater than zero")
        if settings.selection.sample_size < 2:
            raise ValueError("selection sample size must be at least 2")
        if settings.vip_port.enabled and settings.vip_port.port <= 0:
            raise ValueError("vip_port.port must be greater than zero")
        if settings.vip_port.check_interval_seconds <= 0:
            raise ValueError("vip_port.check_interval_seconds must be greater than zero")
        if settings.vip_port.min_switch_interval_seconds < 0:
            raise ValueError("vip_port.min_switch_interval_seconds must be non-negative")
        if settings.vip_port.switch_threshold_score_diff < 0:
            raise ValueError("vip_port.switch_threshold_score_diff must be non-negative")
        if settings.network_guard.check_interval_seconds <= 0:
            raise ValueError("network_guard.check_interval_seconds must be greater than zero")
        if settings.network_guard.failure_threshold <= 0:
            raise ValueError("network_guard.failure_threshold must be greater than zero")
        if settings.network_guard.recovery_threshold <= 0:
            raise ValueError("network_guard.recovery_threshold must be greater than zero")
        if settings.network_guard.minimum_successful_targets <= 0:
            raise ValueError("network_guard.minimum_successful_targets must be greater than zero")
        if not 0 <= settings.network_guard.mass_failure_threshold_percent <= 100:
            raise ValueError("network_guard.mass_failure_threshold_percent must be between 0 and 100")
        if settings.database.type.lower() != "sqlite":
            raise ValueError("only sqlite database.type is supported in v1")
