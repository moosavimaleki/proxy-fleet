use std::{env, fs, path::Path};

use anyhow::Context;
use serde::Deserialize;
use tracing::{info, warn};

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    pub service: ServiceConfig,
    pub publishing: PublishingConfig,
    pub subscriptions: SubscriptionConfig,
    pub ports: PortsConfig,
    pub database: DatabaseConfig,
    pub health: HealthConfig,
    pub metadata: MetadataConfig,
    pub download_test: DownloadTestConfig,
    pub dead_pool: DeadPoolConfig,
    pub client_penalty: ClientPenaltyConfig,
    pub selection: SelectionConfig,
    pub api: ApiConfig,
    pub vip_port: VipPortConfig,
    pub network_guard: NetworkGuardConfig,
    pub retention: RetentionConfig,
    pub assignment_ttl_seconds: u64,
    pub xray_bin: String,
}

impl AppConfig {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let raw = fs::read_to_string(path.as_ref())
            .with_context(|| format!("reading {}", path.as_ref().display()))?;
        warn_unknown_keys(&raw)?;
        let mut config: Self = yaml_serde::from_str(&raw).context("parsing YAML configuration")?;
        config.apply_environment()?;
        config.validate()?;
        config.log_migration_report();
        Ok(config)
    }

    /// Small, explicit environment override surface for containers.  Secrets
    /// deliberately do not belong here and no environment value is logged.
    fn apply_environment(&mut self) -> anyhow::Result<()> {
        if let Ok(value) = env::var("PROXY_FLEET_API_HOST") {
            self.api.host = value;
        }
        if let Some(value) = env_number::<u16>("PROXY_FLEET_API_PORT")? {
            self.api.port = value;
        }
        if let Ok(value) = env::var("PROXY_FLEET_DATABASE_PATH") {
            self.database.path = value;
        }
        if let Ok(value) = env::var("PROXY_FLEET_XRAY_BIN") {
            self.xray_bin = value;
        }
        if let Some(value) = env_bool("PROXY_FLEET_PUBLISHING_ENABLED")? {
            self.publishing.enabled = value;
        }
        Ok(())
    }

    fn log_migration_report(&self) {
        let retired_snapshot_setting = self.publishing.retained_snapshots.is_some();
        let requested_prune = self.subscriptions.prune_missing_after_cycles;
        info!(
            subscriptions = self.subscriptions.urls.len(),
            refresh_seconds = self.subscriptions.refresh_interval_seconds,
            requested_prune_cycles = requested_prune,
            effective_minimum_generations = self.subscriptions.complete_generations_before_retire(),
            retained_snapshots_ignored = retired_snapshot_setting,
            recent_success_retention_hours = self.health.recent_success_retention_hours,
            "configuration migration report: preserved, converted, and deprecated keys"
        );
        if requested_prune < 3 {
            warn!(
                requested_prune,
                effective = 3,
                "deprecated prune_missing_after_cycles is raised to three complete generations"
            );
        }
        if retired_snapshot_setting {
            warn!(
                "deprecated publishing.retained_snapshots is ignored; publication leases control retention"
            );
        }
    }

    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.subscriptions.urls.is_empty(),
            "subscriptions.urls must not be empty"
        );
        anyhow::ensure!(
            self.subscriptions.refresh_interval_seconds > 0,
            "subscription refresh interval must be positive"
        );
        anyhow::ensure!(
            self.ports.main.valid() && self.ports.test.valid(),
            "invalid port range"
        );
        anyhow::ensure!(
            self.ports.main.end < self.ports.test.start
                || self.ports.test.end < self.ports.main.start,
            "main/test port ranges must not overlap"
        );
        anyhow::ensure!(self.api.port > 0, "api.port must be positive");
        anyhow::ensure!(
            self.retention.system_event_max_rows >= 100,
            "retention.system_event_max_rows must be at least 100"
        );
        anyhow::ensure!(
            self.health.candidate_cycle_limit > 0,
            "candidate cycle limit must be positive"
        );
        anyhow::ensure!(
            self.health.xray_concurrency_min > 0
                && self.health.xray_concurrency_min <= self.health.xray_concurrency_max
                && self.health.download_concurrency_min > 0
                && self.health.download_concurrency_min <= self.health.download_concurrency_max,
            "adaptive concurrency ranges must be positive and ordered"
        );
        anyhow::ensure!(
            self.health.http_probe_max_endpoints > 0,
            "health.http_probe_max_endpoints must be positive"
        );
        anyhow::ensure!(
            self.health.http_probe_success_quorum > 0,
            "health.http_probe_success_quorum must be positive"
        );
        anyhow::ensure!(
            self.health.http_probe_body_limit_bytes > 0,
            "health.http_probe_body_limit_bytes must be positive"
        );
        anyhow::ensure!(
            (15 * 60..=30 * 60).contains(&self.health.active_min_residence_seconds),
            "health.active_min_residence_seconds must be between 900 and 1800 seconds"
        );
        anyhow::ensure!(
            self.download_test.timeout_seconds > 0,
            "download timeout must be positive"
        );
        anyhow::ensure!(
            self.selection.sample_size >= 2,
            "selection sample size must be at least 2"
        );
        Ok(())
    }
}

fn env_number<T>(name: &str) -> anyhow::Result<Option<T>>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    env::var(name)
        .ok()
        .map(|value| {
            value
                .parse::<T>()
                .map_err(|error| anyhow::anyhow!("{name} must be a valid number: {error}"))
        })
        .transpose()
}

fn env_bool(name: &str) -> anyhow::Result<Option<bool>> {
    env::var(name)
        .ok()
        .map(|value| match value.as_str() {
            "1" | "true" | "TRUE" | "yes" | "YES" => Ok(true),
            "0" | "false" | "FALSE" | "no" | "NO" => Ok(false),
            _ => anyhow::bail!("{name} must be a boolean"),
        })
        .transpose()
}

fn warn_unknown_keys(raw: &str) -> anyhow::Result<()> {
    let value: yaml_serde::Value =
        yaml_serde::from_str(raw).context("parsing YAML configuration")?;
    inspect_mapping(&value, "")
}

fn inspect_mapping(value: &yaml_serde::Value, path: &str) -> anyhow::Result<()> {
    let yaml_serde::Value::Mapping(mapping) = value else {
        anyhow::bail!(
            "configuration section {} must be a YAML mapping",
            if path.is_empty() { "<root>" } else { path }
        );
    };
    let known = known_keys(path).unwrap_or(&[]);
    for (key, _) in mapping.iter() {
        match key.as_str() {
            Some(name) if known.contains(&name) => {}
            Some(name) => warn!(path, key = name, "unknown configuration key is ignored"),
            None => warn!(path, "non-string configuration key is ignored"),
        }
    }
    for key in nested_mapping_keys(path) {
        if let Some(value) = mapping.get(*key) {
            let child_path = if path.is_empty() {
                (*key).to_owned()
            } else {
                format!("{path}.{key}")
            };
            inspect_mapping(value, &child_path)?;
        }
    }
    for (key, item_path) in nested_sequence_keys(path) {
        if let Some(yaml_serde::Value::Sequence(values)) = mapping.get(*key) {
            for value in values {
                inspect_mapping(value, item_path)?;
            }
        }
    }
    Ok(())
}

fn nested_mapping_keys(path: &str) -> &'static [&'static str] {
    match path {
        "" => &[
            "service",
            "publishing",
            "subscriptions",
            "ports",
            "database",
            "health",
            "metadata",
            "download_test",
            "dead_pool",
            "client_penalty",
            "selection",
            "api",
            "vip_port",
            "network_guard",
            "retention",
        ],
        "ports" => &["main", "test"],
        "client_penalty" => &["broken", "rate_limited"],
        "selection" => &["weights"],
        _ => &[],
    }
}

fn nested_sequence_keys(path: &str) -> &'static [(&'static str, &'static str)] {
    match path {
        "subscriptions" => &[("urls", "subscriptions.urls[]")],
        "network_guard" => &[("sentinel_targets", "network_guard.sentinel_targets[]")],
        _ => &[],
    }
}

fn known_keys(path: &str) -> Option<&'static [&'static str]> {
    match path {
        "" => Some(&[
            "service",
            "publishing",
            "subscriptions",
            "ports",
            "database",
            "health",
            "metadata",
            "download_test",
            "dead_pool",
            "client_penalty",
            "selection",
            "api",
            "vip_port",
            "network_guard",
            "retention",
            "assignment_ttl_seconds",
            "xray_bin",
        ]),
        "service" => Some(&["name", "environment"]),
        "publishing" => Some(&[
            "enabled",
            "git_remote",
            "git_branch",
            "debounce_seconds",
            "reconcile_interval_seconds",
            "retained_snapshots",
            "author_name",
            "author_email",
        ]),
        "subscriptions" => Some(&[
            "refresh_interval_seconds",
            "prune_missing_after_cycles",
            "urls",
        ]),
        "subscriptions.urls[]" => Some(&["name", "url"]),
        "ports" => Some(&["main", "test"]),
        "ports.main" | "ports.test" => Some(&["start", "end"]),
        "database" => Some(&["type", "path"]),
        "health" => Some(&[
            "active_min_residence_seconds",
            "active_pool_relay_check_interval_seconds",
            "active_pool_download_check_interval_seconds",
            "active_relay_failure_threshold",
            "probation_recheck_interval_seconds",
            "probation_failure_threshold",
            "probation_success_threshold",
            "candidate_recheck_interval_seconds",
            "candidate_max_failures",
            "candidate_retry_backoff_seconds",
            "candidate_batch_size",
            "candidate_cycle_limit",
            "candidate_batch_concurrency",
            "candidate_parallel_batches",
            "candidate_batch_timeout_seconds",
            "xray_concurrency_min",
            "xray_concurrency_max",
            "download_concurrency_min",
            "download_concurrency_max",
            "recent_success_retention_hours",
            "dead_recheck_batch_size",
            "dead_retry_recent_seconds",
            "dead_retry_unverified_seconds",
            "relay_timeout_ms",
            "max_relay_delay_ms",
            "http_probe_body_limit_bytes",
            "http_probe_max_endpoints",
            "http_probe_success_quorum",
            "test_url",
            "fallback_urls",
        ]),
        "metadata" => Some(&[
            "enabled",
            "endpoint",
            "timeout_seconds",
            "cache_ttl_seconds",
        ]),
        "download_test" => Some(&[
            "enabled",
            "timeout_seconds",
            "per_url_timeout_seconds",
            "min_download_kbps",
            "target_download_kbps",
            "test_url",
            "fallback_urls",
        ]),
        "dead_pool" => Some(&["ttl_hours", "invalid_ttl_hours"]),
        "client_penalty" => Some(&["broken", "rate_limited"]),
        "client_penalty.broken" | "client_penalty.rate_limited" => Some(&[
            "base_cooldown_seconds",
            "max_cooldown_seconds",
            "jitter_ratio",
        ]),
        "selection" => Some(&["strategy", "sample_size", "weights"]),
        "selection.weights" => Some(&[
            "latency",
            "download",
            "availability",
            "fairness",
            "client_history",
        ]),
        "api" => Some(&["host", "port", "auth_enabled"]),
        "vip_port" => Some(&[
            "enabled",
            "port",
            "check_interval_seconds",
            "min_switch_interval_seconds",
            "switch_threshold_score_diff",
        ]),
        "network_guard" => Some(&[
            "enabled",
            "check_interval_seconds",
            "failure_threshold",
            "recovery_threshold",
            "minimum_successful_targets",
            "require_http_success",
            "mass_failure_threshold_percent",
            "sentinel_targets",
        ]),
        "network_guard.sentinel_targets[]" => Some(&["type", "host", "port", "url"]),
        "retention" => Some(&["system_event_max_rows"]),
        _ => None,
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            service: ServiceConfig::default(),
            publishing: PublishingConfig::default(),
            subscriptions: SubscriptionConfig::default(),
            ports: PortsConfig::default(),
            database: DatabaseConfig::default(),
            health: HealthConfig::default(),
            metadata: MetadataConfig::default(),
            download_test: DownloadTestConfig::default(),
            dead_pool: DeadPoolConfig::default(),
            client_penalty: ClientPenaltyConfig::default(),
            selection: SelectionConfig::default(),
            api: ApiConfig::default(),
            vip_port: VipPortConfig::default(),
            network_guard: NetworkGuardConfig::default(),
            retention: RetentionConfig::default(),
            assignment_ttl_seconds: 60,
            xray_bin: "/usr/local/bin/xray".to_owned(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServiceConfig {
    pub name: String,
    pub environment: String,
}
impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            name: "config-orchestrator".to_owned(),
            environment: "production".to_owned(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PublishingConfig {
    pub enabled: bool,
    pub git_remote: String,
    pub git_branch: String,
    pub debounce_seconds: f64,
    pub reconcile_interval_seconds: u64,
    #[serde(default)]
    pub retained_snapshots: Option<u32>,
    pub author_name: String,
    pub author_email: String,
}
impl Default for PublishingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            git_remote: String::new(),
            git_branch: "main".to_owned(),
            debounce_seconds: 2.0,
            reconcile_interval_seconds: 60,
            retained_snapshots: None,
            author_name: "Proxy Fleet".to_owned(),
            author_email: "proxy-fleet@users.noreply.github.com".to_owned(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct SubscriptionConfig {
    pub refresh_interval_seconds: u64,
    pub prune_missing_after_cycles: u32,
    pub urls: Vec<SubscriptionSource>,
}
impl SubscriptionConfig {
    pub fn complete_generations_before_retire(&self) -> u32 {
        self.prune_missing_after_cycles.max(3)
    }
}
#[derive(Debug, Clone, Deserialize)]
pub struct SubscriptionSource {
    pub name: String,
    pub url: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PortsConfig {
    pub main: PortRange,
    pub test: PortRange,
}
impl Default for PortsConfig {
    fn default() -> Self {
        Self {
            main: PortRange {
                start: 30000,
                end: 31999,
            },
            test: PortRange {
                start: 32000,
                end: 39999,
            },
        }
    }
}
#[derive(Debug, Clone, Deserialize)]
pub struct PortRange {
    pub start: u16,
    pub end: u16,
}
impl PortRange {
    fn valid(&self) -> bool {
        self.start > 0 && self.start <= self.end
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DatabaseConfig {
    #[serde(rename = "type")]
    pub kind: String,
    pub path: String,
}
impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            kind: "sqlite".to_owned(),
            path: "./data/app.db".to_owned(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct HealthConfig {
    /// A recently download-verified node is protected from normal lifecycle
    /// demotion for this bounded period.  Keeping it configurable makes the
    /// retention policy explicit while preventing accidental multi-hour
    /// settings that would hide a genuinely broken node for too long.
    pub active_min_residence_seconds: u64,
    pub active_pool_relay_check_interval_seconds: u64,
    pub active_pool_download_check_interval_seconds: u64,
    pub active_relay_failure_threshold: u32,
    pub probation_recheck_interval_seconds: u64,
    pub probation_failure_threshold: u32,
    pub probation_success_threshold: u32,
    pub candidate_recheck_interval_seconds: u64,
    pub candidate_max_failures: u32,
    pub candidate_retry_backoff_seconds: Vec<u64>,
    pub candidate_batch_size: usize,
    pub candidate_cycle_limit: usize,
    pub candidate_batch_concurrency: usize,
    pub candidate_parallel_batches: usize,
    pub candidate_batch_timeout_seconds: u64,
    pub xray_concurrency_min: usize,
    pub xray_concurrency_max: usize,
    pub download_concurrency_min: usize,
    pub download_concurrency_max: usize,
    pub recent_success_retention_hours: u64,
    pub dead_recheck_batch_size: usize,
    pub dead_retry_recent_seconds: u64,
    pub dead_retry_unverified_seconds: u64,
    pub relay_timeout_ms: u64,
    pub max_relay_delay_ms: u64,
    /// A Stage 3 request reads only this many bytes.  It is enough to prove
    /// that HTTP works without turning every relay revalidation into a
    /// download benchmark.
    pub http_probe_body_limit_bytes: usize,
    /// Number of independently configured HTTP targets attempted at Stage 3.
    pub http_probe_max_endpoints: usize,
    /// Successful targets required to pass Stage 3.  It is clamped to the
    /// number of configured distinct targets at runtime.
    pub http_probe_success_quorum: usize,
    pub test_url: String,
    pub fallback_urls: Vec<String>,
}
impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            active_min_residence_seconds: 30 * 60,
            active_pool_relay_check_interval_seconds: 10,
            active_pool_download_check_interval_seconds: 300,
            active_relay_failure_threshold: 3,
            probation_recheck_interval_seconds: 120,
            probation_failure_threshold: 3,
            probation_success_threshold: 1,
            candidate_recheck_interval_seconds: 60,
            candidate_max_failures: 5,
            candidate_retry_backoff_seconds: vec![300, 1800, 7200, 21600],
            candidate_batch_size: 32,
            candidate_cycle_limit: 256,
            candidate_batch_concurrency: 16,
            candidate_parallel_batches: 4,
            candidate_batch_timeout_seconds: 20,
            xray_concurrency_min: 4,
            xray_concurrency_max: 16,
            download_concurrency_min: 2,
            download_concurrency_max: 8,
            recent_success_retention_hours: 24,
            dead_recheck_batch_size: 32,
            dead_retry_recent_seconds: 7200,
            dead_retry_unverified_seconds: 21600,
            relay_timeout_ms: 3000,
            max_relay_delay_ms: 3000,
            http_probe_body_limit_bytes: 16 * 1024,
            http_probe_max_endpoints: 2,
            http_probe_success_quorum: 1,
            test_url: "https://www.cloudflare.com/cdn-cgi/trace".to_owned(),
            fallback_urls: vec![
                "https://www.gstatic.com/generate_204".to_owned(),
                "https://www.google.com/generate_204".to_owned(),
            ],
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DownloadTestConfig {
    pub enabled: bool,
    pub timeout_seconds: u64,
    pub per_url_timeout_seconds: f64,
    pub min_download_kbps: u64,
    pub target_download_kbps: u64,
    pub test_url: String,
    pub fallback_urls: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct MetadataConfig {
    pub enabled: bool,
    /// The request is made through the verified proxy runtime, never directly
    /// from the host. Any endpoint that returns a JSON exit-IP document can
    /// be used here.
    pub endpoint: String,
    pub timeout_seconds: u64,
    pub cache_ttl_seconds: u64,
}
impl Default for MetadataConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            endpoint: "https://ipapi.co/json/".to_owned(),
            timeout_seconds: 5,
            cache_ttl_seconds: 24 * 60 * 60,
        }
    }
}
impl Default for DownloadTestConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            timeout_seconds: 5,
            per_url_timeout_seconds: 1.5,
            min_download_kbps: 100,
            target_download_kbps: 1000,
            test_url: "https://proof.ovh.net/files/1Mb.dat".to_owned(),
            fallback_urls: vec![],
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DeadPoolConfig {
    pub ttl_hours: u64,
    pub invalid_ttl_hours: u64,
}
impl Default for DeadPoolConfig {
    fn default() -> Self {
        Self {
            ttl_hours: 24,
            invalid_ttl_hours: 720,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ClientPenaltyConfig {
    pub broken: PenaltyRule,
    pub rate_limited: PenaltyRule,
}
impl Default for ClientPenaltyConfig {
    fn default() -> Self {
        Self {
            broken: PenaltyRule {
                base_cooldown_seconds: 300,
                max_cooldown_seconds: 21600,
                jitter_ratio: 0.2,
            },
            rate_limited: PenaltyRule {
                base_cooldown_seconds: 900,
                max_cooldown_seconds: 43200,
                jitter_ratio: 0.2,
            },
        }
    }
}
#[derive(Debug, Clone, Deserialize)]
pub struct PenaltyRule {
    pub base_cooldown_seconds: u64,
    pub max_cooldown_seconds: u64,
    pub jitter_ratio: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SelectionConfig {
    pub strategy: String,
    pub sample_size: usize,
    pub weights: SelectionWeights,
}
impl Default for SelectionConfig {
    fn default() -> Self {
        Self {
            strategy: "weighted_power_of_choices".to_owned(),
            sample_size: 5,
            weights: SelectionWeights::default(),
        }
    }
}
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SelectionWeights {
    pub latency: f64,
    pub download: f64,
    pub availability: f64,
    pub fairness: f64,
    pub client_history: f64,
}
impl Default for SelectionWeights {
    fn default() -> Self {
        Self {
            latency: 0.35,
            download: 0.20,
            availability: 0.20,
            fairness: 0.15,
            client_history: 0.10,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ApiConfig {
    pub host: String,
    pub port: u16,
    pub auth_enabled: bool,
}
impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_owned(),
            port: 8080,
            auth_enabled: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct VipPortConfig {
    pub enabled: bool,
    pub port: u16,
    pub check_interval_seconds: u64,
    pub min_switch_interval_seconds: u64,
    pub switch_threshold_score_diff: f64,
}
impl Default for VipPortConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 5050,
            check_interval_seconds: 10,
            min_switch_interval_seconds: 60,
            switch_threshold_score_diff: 0.15,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct NetworkGuardConfig {
    pub enabled: bool,
    pub check_interval_seconds: u64,
    pub failure_threshold: u32,
    pub recovery_threshold: u32,
    pub minimum_successful_targets: usize,
    pub require_http_success: bool,
    pub mass_failure_threshold_percent: u8,
    pub sentinel_targets: Vec<SentinelTarget>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RetentionConfig {
    pub system_event_max_rows: u64,
}
impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            system_event_max_rows: 10_000,
        }
    }
}
impl Default for NetworkGuardConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            check_interval_seconds: 5,
            failure_threshold: 1,
            recovery_threshold: 1,
            minimum_successful_targets: 2,
            require_http_success: true,
            mass_failure_threshold_percent: 40,
            sentinel_targets: vec![],
        }
    }
}
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SentinelTarget {
    #[serde(rename = "type")]
    pub kind: String,
    pub host: String,
    pub port: u16,
    pub url: String,
}
impl Default for SentinelTarget {
    fn default() -> Self {
        Self {
            kind: "tcp".to_owned(),
            host: String::new(),
            port: 0,
            url: String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_unknown_top_level_keys_without_silently_rejecting_valid_config() {
        let temp = tempfile::NamedTempFile::new().expect("temporary config");
        std::fs::write(
            temp.path(),
            "subscriptions:\n  refresh_interval_seconds: 60\n  urls:\n    - name: fixture\n      url: https://example.invalid/sub\nhealth:\n  active_min_residence_seconds: 900\nunknown_typo: true\n",
        )
        .expect("write config");
        let config = AppConfig::load(temp.path()).expect("config remains loadable");
        assert_eq!(config.subscriptions.urls.len(), 1);
    }

    #[test]
    fn rejects_non_mapping_configuration_root() {
        let temp = tempfile::NamedTempFile::new().expect("temporary config");
        std::fs::write(temp.path(), "- invalid\n").expect("write config");
        assert!(AppConfig::load(temp.path()).is_err());
    }
}
