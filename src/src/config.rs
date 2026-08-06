use std::{fs, path::Path};

use anyhow::Context;
use serde::Deserialize;

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
    pub assignment_ttl_seconds: u64,
    pub xray_bin: String,
}

impl AppConfig {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let raw = fs::read_to_string(path.as_ref())
            .with_context(|| format!("reading {}", path.as_ref().display()))?;
        let config: Self = yaml_serde::from_str(&raw).context("parsing YAML configuration")?;
        config.validate()?;
        Ok(config)
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
