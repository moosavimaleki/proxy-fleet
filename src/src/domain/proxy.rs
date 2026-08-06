use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LifecycleState {
    Candidate,
    Testing,
    Active,
    Probation,
    Dormant,
    Invalid,
    Retired,
    WaitingForPort,
}

impl LifecycleState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Candidate => "CANDIDATE",
            Self::Testing => "TESTING",
            Self::Active => "ACTIVE",
            Self::Probation => "PROBATION",
            Self::Dormant => "DORMANT",
            Self::Invalid => "INVALID",
            Self::Retired => "RETIRED",
            Self::WaitingForPort => "WAITING_FOR_PORT",
        }
    }
    pub const fn publishable_state(self) -> bool {
        !matches!(self, Self::Invalid | Self::Retired)
    }
}

impl std::str::FromStr for LifecycleState {
    type Err = &'static str;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(match value {
            "CANDIDATE" => Self::Candidate,
            "TESTING" => Self::Testing,
            "ACTIVE" => Self::Active,
            "PROBATION" => Self::Probation,
            "DORMANT" | "DEAD" => Self::Dormant,
            "INVALID" => Self::Invalid,
            "RETIRED" | "REMOVED" => Self::Retired,
            "WAITING_FOR_PORT" => Self::WaitingForPort,
            _ => return Err("unknown lifecycle state"),
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeSummary {
    pub id: String,
    pub config_hash: String,
    /// Needed while reading a row to derive display metadata, but credentials
    /// must only be returned by the explicit `/:id/config` endpoint.
    #[serde(skip_serializing)]
    pub raw_config: String,
    pub protocol: String,
    pub server: String,
    pub remote_port: Option<i64>,
    pub remark: String,
    pub source_subs: Vec<String>,
    pub status: String,
    pub lifecycle_state: String,
    pub main_port: Option<i64>,
    pub relay_delay_ms: Option<i64>,
    pub download_kbps: Option<i64>,
    pub exit_country: String,
    pub exit_ip: String,
    pub exit_hostname: String,
    pub exit_city: String,
    pub exit_region: String,
    pub exit_loc: String,
    pub exit_org: String,
    pub exit_postal: String,
    pub exit_timezone: String,
    pub exit_info_fetched_at: Option<DateTime<Utc>>,
    pub health_success_ewma: f64,
    pub health_alpha: f64,
    pub health_beta: f64,
    pub health_score: f64,
    pub next_test_at: Option<DateTime<Utc>>,
    pub publication_lease_until: Option<DateTime<Utc>>,
    pub publication_lease_kind: Option<String>,
    pub last_failure_class: Option<String>,
    pub last_seen_generation: Option<i64>,
    pub upstream_missing_generations: i64,
    pub evidence_summary: EvidenceSummary,
    pub created_at: Option<DateTime<Utc>>,
    pub last_test_at: Option<DateTime<Utc>>,
}

/// Public information observed after a successful proxy download.  It is
/// deliberately independent from probe evidence: a metadata provider outage
/// can never demote a usable proxy.
#[derive(Debug, Clone)]
pub struct ExitMetadata {
    pub ip: String,
    pub hostname: String,
    pub city: String,
    pub region: String,
    pub country: String,
    pub loc: String,
    pub org: String,
    pub postal: String,
    pub timezone: String,
    pub raw: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct EvidenceSummary {
    pub alpha: f64,
    pub beta: f64,
    pub score: f64,
    pub last_failure_class: Option<String>,
}
