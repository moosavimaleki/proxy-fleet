//! Cost-aware, multi-queue test scheduling with expiring leases first.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sqlx::Row;

use crate::storage::Store;
use crate::{domain::failure::FailureClass, probe::ProbeReport};

#[derive(Debug, Clone, serde::Serialize)]
pub struct TestJob {
    pub id: String,
    pub raw_config: String,
    pub lifecycle_state: String,
    pub reason: String,
    /// ACTIVE runtimes get cheap relay/HTTP observations frequently. A real
    /// download is reserved for their slower revalidation cadence; every
    /// other lifecycle still needs a real download to become ACTIVE.
    pub download_due: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct QueueQuota {
    pub new: usize,
    pub successful_probation: usize,
    pub recoverable_dormant: usize,
    pub exploration: usize,
}

const QUEUE_ORDER: [(&str, &str); 4] = [
    ("CANDIDATE", "new"),
    ("PROBATION", "successful_probation"),
    ("DORMANT", "recoverable_dormant"),
    ("ACTIVE", "active_revalidation"),
];

/// Persistent, fractional scheduler credits. Each tick contributes the
/// configured 40/30/20/10 share. A successful claim consumes one credit;
/// unused credit is retained (with a bounded cap), so a smaller queue cannot
/// be starved forever by a permanently full candidate queue.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
struct QueueDebt {
    #[serde(default)]
    candidate: f64,
    #[serde(default)]
    probation: f64,
    #[serde(default)]
    dormant: f64,
    #[serde(default)]
    active: f64,
}

impl QueueDebt {
    fn from_value(value: Option<serde_json::Value>) -> Self {
        value
            .and_then(|value| serde_json::from_value(value).ok())
            .unwrap_or_default()
    }

    fn accrue(&mut self, capacity: usize) {
        let capacity = capacity.max(1) as f64;
        let cap = capacity * 4.0;
        self.candidate = (self.candidate + capacity * 0.40).clamp(-1.0, cap);
        self.probation = (self.probation + capacity * 0.30).clamp(-1.0, cap);
        self.dormant = (self.dormant + capacity * 0.20).clamp(-1.0, cap);
        self.active = (self.active + capacity * 0.10).clamp(-1.0, cap);
    }

    fn credit(self, state: &str) -> f64 {
        match state {
            "CANDIDATE" => self.candidate,
            "PROBATION" => self.probation,
            "DORMANT" => self.dormant,
            "ACTIVE" => self.active,
            _ => f64::NEG_INFINITY,
        }
    }

    fn consume(&mut self, state: &str) {
        match state {
            "CANDIDATE" => self.candidate -= 1.0,
            "PROBATION" => self.probation -= 1.0,
            "DORMANT" => self.dormant -= 1.0,
            "ACTIVE" => self.active -= 1.0,
            _ => {}
        }
    }

    fn next_untried(self, tried: &[&str]) -> Option<(&'static str, &'static str)> {
        QUEUE_ORDER
            .iter()
            .copied()
            .filter(|(state, _)| !tried.contains(state))
            .max_by(|(left, _), (right, _)| {
                self.credit(left)
                    .partial_cmp(&self.credit(right))
                    .unwrap_or(std::cmp::Ordering::Equal)
                    // Stable queue order makes equal credits deterministic.
                    .then_with(|| right.cmp(left))
            })
    }
}

/// Detects a correlated failure before the evidence writer sees the batch.
/// A proxy that has any successful stage is not counted as failed.  Structural
/// errors remain per-proxy and are deliberately excluded by the caller.
pub fn is_mass_failure(reports: &[(String, ProbeReport)], threshold_percent: u8) -> bool {
    if reports.len() < 3 {
        return false;
    }
    let failed = reports
        .iter()
        .filter(|(_, report)| {
            let succeeded = report
                .events
                .iter()
                .any(|event| event.class == FailureClass::Success);
            let inconclusive_only = report.events.iter().all(|event| {
                event.class.inconclusive() || event.class == FailureClass::InvalidConfig
            });
            !succeeded && !inconclusive_only && !report.events.is_empty()
        })
        .count();
    failed.saturating_mul(100) >= reports.len().saturating_mul(threshold_percent as usize)
}

impl QueueQuota {
    pub fn for_capacity(capacity: usize) -> Self {
        // Largest-remainder allocation preserves the configured 40/30/20/10
        // policy for large batches and never turns a capacity-1 tick into
        // ACTIVE exploration while fresh candidates are waiting.
        let mut quotas = [
            (capacity * 40 / 100, capacity * 40 % 100, 0_usize),
            (capacity * 30 / 100, capacity * 30 % 100, 1_usize),
            (capacity * 20 / 100, capacity * 20 % 100, 2_usize),
            (capacity * 10 / 100, capacity * 10 % 100, 3_usize),
        ];
        let allocated = quotas.iter().map(|(whole, _, _)| *whole).sum::<usize>();
        quotas.sort_by_key(|(_, remainder, priority)| (std::cmp::Reverse(*remainder), *priority));
        for (whole, _, _) in quotas.iter_mut().take(capacity.saturating_sub(allocated)) {
            *whole += 1;
        }
        quotas.sort_by_key(|(_, _, priority)| *priority);
        Self {
            new: quotas[0].0,
            successful_probation: quotas[1].0,
            recoverable_dormant: quotas[2].0,
            exploration: quotas[3].0,
        }
    }
}

pub async fn claim_due(
    store: &Store,
    capacity: usize,
    lease: Duration,
    active_download_interval: Duration,
) -> anyhow::Result<Vec<TestJob>> {
    let capacity = capacity.max(1);
    let now = Utc::now();
    let mut debt = QueueDebt::from_value(store.scheduler_state("quota_debt").await?);
    debt.accrue(capacity);
    let mut jobs = Vec::with_capacity(capacity);
    while jobs.len() < capacity {
        let mut tried = Vec::with_capacity(QUEUE_ORDER.len());
        let mut claimed = false;
        while let Some((state, reason)) = debt.next_untried(&tried) {
            tried.push(state);
            let before = jobs.len();
            append_claims(
                store,
                &mut jobs,
                capacity,
                ClaimParams {
                    state,
                    queue_limit: 1,
                    reason,
                    now,
                    lease,
                    active_download_interval,
                },
            )
            .await?;
            if jobs.len() > before {
                debt.consume(state);
                claimed = true;
                break;
            }
        }
        if !claimed {
            break;
        }
    }
    if !jobs.is_empty() {
        store
            .set_scheduler_state(
                "quota_debt",
                serde_json::to_value(debt).expect("queue debt is serializable"),
            )
            .await?;
    }
    Ok(jobs)
}

/// A portable, dependency-free pressure signal. Linux values are read from procfs;
/// non-Linux hosts get a neutral signal rather than a false overload verdict.
pub fn system_pressure() -> f64 {
    let cores = std::thread::available_parallelism()
        .map(|value| value.get() as f64)
        .unwrap_or(1.0);
    let load = std::fs::read_to_string("/proc/loadavg")
        .ok()
        .and_then(|value| value.split_whitespace().next()?.parse::<f64>().ok())
        .map(|value| (value / cores).clamp(0.0, 1.0))
        .unwrap_or(0.0);
    let memory = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|value| {
            let mut total = 0_f64;
            let mut available = 0_f64;
            for line in value.lines() {
                if let Some(value) = line
                    .strip_prefix("MemTotal:")
                    .and_then(|item| item.split_whitespace().next())
                    .and_then(|item| item.parse::<f64>().ok())
                {
                    total = value;
                }
                if let Some(value) = line
                    .strip_prefix("MemAvailable:")
                    .and_then(|item| item.split_whitespace().next())
                    .and_then(|item| item.parse::<f64>().ok())
                {
                    available = value;
                }
            }
            (total > 0.0).then(|| (1.0 - available / total).clamp(0.0, 1.0))
        })
        .unwrap_or(0.0);
    load.max(memory)
}

struct ClaimParams<'a> {
    state: &'a str,
    queue_limit: usize,
    reason: &'a str,
    now: DateTime<Utc>,
    lease: Duration,
    active_download_interval: Duration,
}

async fn append_claims(
    store: &Store,
    jobs: &mut Vec<TestJob>,
    total_limit: usize,
    params: ClaimParams<'_>,
) -> anyhow::Result<()> {
    if params.queue_limit == 0 || jobs.len() >= total_limit {
        return Ok(());
    }
    let remaining = params.queue_limit.min(total_limit - jobs.len());
    let state_clause = if params.state.is_empty() {
        "lifecycle_state IN ('CANDIDATE', 'PROBATION', 'DORMANT', 'ACTIVE')"
    } else {
        "lifecycle_state = ?"
    };
    let sql = format!(
        "SELECT id, raw_config, lifecycle_state, last_real_download_at FROM nodes WHERE {state_clause} AND structurally_valid = 1 AND (next_test_at IS NULL OR next_test_at <= ?) AND (test_lease_until IS NULL OR test_lease_until <= ?) ORDER BY CASE WHEN publication_lease_until IS NOT NULL AND publication_lease_until <= ? THEN 100 ELSE 0 END + CASE WHEN last_real_download_at IS NOT NULL THEN 80 ELSE 0 END + CASE WHEN last_failure_class = 'TLS_TIMEOUT' THEN 50 ELSE 0 END + CASE WHEN last_test_at IS NULL OR last_test_at <= ? THEN 40 ELSE 0 END + CASE WHEN last_seen_generation IS NOT NULL THEN 30 ELSE 0 END - CASE WHEN last_real_download_at IS NULL AND failure_streak >= 5 THEN 80 ELSE 0 END DESC, next_test_at ASC LIMIT ?"
    );
    let mut query = sqlx::query(&sql);
    if !params.state.is_empty() {
        query = query.bind(params.state);
    }
    let stale = params.now - Duration::hours(1);
    let rows = query
        .bind(params.now.to_rfc3339())
        .bind(params.now.to_rfc3339())
        .bind((params.now + Duration::hours(1)).to_rfc3339())
        .bind(stale.to_rfc3339())
        .bind(remaining as i64)
        .fetch_all(store.pool())
        .await?;
    let lease_until = params.now + params.lease;
    for row in rows {
        let id: String = row.get("id");
        let update = sqlx::query("UPDATE nodes SET testing_from_state = lifecycle_state, lifecycle_state = 'TESTING', status = 'TESTING', test_lease_until = ?, updated_at = ? WHERE id = ? AND (test_lease_until IS NULL OR test_lease_until <= ?)")
            .bind(lease_until.to_rfc3339()).bind(params.now.to_rfc3339()).bind(&id).bind(params.now.to_rfc3339()).execute(store.pool()).await?;
        if update.rows_affected() == 1 {
            let lifecycle_state: String = row.get("lifecycle_state");
            let last_download = row
                .get::<Option<String>, _>("last_real_download_at")
                .and_then(|value| chrono::DateTime::parse_from_rfc3339(&value).ok())
                .map(|value| value.with_timezone(&Utc));
            jobs.push(TestJob {
                id,
                raw_config: row.get("raw_config"),
                download_due: lifecycle_state != "ACTIVE"
                    || last_download
                        .map(|at| at + params.active_download_interval <= params.now)
                        .unwrap_or(true),
                lifecycle_state,
                reason: params.reason.to_owned(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{
        domain::{evidence::TestStage, failure::FailureClass},
        probe::{ProbeEvent, ProbeReport},
    };

    use super::{QueueDebt, QueueQuota, is_mass_failure};

    fn report(class: FailureClass) -> ProbeReport {
        ProbeReport {
            proxy: None,
            events: vec![ProbeEvent {
                stage: TestStage::Relay,
                class,
                fast_download: false,
                latency_ms: None,
                download_bps: None,
                bytes_transferred: None,
                duration_ms: None,
                endpoint: None,
                detail: serde_json::json!({}),
            }],
        }
    }

    #[test]
    fn queue_quota_conserves_capacity() {
        let quota = QueueQuota::for_capacity(17);
        assert_eq!(
            quota.new + quota.successful_probation + quota.recoverable_dormant + quota.exploration,
            17
        );
    }

    #[test]
    fn queue_quota_prioritizes_candidates_at_small_capacities() {
        assert_eq!(
            QueueQuota::for_capacity(1),
            QueueQuota {
                new: 1,
                successful_probation: 0,
                recoverable_dormant: 0,
                exploration: 0,
            }
        );
        assert_eq!(
            QueueQuota::for_capacity(4),
            QueueQuota {
                new: 2,
                successful_probation: 1,
                recoverable_dormant: 1,
                exploration: 0,
            }
        );
    }

    #[test]
    fn persistent_quota_debt_prevents_active_starvation() {
        let mut debt = QueueDebt::default();
        let mut selected = Vec::new();
        for _ in 0..10 {
            debt.accrue(2);
            for _ in 0..2 {
                let (state, _) = debt.next_untried(&[]).expect("queue");
                selected.push(state);
                debt.consume(state);
            }
        }
        assert!(selected.contains(&"CANDIDATE"));
        assert!(selected.contains(&"PROBATION"));
        assert!(selected.contains(&"DORMANT"));
        assert!(selected.contains(&"ACTIVE"));
    }

    #[test]
    fn correlated_batch_failure_is_detected_but_single_failure_is_not() {
        let reports = vec![
            ("one".to_owned(), report(FailureClass::TcpTimeout)),
            ("two".to_owned(), report(FailureClass::RelayTimeout)),
            ("three".to_owned(), report(FailureClass::Success)),
        ];
        assert!(is_mass_failure(&reports, 60));
        assert!(!is_mass_failure(&reports[..2], 40));
    }
}
