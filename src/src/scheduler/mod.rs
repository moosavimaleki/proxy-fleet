//! Cost-aware, multi-queue test scheduling with expiring leases first.

use std::collections::HashMap;

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

#[derive(Debug, Clone, serde::Serialize)]
pub struct SystemMetrics {
    pub pressure: f64,
    pub load_pressure: f64,
    pub memory_pressure: f64,
    pub open_fds: usize,
    pub child_processes: usize,
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

    fn consume(&mut self, state: &str) {
        match state {
            "CANDIDATE" => self.candidate -= 1.0,
            "PROBATION" => self.probation -= 1.0,
            "DORMANT" => self.dormant -= 1.0,
            "ACTIVE" => self.active -= 1.0,
            _ => {}
        }
    }
}

/// Detects a correlated failure before the evidence writer sees the batch.
/// A proxy that has any successful stage is not counted as failed.  Structural
/// errors remain per-proxy and are deliberately excluded by the caller.
pub fn is_mass_failure(reports: &[(String, ProbeReport)], threshold_percent: u8) -> bool {
    correlated_incident(reports, threshold_percent).is_some()
}

/// Classify a batch as a local/shared incident only when failures cross
/// independent proxy dimensions. A single bad source or server must remain
/// ordinary per-proxy evidence; a DNS collapse or endpoint outage across
/// multiple sources/protocols must not demote the fleet.
pub fn correlated_incident(
    reports: &[(String, ProbeReport)],
    threshold_percent: u8,
) -> Option<&'static str> {
    if reports.len() < 3 {
        return None;
    }
    let failed: Vec<_> = reports
        .iter()
        .filter(|(_, report)| {
            let succeeded = report
                .events
                .iter()
                .any(|event| event.class == FailureClass::Success);
            let inconclusive_only = report.events.iter().all(|event| {
                event.class.inconclusive() || event.class == FailureClass::InvalidConfig
            });
            let structurally_invalid = report
                .events
                .iter()
                .any(|event| event.class == FailureClass::InvalidConfig);
            !succeeded && !inconclusive_only && !structurally_invalid && !report.events.is_empty()
        })
        .collect();
    if failed.len().saturating_mul(100) < reports.len().saturating_mul(threshold_percent as usize) {
        return None;
    }
    let mut sources = std::collections::BTreeSet::new();
    let mut protocols = std::collections::BTreeSet::new();
    let mut servers = std::collections::BTreeSet::new();
    let mut dns_failures = 0_usize;
    let mut endpoint_failures = 0_usize;
    for (node_id, report) in &failed {
        if let Some(proxy) = &report.proxy {
            sources.insert(proxy.source.as_str());
            protocols.insert(proxy.protocol.as_str());
            servers.insert(proxy.address.as_str());
        } else {
            // Test fixtures and a rare parser-less report have no transport
            // metadata; distinct node IDs are still safer than declaring a
            // shared incident from one repeated identifier.
            servers.insert(node_id.as_str());
        }
        dns_failures += report
            .events
            .iter()
            .filter(|event| event.class == FailureClass::DnsFailure)
            .count();
        endpoint_failures += report
            .events
            .iter()
            .filter(|event| event.class == FailureClass::EndpointFailure)
            .count();
    }
    let independent = sources.len() >= 2 || protocols.len() >= 2 || servers.len() >= 2;
    if !independent {
        return None;
    }
    if dns_failures.saturating_mul(2) >= failed.len() {
        Some("dns_failure_cluster")
    } else if endpoint_failures.saturating_mul(2) >= failed.len() {
        Some("endpoint_failure_cluster")
    } else {
        Some("cross_source_failure_cluster")
    }
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
    claim_due_with_pressure(
        store,
        capacity,
        lease,
        active_download_interval,
        system_pressure(),
    )
    .await
}

async fn claim_due_with_pressure(
    store: &Store,
    capacity: usize,
    lease: Duration,
    active_download_interval: Duration,
    pressure: f64,
) -> anyhow::Result<Vec<TestJob>> {
    let capacity = capacity.max(1);
    let now = Utc::now();
    let mut debt = QueueDebt::from_value(store.scheduler_state("quota_debt").await?);
    debt.accrue(capacity);
    let mut jobs = Vec::with_capacity(capacity);
    let quota = QueueQuota::for_capacity(capacity);
    let quota_by_state = HashMap::from([
        ("CANDIDATE", quota.new),
        ("PROBATION", quota.successful_probation),
        ("DORMANT", quota.recoverable_dormant),
        ("ACTIVE", quota.exploration),
    ]);
    // The former one-query-per-slot loop re-sorted a large candidate queue
    // for every claim.  Claim a guaranteed quota with one indexed query per
    // queue, then use at most one overflow query per queue.
    for (state, reason) in QUEUE_ORDER {
        let before = jobs.len();
        append_claims(
            store,
            &mut jobs,
            capacity,
            ClaimParams {
                state,
                queue_limit: quota_by_state.get(state).copied().unwrap_or_default(),
                reason,
                now,
                lease,
                active_download_interval,
                pressure,
            },
        )
        .await?;
        for _ in 0..jobs.len() - before {
            debt.consume(state);
        }
    }
    for (state, reason) in QUEUE_ORDER {
        if jobs.len() == capacity {
            break;
        }
        let before = jobs.len();
        let remaining = capacity - before;
        append_claims(
            store,
            &mut jobs,
            capacity,
            ClaimParams {
                state,
                queue_limit: remaining,
                reason,
                now,
                lease,
                active_download_interval,
                pressure,
            },
        )
        .await?;
        for _ in 0..jobs.len() - before {
            debt.consume(state);
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
    let (load, memory) = system_pressure_components();
    load.max(memory)
}

fn system_pressure_components() -> (f64, f64) {
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
    (load, memory)
}

/// Expensive process details are deliberately sampled by the heartbeat, not
/// by each two-second scheduler tick. This makes a growing FD/process count
/// visible before it becomes a local-overload false negative.
pub fn system_metrics() -> SystemMetrics {
    let (load_pressure, memory_pressure) = system_pressure_components();
    let own_pid = std::process::id().to_string();
    let child_processes = std::fs::read_dir("/proc")
        .ok()
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .chars()
                .all(|item| item.is_ascii_digit())
        })
        .filter(|entry| {
            std::fs::read_to_string(entry.path().join("status"))
                .ok()
                .and_then(|status| {
                    status.lines().find_map(|line| {
                        line.strip_prefix("PPid:")
                            .and_then(|value| value.split_whitespace().next())
                            .map(str::to_owned)
                    })
                })
                .as_deref()
                == Some(own_pid.as_str())
        })
        .count();
    let open_fds = std::fs::read_dir("/proc/self/fd")
        .map(|entries| entries.filter_map(Result::ok).count())
        .unwrap_or_default();
    SystemMetrics {
        pressure: load_pressure.max(memory_pressure),
        load_pressure,
        memory_pressure,
        open_fds,
        child_processes,
    }
}

struct ClaimParams<'a> {
    state: &'a str,
    queue_limit: usize,
    reason: &'a str,
    now: DateTime<Utc>,
    lease: Duration,
    active_download_interval: Duration,
    pressure: f64,
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
    // WAITING_FOR_PORT is an execution state whose saved origin determines
    // its fair queue. Keep it as a separate indexed branch instead of using
    // CASE in WHERE: CASE prevents SQLite from seeking on lifecycle_state.
    // The scheduler always claims a concrete queue, so one bind is enough.
    let sql = "WITH eligible AS (
            SELECT id, raw_config, lifecycle_state AS effective_state,
                   last_real_download_at, publication_lease_until,
                   last_failure_class, last_test_at, last_seen_generation,
                   failure_streak, config_hash, next_test_at
              FROM nodes
             WHERE lifecycle_state = ?
               AND structurally_valid = 1
               AND (next_test_at IS NULL OR next_test_at <= ?)
               AND (test_lease_until IS NULL OR test_lease_until <= ?)
            UNION ALL
            SELECT id, raw_config, waiting_from_state AS effective_state,
                   last_real_download_at, publication_lease_until,
                   last_failure_class, last_test_at, last_seen_generation,
                   failure_streak, config_hash, next_test_at
              FROM nodes
             WHERE lifecycle_state = 'WAITING_FOR_PORT'
               AND waiting_from_state = ?
               AND structurally_valid = 1
               AND (next_test_at IS NULL OR next_test_at <= ?)
               AND (test_lease_until IS NULL OR test_lease_until <= ?)
        )
        SELECT id, raw_config, effective_state, last_real_download_at
          FROM eligible
         ORDER BY CASE WHEN publication_lease_until IS NOT NULL AND publication_lease_until <= ? THEN 100 ELSE 0 END
                + CASE WHEN last_real_download_at IS NOT NULL THEN 80 ELSE 0 END
                + CASE WHEN last_failure_class = 'TLS_TIMEOUT' THEN 50 ELSE 0 END
                + CASE WHEN last_test_at IS NULL OR last_test_at <= ? THEN 40 ELSE 0 END
                + CASE WHEN last_seen_generation IS NOT NULL THEN 30 ELSE 0 END
                - CASE WHEN last_real_download_at IS NULL AND failure_streak >= 5 THEN 80 ELSE 0 END
                - CASE WHEN ? >= 0.75 AND last_real_download_at IS NULL THEN 100 ELSE 0 END DESC,
                  ((unicode(substr(config_hash, 1, 1)) * 31 + unicode(substr(config_hash, 2, 1))) % 17) ASC,
                  config_hash ASC, next_test_at ASC
         LIMIT ?";
    let query = sqlx::query(sql);
    let stale = params.now - Duration::hours(1);
    let rows = query
        .bind(params.state)
        .bind(params.now.to_rfc3339())
        .bind(params.now.to_rfc3339())
        .bind(params.state)
        .bind(params.now.to_rfc3339())
        .bind(params.now.to_rfc3339())
        .bind((params.now + Duration::hours(1)).to_rfc3339())
        .bind(stale.to_rfc3339())
        .bind(params.pressure.clamp(0.0, 1.0))
        .bind(remaining as i64)
        .fetch_all(store.pool())
        .await?;
    let lease_until = params.now + params.lease;
    for row in rows {
        let id: String = row.get("id");
        let update = sqlx::query("UPDATE nodes SET testing_from_state = CASE WHEN lifecycle_state = 'WAITING_FOR_PORT' THEN COALESCE(waiting_from_state, 'CANDIDATE') ELSE lifecycle_state END, waiting_from_state = NULL, lifecycle_state = 'TESTING', status = 'TESTING', test_lease_until = ?, updated_at = ? WHERE id = ? AND (test_lease_until IS NULL OR test_lease_until <= ?)")
            .bind(lease_until.to_rfc3339()).bind(params.now.to_rfc3339()).bind(&id).bind(params.now.to_rfc3339()).execute(store.pool()).await?;
        if update.rows_affected() == 1 {
            let lifecycle_state: String = row.get("effective_state");
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
    use sqlx::Row;

    use crate::{
        domain::{evidence::TestStage, failure::FailureClass},
        probe::{ProbeEvent, ProbeReport},
    };

    use super::{
        QueueDebt, QueueQuota, claim_due, claim_due_with_pressure, is_mass_failure, system_metrics,
    };
    use crate::storage::Store;

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
        for _ in 0..10 {
            debt.accrue(2);
            debt.consume("CANDIDATE");
            debt.consume("PROBATION");
            debt.consume("DORMANT");
            debt.consume("ACTIVE");
        }
        assert!(debt.candidate.is_finite());
        assert!(debt.probation.is_finite());
        assert!(debt.dormant.is_finite());
        assert!(debt.active.is_finite());
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

    #[test]
    fn sampled_system_metrics_are_bounded() {
        let metrics = system_metrics();
        assert!((0.0..=1.0).contains(&metrics.pressure));
        assert!((0.0..=1.0).contains(&metrics.load_pressure));
        assert!((0.0..=1.0).contains(&metrics.memory_pressure));
    }

    #[tokio::test]
    async fn active_test_lease_is_never_claimed_twice() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = Store::connect(temp.path().join("fleet.db"))
            .await
            .expect("connect");
        store.migrate().await.expect("migrate");
        let now = chrono::Utc::now();
        sqlx::query("INSERT INTO nodes(id, config_hash, raw_config, normalized_config, source_subs, status, lifecycle_state, structurally_valid, health_alpha, health_beta, health_score, created_at, updated_at, next_test_at, test_lease_until, testing_from_state) VALUES ('leased', 'lease-hash', 'vless://demo', '{}', '[]', 'TESTING', 'TESTING', 1, 1, 1, 0.5, ?, ?, ?, ?, 'CANDIDATE')")
            .bind(now.to_rfc3339())
            .bind(now.to_rfc3339())
            .bind((now - chrono::Duration::seconds(1)).to_rfc3339())
            .bind((now + chrono::Duration::minutes(1)).to_rfc3339())
            .execute(store.pool())
            .await
            .expect("leased fixture");
        let jobs = claim_due(
            &store,
            4,
            chrono::Duration::seconds(30),
            chrono::Duration::minutes(5),
        )
        .await
        .expect("claim");
        assert!(jobs.is_empty());
    }

    #[tokio::test]
    async fn normal_scheduler_queue_uses_due_index_not_a_table_scan() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = Store::connect(temp.path().join("fleet.db"))
            .await
            .expect("connect");
        store.migrate().await.expect("migrate");
        let plan = sqlx::query(
            "EXPLAIN QUERY PLAN SELECT id FROM nodes
              WHERE lifecycle_state = 'CANDIDATE' AND structurally_valid = 1
                AND (next_test_at IS NULL OR next_test_at <= '2099-01-01T00:00:00+00:00')
                AND (test_lease_until IS NULL OR test_lease_until <= '2099-01-01T00:00:00+00:00')",
        )
        .fetch_all(store.pool())
        .await
        .expect("query plan");
        let details = plan
            .iter()
            .map(|row| row.get::<String, _>("detail"))
            .collect::<Vec<_>>();
        assert!(
            details
                .iter()
                .any(|detail| detail.contains("idx_nodes_lifecycle_due")),
            "scheduler queue must seek the due index, got {details:?}"
        );
        assert!(
            !details
                .iter()
                .any(|detail| detail.starts_with("SCAN nodes")),
            "normal queue may not perform a full nodes scan, got {details:?}"
        );
    }

    #[tokio::test]
    async fn scheduler_tick_remains_bounded_with_a_large_due_queue() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = Store::connect(temp.path().join("fleet.db"))
            .await
            .expect("connect");
        store.migrate().await.expect("migrate");
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            "WITH RECURSIVE sequence(value) AS (
                 SELECT 1 UNION ALL SELECT value + 1 FROM sequence WHERE value < 20000
             )
             INSERT INTO nodes(id, config_hash, raw_config, normalized_config, source_subs,
                               status, lifecycle_state, structurally_valid, health_alpha,
                               health_beta, health_score, created_at, updated_at, next_test_at)
             SELECT printf('queue-%d', value), printf('hash-%d', value),
                    printf('vless://fixture-%d', value), '{}', '[]',
                    'CANDIDATE', 'CANDIDATE', 1, 1, 1, 0.5, ?, ?, ?
               FROM sequence",
        )
        .bind(&now)
        .bind(&now)
        .bind(&now)
        .execute(store.pool())
        .await
        .expect("large queue fixture");
        let started = std::time::Instant::now();
        let jobs = claim_due(
            &store,
            128,
            chrono::Duration::seconds(30),
            chrono::Duration::minutes(5),
        )
        .await
        .expect("large queue claim");
        let elapsed = started.elapsed();
        eprintln!(
            "scheduler large-queue benchmark: 20k due nodes, {} claims, {elapsed:?}",
            jobs.len()
        );
        assert_eq!(jobs.len(), 128);
        assert!(elapsed < std::time::Duration::from_secs(2), "{elapsed:?}");
    }

    #[tokio::test]
    async fn high_pressure_demotes_heavy_work_with_a_deterministic_tie_breaker() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = Store::connect(temp.path().join("fleet.db"))
            .await
            .expect("connect");
        store.migrate().await.expect("migrate");
        let now = chrono::Utc::now();
        for (id, hash, lease, download) in [
            ("heavy", "aa-heavy", Some(now), None),
            ("cheap", "bb-cheap", None, Some(now)),
        ] {
            sqlx::query("INSERT INTO nodes(id, config_hash, raw_config, normalized_config, source_subs, status, lifecycle_state, structurally_valid, health_alpha, health_beta, health_score, created_at, updated_at, next_test_at, publication_lease_until, last_real_download_at) VALUES (?, ?, 'vless://demo', '{}', '[]', 'CANDIDATE', 'CANDIDATE', 1, 1, 1, 0.5, ?, ?, ?, ?, ?)")
                .bind(id).bind(hash).bind(now.to_rfc3339()).bind(now.to_rfc3339()).bind((now - chrono::Duration::seconds(1)).to_rfc3339()).bind(lease.map(|value| value.to_rfc3339())).bind(download.map(|value| value.to_rfc3339()))
                .execute(store.pool()).await.expect("fixture node");
        }
        let low = claim_due_with_pressure(
            &store,
            1,
            chrono::Duration::seconds(30),
            chrono::Duration::minutes(5),
            0.1,
        )
        .await
        .expect("low pressure claim");
        assert_eq!(low[0].id, "heavy");
        sqlx::query("UPDATE nodes SET lifecycle_state = 'CANDIDATE', status = 'CANDIDATE', testing_from_state = NULL, test_lease_until = NULL WHERE id = 'heavy'")
            .execute(store.pool()).await.expect("release first claim");
        let high = claim_due_with_pressure(
            &store,
            1,
            chrono::Duration::seconds(30),
            chrono::Duration::minutes(5),
            0.9,
        )
        .await
        .expect("high pressure claim");
        assert_eq!(high[0].id, "cheap");
    }
}
