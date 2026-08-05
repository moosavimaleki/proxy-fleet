//! Cost-aware, multi-queue test scheduling with expiring leases first.

use chrono::{DateTime, Duration, Utc};
use sqlx::Row;

use crate::storage::Store;

#[derive(Debug, Clone, serde::Serialize)]
pub struct TestJob {
    pub id: String,
    pub raw_config: String,
    pub lifecycle_state: String,
    pub reason: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct QueueQuota {
    pub new: usize,
    pub successful_probation: usize,
    pub recoverable_dormant: usize,
    pub exploration: usize,
}

impl QueueQuota {
    pub fn for_capacity(capacity: usize) -> Self {
        let new = capacity * 40 / 100;
        let successful_probation = capacity * 30 / 100;
        let recoverable_dormant = capacity * 20 / 100;
        Self {
            new,
            successful_probation,
            recoverable_dormant,
            exploration: capacity.saturating_sub(new + successful_probation + recoverable_dormant),
        }
    }
}

pub async fn claim_due(
    store: &Store,
    capacity: usize,
    lease: Duration,
) -> anyhow::Result<Vec<TestJob>> {
    let quota = QueueQuota::for_capacity(capacity.max(1));
    let now = Utc::now();
    let mut jobs = Vec::with_capacity(capacity);
    for (state, limit, reason) in [
        ("CANDIDATE", quota.new, "new"),
        (
            "PROBATION",
            quota.successful_probation,
            "successful_probation",
        ),
        ("DORMANT", quota.recoverable_dormant, "recoverable_dormant"),
    ] {
        append_claims(
            store,
            &mut jobs,
            capacity,
            ClaimParams {
                state,
                queue_limit: limit,
                reason,
                now,
                lease,
            },
        )
        .await?;
    }
    if jobs.len() < capacity {
        let exploration_limit = quota.exploration.max(capacity - jobs.len());
        append_claims(
            store,
            &mut jobs,
            capacity,
            ClaimParams {
                state: "ACTIVE",
                queue_limit: exploration_limit,
                reason: "active_revalidation",
                now,
                lease,
            },
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
        "SELECT id, raw_config, lifecycle_state FROM nodes WHERE {state_clause} AND structurally_valid = 1 AND (next_test_at IS NULL OR next_test_at <= ?) AND (test_lease_until IS NULL OR test_lease_until <= ?) ORDER BY CASE WHEN publication_lease_until IS NOT NULL AND publication_lease_until <= ? THEN 100 ELSE 0 END + CASE WHEN last_real_download_at IS NOT NULL THEN 80 ELSE 0 END + CASE WHEN last_failure_class = 'TLS_TIMEOUT' THEN 50 ELSE 0 END + CASE WHEN last_test_at IS NULL OR last_test_at <= ? THEN 40 ELSE 0 END + CASE WHEN last_seen_generation IS NOT NULL THEN 30 ELSE 0 END - CASE WHEN last_real_download_at IS NULL AND failure_streak >= 5 THEN 80 ELSE 0 END DESC, next_test_at ASC LIMIT ?"
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
            jobs.push(TestJob {
                id,
                raw_config: row.get("raw_config"),
                lifecycle_state: row.get("lifecycle_state"),
                reason: params.reason.to_owned(),
            });
        }
    }
    Ok(())
}
