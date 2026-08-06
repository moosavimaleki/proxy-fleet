//! Per-client circuit breaking and weighted power-of-choices selection.

use rand::prelude::IndexedRandom;

use crate::{
    config::AppConfig,
    storage::{CandidateForSelection, Store},
};

#[derive(Debug, Clone, serde::Serialize)]
pub struct BestDecision {
    pub node_id: String,
    pub port: u16,
    pub assignment_id: String,
    pub relay_delay_ms: Option<i64>,
    pub expires_in_seconds: u64,
}

pub async fn best(
    store: &Store,
    config: &AppConfig,
    client: &str,
) -> anyhow::Result<Option<BestDecision>> {
    let candidates = store
        .candidates_for_client(client, config.assignment_ttl_seconds)
        .await?;
    let selectable: Vec<_> = candidates
        .into_iter()
        .filter(|candidate| !candidate.circuit_open)
        .collect();
    if selectable.is_empty() {
        return Ok(None);
    }
    let sample_size = config.selection.sample_size.min(selectable.len()).max(1);
    // ThreadRng is deliberately dropped before the database await below so
    // this future remains Send and can be used by Axum/Tokio workers.
    let (node_id, port, relay_delay_ms) = {
        let mut rng = rand::rng();
        let sample: Vec<&CandidateForSelection> =
            selectable.choose_multiple(&mut rng, sample_size).collect();
        let chosen = sample
            .into_iter()
            .max_by(|left, right| score(left, config).total_cmp(&score(right, config)))
            .expect("non-empty sample");
        (
            chosen.id.clone(),
            chosen
                .main_port
                .context("selected node has no runtime port")? as u16,
            chosen.relay_delay_ms,
        )
    };
    let assignment_id = store.record_assignment(client, &node_id, port).await?;
    Ok(Some(BestDecision {
        node_id,
        port,
        assignment_id,
        relay_delay_ms,
        expires_in_seconds: config.assignment_ttl_seconds,
    }))
}

pub async fn feedback(
    store: &Store,
    config: &AppConfig,
    client: &str,
    node_id: &str,
    status: &str,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        matches!(status, "used" | "broken" | "rate_limited"),
        "invalid feedback status"
    );
    store
        .apply_client_feedback(client, node_id, status, config)
        .await
}

fn score(candidate: &CandidateForSelection, config: &AppConfig) -> f64 {
    let latency = (1.0
        - candidate
            .relay_delay_ms
            .unwrap_or(config.health.max_relay_delay_ms as i64) as f64
            / config.health.max_relay_delay_ms.max(1) as f64)
        .clamp(0.0, 1.0);
    let download = (candidate.download_kbps.unwrap_or_default() as f64
        / config.download_test.target_download_kbps.max(1) as f64)
        .clamp(0.0, 1.0);
    let fairness = 1.0
        / (1.0
            + candidate.active_assignments as f64
            + candidate.recent_global_usage as f64
            + candidate.recent_client_usage as f64);
    let history = candidate.client_success_ewma.unwrap_or(0.5);
    // A lease close to expiry is still selectable, but it receives less
    // availability credit than one with a recently verified publication
    // window. This avoids over-assigning a node that may leave the feed soon.
    let lease_freshness = candidate
        .publication_lease_until
        .map(|until| {
            (until - chrono::Utc::now()).num_seconds().max(0) as f64
                / chrono::Duration::hours(12).num_seconds() as f64
        })
        .unwrap_or_default()
        .clamp(0.0, 1.0);
    let availability = candidate.health_score * (0.75 + 0.25 * lease_freshness);
    let penalty = (candidate.client_fail_streak as f64 * 0.15
        + candidate.client_rate_limit_streak as f64 * 0.25)
        .min(0.5);
    config.selection.weights.latency * latency
        + config.selection.weights.download * download
        + config.selection.weights.availability * availability
        + config.selection.weights.fairness * fairness
        + config.selection.weights.client_history * history
        - penalty
}

trait OptionContext<T> {
    fn context(self, message: &str) -> anyhow::Result<T>;
}
impl<T> OptionContext<T> for Option<T> {
    fn context(self, message: &str) -> anyhow::Result<T> {
        self.ok_or_else(|| anyhow::anyhow!(message.to_owned()))
    }
}
