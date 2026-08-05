//! Generation-based source refresh. A partial upstream outage never retires a proxy.

use std::{sync::Arc, time::Duration};

use futures_util::{StreamExt, stream};
use tracing::{info, warn};

use crate::{config::AppConfig, parser::parse_subscription, storage::Store};

#[derive(Debug, Clone, serde::Serialize)]
pub struct RefreshReport {
    pub generation: i64,
    pub complete: bool,
    pub source_count: usize,
    pub successful_sources: usize,
    pub accepted: usize,
    pub rejected: usize,
    pub inserted: usize,
    pub errors: Vec<String>,
}

pub async fn refresh(store: &Store, config: Arc<AppConfig>) -> anyhow::Result<RefreshReport> {
    let sources = config.subscriptions.urls.clone();
    let (run_id, generation) = store.begin_refresh(sources.len()).await?;
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .user_agent("proxy-fleet/0.1")
        .build()?;
    let mut requests = stream::iter(sources.iter().cloned().map(|source| {
        let client = client.clone();
        async move {
            let result = async {
                let response = client.get(&source.url).send().await?.error_for_status()?;
                response.text().await
            }
            .await;
            (source, result)
        }
    }))
    .buffer_unordered(8);
    let mut successful_sources = 0;
    let mut accepted = 0;
    let mut rejected = 0;
    let mut errors = Vec::new();
    let mut parsed = Vec::new();
    while let Some((source, result)) = requests.next().await {
        match result {
            Ok(body) => {
                successful_sources += 1;
                store
                    .record_source_success(&source.name, &source.url)
                    .await?;
                let report = parse_subscription(&body, &source.name);
                accepted += report.accepted.len();
                rejected += report.rejected.len();
                parsed.extend(report.accepted);
            }
            Err(error) => {
                store
                    .record_source_failure(&source.name, &source.url)
                    .await?;
                let message = format!("{}: {}", source.name, error);
                warn!(source = %source.name, error = %error, "subscription refresh failed");
                errors.push(message);
            }
        }
    }
    let inserted = store.ingest_many(&parsed, generation).await? as usize;
    let complete = store
        .finish_refresh(
            &run_id,
            generation,
            sources.len(),
            successful_sources,
            accepted,
            config.subscriptions.complete_generations_before_retire(),
        )
        .await?;
    let report = RefreshReport {
        generation,
        complete,
        source_count: sources.len(),
        successful_sources,
        accepted,
        rejected,
        inserted,
        errors,
    };
    info!(
        generation,
        complete, accepted, inserted, "upstream refresh finished"
    );
    Ok(report)
}
