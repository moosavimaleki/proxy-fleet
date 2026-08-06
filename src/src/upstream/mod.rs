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

enum FetchResult {
    Body {
        body: String,
        etag: Option<String>,
        last_modified: Option<String>,
    },
    NotModified,
}

pub async fn refresh(store: &Store, config: Arc<AppConfig>) -> anyhow::Result<RefreshReport> {
    let sources = config.subscriptions.urls.clone();
    let (run_id, generation) = store.begin_refresh(sources.len()).await?;
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .user_agent("proxy-fleet/0.1")
        .build()?;
    let mut cached_sources = Vec::with_capacity(sources.len());
    for source in &sources {
        let (etag, last_modified) = store.source_http_cache(&source.name).await?;
        cached_sources.push((source.clone(), etag, last_modified));
    }
    let mut requests = stream::iter(cached_sources.into_iter().map(
        |(source, etag, last_modified)| {
            let client = client.clone();
            async move {
                let result: anyhow::Result<FetchResult> = async {
                    let mut request = client.get(&source.url);
                    if let Some(etag) = etag.as_deref() {
                        request = request.header(reqwest::header::IF_NONE_MATCH, etag);
                    }
                    if let Some(last_modified) = last_modified.as_deref() {
                        request = request.header(reqwest::header::IF_MODIFIED_SINCE, last_modified);
                    }
                    let response = request.send().await?;
                    if response.status() == reqwest::StatusCode::NOT_MODIFIED {
                        return Ok(FetchResult::NotModified);
                    }
                    let response = response.error_for_status()?;
                    let headers = response.headers();
                    let etag = headers
                        .get(reqwest::header::ETAG)
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_owned);
                    let last_modified = headers
                        .get(reqwest::header::LAST_MODIFIED)
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_owned);
                    let body = response.text().await?;
                    Ok(FetchResult::Body {
                        body,
                        etag,
                        last_modified,
                    })
                }
                .await;
                (source, result)
            }
        },
    ))
    .buffer_unordered(8);
    let mut successful_sources = 0;
    let mut accepted = 0;
    let mut rejected = 0;
    let mut errors = Vec::new();
    let mut parsed = Vec::new();
    while let Some((source, result)) = requests.next().await {
        match result {
            Ok(FetchResult::Body {
                body,
                etag,
                last_modified,
            }) => {
                successful_sources += 1;
                store
                    .record_source_success(
                        &source.name,
                        &source.url,
                        etag.as_deref(),
                        last_modified.as_deref(),
                    )
                    .await?;
                let report = parse_subscription(&body, &source.name);
                accepted += report.accepted.len();
                rejected += report.rejected.len();
                store
                    .record_invalid_config_rejections(&source.name, &report.rejected)
                    .await?;
                parsed.extend(report.accepted);
            }
            Ok(FetchResult::NotModified) => {
                successful_sources += 1;
                let copied = store
                    .copy_cached_source_generation(&source.name, generation)
                    .await?;
                store
                    .record_source_success(&source.name, &source.url, None, None)
                    .await?;
                accepted += copied as usize;
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
