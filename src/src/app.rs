use std::{sync::Arc, time::Duration};

use tokio::{sync::RwLock, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{config::AppConfig, storage::Store};

#[derive(Debug, Clone, serde::Serialize)]
pub struct RuntimeStatus {
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub scheduler_ticks: u64,
    pub last_tick_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_error: String,
    pub xray_concurrency: usize,
    pub download_concurrency: usize,
    pub system_pressure: f64,
    pub last_scheduler_jobs: usize,
    pub network_incident: bool,
    pub network_message: String,
}

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<AppConfig>,
    pub store: Store,
    pub shutdown: CancellationToken,
    pub runtime: Arc<RwLock<RuntimeStatus>>,
    pub xray_runtimes: crate::xray::runtime::RuntimeManager,
}

impl AppState {
    pub fn new(config: Arc<AppConfig>, store: Store, shutdown: CancellationToken) -> Self {
        Self {
            config,
            store,
            shutdown,
            runtime: Arc::new(RwLock::new(RuntimeStatus {
                started_at: chrono::Utc::now(),
                scheduler_ticks: 0,
                last_tick_at: None,
                last_error: String::new(),
                xray_concurrency: 4,
                download_concurrency: 2,
                system_pressure: 0.0,
                last_scheduler_jobs: 0,
                network_incident: false,
                network_message: "baseline pending".to_owned(),
            })),
            xray_runtimes: crate::xray::runtime::RuntimeManager::default(),
        }
    }

    pub fn spawn_background_services(&self) -> Vec<JoinHandle<()>> {
        vec![
            self.spawn_heartbeat(),
            self.spawn_upstream_refresh(),
            self.spawn_network_guard(),
            self.spawn_scheduler(),
            self.spawn_vip_manager(),
            self.spawn_publisher(),
        ]
    }

    /// Process-local Xray ports cannot survive a restart.  Restore a runtime
    /// for every still-publishable ACTIVE node before serving `/best`; a
    /// failure remains isolated to that node and does not demote its evidence.
    pub async fn reconcile_active_runtimes(&self) -> (usize, usize) {
        let candidates = match self.store.active_runtime_candidates().await {
            Ok(candidates) => candidates,
            Err(error) => {
                warn!(%error, "could not load ACTIVE runtimes for reconciliation");
                return (0, 0);
            }
        };
        let mut started = 0;
        let mut failed = 0;
        for candidate in candidates {
            match self
                .xray_runtimes
                .ensure(
                    &candidate.id,
                    &candidate.raw_config,
                    &self.config,
                    &self.store,
                )
                .await
            {
                Ok(_) => started += 1,
                Err(error) => {
                    failed += 1;
                    warn!(node = %candidate.id, %error, "could not reconcile ACTIVE runtime");
                }
            }
        }
        (started, failed)
    }

    pub async fn shutdown_runtimes(&self) {
        self.xray_runtimes.shutdown(&self.store).await;
    }

    fn spawn_network_guard(&self) -> JoinHandle<()> {
        let state = self.clone();
        tokio::spawn(async move {
            if !state.config.network_guard.enabled {
                return;
            }
            let mut failures = 0_u32;
            let mut recoveries = 0_u32;
            let mut interval = tokio::time::interval(Duration::from_secs(
                state.config.network_guard.check_interval_seconds.max(1),
            ));
            loop {
                tokio::select! {
                    _ = state.shutdown.cancelled() => return,
                    _ = interval.tick() => {
                        let (healthy, message) = sentinel_health(&state.config.network_guard).await;
                        let event = {
                            let mut runtime = state.runtime.write().await;
                            if healthy {
                                failures = 0;
                                recoveries = recoveries.saturating_add(1);
                                if runtime.network_incident && recoveries >= state.config.network_guard.recovery_threshold.max(1) {
                                    runtime.network_incident = false;
                                    runtime.network_message = format!("recovered: {message}");
                                    Some(("INFO", "RECOVERED", runtime.network_message.clone(), serde_json::json!({})))
                                } else {
                                    if !runtime.network_incident { runtime.network_message = message; }
                                    None
                                }
                            } else {
                                recoveries = 0;
                                failures = failures.saturating_add(1);
                                runtime.network_message = message.clone();
                                if !runtime.network_incident && failures >= state.config.network_guard.failure_threshold.max(1) {
                                    runtime.network_incident = true;
                                    Some(("WARN", "STARTED", message, serde_json::json!({"consecutive_failures":failures})))
                                } else { None }
                            }
                        };
                        if let Some((level, kind, message, detail)) = event {
                            let state_value = serde_json::json!({
                                "at": chrono::Utc::now(),
                                "level": level,
                                "kind": kind,
                                "message": message,
                                "details": detail,
                            });
                            if let Err(error) = state.store.record_system_event(level, "incident", kind, &message, detail).await { warn!(%error, "could not record incident transition"); }
                            if let Err(error) = state.store.set_service_state("last_incident", state_value).await { warn!(%error, "could not persist incident service state"); }
                        }
                    }
                }
            }
        })
    }

    fn spawn_heartbeat(&self) -> JoinHandle<()> {
        let state = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = state.shutdown.cancelled() => { info!("background services stopped"); return; }
                    _ = interval.tick() => {
                        let result = state.store.counts().await;
                        let mut runtime = state.runtime.write().await;
                        runtime.scheduler_ticks = runtime.scheduler_ticks.saturating_add(1);
                        runtime.last_tick_at = Some(chrono::Utc::now());
                        if let Err(error) = result { runtime.last_error = error.to_string(); warn!(%error, "storage heartbeat failed"); }
                    }
                }
            }
        })
    }

    fn spawn_upstream_refresh(&self) -> JoinHandle<()> {
        let state = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(
                state.config.subscriptions.refresh_interval_seconds,
            ));
            loop {
                tokio::select! {
                    _ = state.shutdown.cancelled() => return,
                    _ = interval.tick() => {
                        match crate::upstream::refresh(&state.store, state.config.clone()).await {
                            Ok(report) => {
                                let value = serde_json::json!({"at": chrono::Utc::now(), "status": "ok", "report": report});
                                if let Err(error) = state.store.set_service_state("last_refresh", value).await {
                                    warn!(%error, "could not persist refresh service state");
                                }
                            }
                            Err(error) => {
                                let value = serde_json::json!({"at": chrono::Utc::now(), "status": "error", "error": error.to_string()});
                                if let Err(state_error) = state.store.set_service_state("last_refresh", value).await {
                                    warn!(%state_error, "could not persist failed refresh service state");
                                }
                                warn!(%error, "upstream refresh loop failed");
                            }
                        }
                    }
                }
            }
        })
    }

    fn spawn_scheduler(&self) -> JoinHandle<()> {
        let state = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(2));
            loop {
                tokio::select! {
                    _ = state.shutdown.cancelled() => return,
                    _ = interval.tick() => {
                        let pressure = crate::scheduler::system_pressure();
                        let (concurrency, download_concurrency, incident) = {
                            let mut runtime = state.runtime.write().await;
                            runtime.system_pressure = pressure;
                            (runtime.xray_concurrency, runtime.download_concurrency, runtime.network_incident)
                        };
                        if pressure >= 0.90 || incident { continue; }
                        let jobs = match crate::scheduler::claim_due(&state.store, concurrency, chrono::Duration::seconds(state.config.health.candidate_batch_timeout_seconds as i64)).await {
                            Ok(jobs) => jobs,
                            Err(error) => { warn!(%error, "scheduler claim failed"); continue; }
                        };
                        if jobs.is_empty() { continue; }
                        { state.runtime.write().await.last_scheduler_jobs = jobs.len(); }
                        let config = state.config.clone();
                        let store = state.store.clone();
                        let runtimes = state.xray_runtimes.clone();
                        let raw_configs: std::collections::HashMap<_, _> = jobs.iter().map(|job| (job.id.clone(), job.raw_config.clone())).collect();
                        // The batch owns one Xray process with routed SOCKS inbounds.  It
                        // recursively isolates startup failures, then permits only a small
                        // number of bounded downloads through that batch.
                        let reports = crate::probe::test_batch(jobs.into_iter().map(|job| (job.id, job.raw_config)).collect(), "scheduler", &config, download_concurrency).await;
                        let mass_failure = crate::scheduler::is_mass_failure(
                            &reports,
                            config.network_guard.mass_failure_threshold_percent,
                        );
                        let incident_id = mass_failure.then(|| format!("batch-{}", uuid::Uuid::new_v4().simple()));
                        if mass_failure {
                            let message = format!("correlated failure detected across {} scheduler jobs", reports.len());
                            {
                                let mut runtime = state.runtime.write().await;
                                runtime.network_incident = true;
                                runtime.network_message = message.clone();
                            }
                            if let Err(error) = store.record_system_event(
                                "WARN",
                                "incident",
                                "MASS_FAILURE",
                                &message,
                                serde_json::json!({"jobs": reports.len(), "threshold_percent": config.network_guard.mass_failure_threshold_percent}),
                            ).await {
                                warn!(%error, "could not record correlated scheduler failure");
                            }
                            if let Err(error) = store.set_service_state("last_incident", serde_json::json!({
                                "at": chrono::Utc::now(),
                                "level": "WARN",
                                "kind": "MASS_FAILURE",
                                "message": message,
                                "details": {"jobs": reports.len(), "threshold_percent": config.network_guard.mass_failure_threshold_percent},
                            })).await {
                                warn!(%error, "could not persist mass-failure service state");
                            }
                        }
                        let mut results = Vec::with_capacity(reports.len());
                        for (node_id, report) in reports {
                            let mut real_download_success = false;
                            let run_id = uuid::Uuid::new_v4().simple().to_string();
                            for probe_event in report.events {
                                real_download_success |= probe_event.stage == crate::domain::evidence::TestStage::Download
                                    && probe_event.class == crate::domain::failure::FailureClass::Success;
                                let class = if mass_failure && probe_event.class != crate::domain::failure::FailureClass::Success && probe_event.class != crate::domain::failure::FailureClass::InvalidConfig {
                                    crate::domain::failure::FailureClass::EndpointFailure
                                } else { probe_event.class };
                                let event = crate::storage::TestEventInput { proxy_id: node_id.clone(), run_id: run_id.clone(), stage: probe_event.stage, class, fast_download: probe_event.fast_download, latency_ms: probe_event.latency_ms, download_bps: probe_event.download_bps, bytes_transferred: probe_event.bytes_transferred, duration_ms: probe_event.duration_ms, endpoint: probe_event.endpoint, system_pressure: Some(crate::scheduler::system_pressure()), incident_id: incident_id.clone(), detail_json: probe_event.detail };
                                if let Err(error) = store.apply_test_event(event, Duration::from_secs(config.health.active_pool_download_check_interval_seconds)).await { warn!(node = %node_id, %error, "could not persist test event"); }
                            }
                            if real_download_success {
                                if let Some(raw_config) = raw_configs.get(&node_id) {
                                    if let Err(error) = runtimes.ensure(&node_id, raw_config, &config, &store).await { warn!(node = %node_id, %error, "could not start persistent runtime"); }
                                }
                            }
                            let _ = store.release_test_lease(&node_id).await;
                            results.push(real_download_success);
                        }
                        let successes = results.iter().filter(|value| **value).count();
                        let mut runtime = state.runtime.write().await;
                        let recovered = successes == results.len() && pressure < 0.65;
                        if recovered {
                            runtime.xray_concurrency = (runtime.xray_concurrency + 1).min(config.health.candidate_batch_concurrency.max(4));
                            runtime.download_concurrency = (runtime.download_concurrency + 1).min(8);
                        }
                        if successes < results.len() || pressure >= 0.75 {
                            runtime.xray_concurrency = ((runtime.xray_concurrency as f64 * 0.7).floor() as usize).max(1);
                            runtime.download_concurrency = ((runtime.download_concurrency as f64 * 0.7).floor() as usize).max(1);
                        }
                        let scheduler_state = serde_json::json!({
                            "at": chrono::Utc::now(),
                            "pressure": pressure,
                            "xray_concurrency": runtime.xray_concurrency,
                            "download_concurrency": runtime.download_concurrency,
                            "last_scheduler_jobs": runtime.last_scheduler_jobs,
                            "network_incident": runtime.network_incident,
                            "last_recovery_at": recovered.then(chrono::Utc::now),
                        });
                        drop(runtime);
                        if let Err(error) = store.set_scheduler_state("runtime", scheduler_state).await {
                            warn!(%error, "could not persist scheduler runtime state");
                        }
                    }
                }
            }
        })
    }

    fn spawn_vip_manager(&self) -> JoinHandle<()> {
        let state = self.clone();
        tokio::spawn(async move {
            if !state.config.vip_port.enabled {
                return;
            }
            let mut interval = tokio::time::interval(Duration::from_secs(
                state.config.vip_port.check_interval_seconds.max(1),
            ));
            loop {
                tokio::select! {
                    _ = state.shutdown.cancelled() => return,
                    _ = interval.tick() => {
                        if state.runtime.read().await.network_incident { continue; }
                        if let Err(error) = state.xray_runtimes.maintain_vip(&state.config, &state.store).await {
                            warn!(%error, "VIP runtime maintenance failed");
                        }
                    }
                }
            }
        })
    }

    fn spawn_publisher(&self) -> JoinHandle<()> {
        let state = self.clone();
        tokio::spawn(async move {
            if !state.config.publishing.enabled {
                return;
            }
            let mut interval = tokio::time::interval(Duration::from_secs(
                state.config.publishing.reconcile_interval_seconds,
            ));
            loop {
                tokio::select! {
                    _ = state.shutdown.cancelled() => return,
                    _ = interval.tick() => {
                        match crate::publisher::publish(&state.store, &state.config.database.path, &state.config.publishing).await {
                            Ok(result) => {
                                let value = serde_json::json!({"at": chrono::Utc::now(), "status": "ok", "result": result});
                                if let Err(error) = state.store.set_service_state("last_publisher", value).await {
                                    warn!(%error, "could not persist publisher service state");
                                }
                                info!(active = result.active_count, changed = result.changed, pushed = result.pushed, commit = %result.commit, "lease publication reconciled");
                            }
                            Err(error) => {
                                let value = serde_json::json!({"at": chrono::Utc::now(), "status": "error", "error": error.to_string()});
                                if let Err(state_error) = state.store.set_service_state("last_publisher", value).await {
                                    warn!(%state_error, "could not persist failed publisher service state");
                                }
                                warn!(%error, "subscription publisher failed");
                            }
                        }
                    }
                }
            }
        })
    }
}

async fn sentinel_health(config: &crate::config::NetworkGuardConfig) -> (bool, String) {
    if config.sentinel_targets.is_empty() {
        return (true, "no sentinel targets configured".to_owned());
    }
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(4))
        .build();
    let Ok(client) = client else {
        return (false, "could not build sentinel client".to_owned());
    };
    let mut successful = 0_usize;
    let mut http_success = !config.require_http_success;
    for target in &config.sentinel_targets {
        let ok = if target.kind.eq_ignore_ascii_case("http") {
            client
                .get(&target.url)
                .send()
                .await
                .map(|response| {
                    response.status().is_success() || response.status().is_redirection()
                })
                .unwrap_or(false)
        } else {
            tokio::time::timeout(
                Duration::from_secs(2),
                tokio::net::TcpStream::connect((target.host.as_str(), target.port)),
            )
            .await
            .map(|result| result.is_ok())
            .unwrap_or(false)
        };
        if ok {
            successful += 1;
            if target.kind.eq_ignore_ascii_case("http") {
                http_success = true;
            }
        }
    }
    let healthy = successful >= config.network_guard_minimum_successes() && http_success;
    (
        healthy,
        format!(
            "sentinel successes={successful}/{} http_success={http_success}",
            config.sentinel_targets.len()
        ),
    )
}

trait NetworkGuardMinimum {
    fn network_guard_minimum_successes(&self) -> usize;
}
impl NetworkGuardMinimum for crate::config::NetworkGuardConfig {
    fn network_guard_minimum_successes(&self) -> usize {
        self.minimum_successful_targets
            .max(1)
            .min(self.sentinel_targets.len().max(1))
    }
}
