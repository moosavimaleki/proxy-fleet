use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Utc};
use tokio::sync::Mutex;

use crate::{
    config::AppConfig,
    parser::{ParsedProxy, parse_share_url},
    storage::Store,
    xray::{XraySession, allocate_port},
};

pub struct PersistentRuntime {
    pub port: u16,
    pub started_at: DateTime<Utc>,
    session: XraySession,
}

struct VipRuntime {
    node_id: String,
    score: f64,
    raw_config: String,
    started_at: DateTime<Utc>,
    session: XraySession,
}

#[derive(Clone, Default)]
pub struct RuntimeManager {
    inner: Arc<Mutex<HashMap<String, PersistentRuntime>>>,
    vip: Arc<Mutex<Option<VipRuntime>>>,
}

impl RuntimeManager {
    pub async fn port_for(&self, node_id: &str) -> Option<u16> {
        self.inner
            .lock()
            .await
            .get(node_id)
            .map(|runtime| runtime.port)
    }

    pub async fn ensure(
        &self,
        node_id: &str,
        raw_config: &str,
        config: &AppConfig,
        store: &Store,
    ) -> anyhow::Result<u16> {
        if let Some(port) = self.port_for(node_id).await {
            return Ok(port);
        }
        let proxy: ParsedProxy = parse_share_url(raw_config, "runtime")?;
        let reservation = allocate_port(config.ports.main.start..=config.ports.main.end).await?;
        let port = reservation.port();
        let session =
            XraySession::start_with_listen(&config.xray_bin, &proxy, reservation, "0.0.0.0")
                .await?;
        let mut runtimes = self.inner.lock().await;
        let existing_port = runtimes.get(node_id).map(|existing| existing.port);
        if let Some(port) = existing_port {
            drop(runtimes);
            let mut redundant = session;
            redundant.stop().await;
            return Ok(port);
        }
        runtimes.insert(
            node_id.to_owned(),
            PersistentRuntime {
                port,
                started_at: Utc::now(),
                session,
            },
        );
        drop(runtimes);
        store.set_main_port(node_id, port).await?;
        Ok(port)
    }

    pub async fn stop(&self, node_id: &str, store: &Store) {
        let runtime = self.inner.lock().await.remove(node_id);
        if let Some(mut runtime) = runtime {
            runtime.session.stop().await;
        }
        let _ = store.clear_main_port(node_id).await;
    }

    /// Explicitly tear down every process owned by this service.  This is
    /// called from the application shutdown path rather than relying on Drop,
    /// so each child receives the graceful termination/reap sequence.
    pub async fn shutdown(&self, store: &Store) -> (usize, usize) {
        let runtimes = {
            let mut inner = self.inner.lock().await;
            std::mem::take(&mut *inner)
        };
        let runtime_count = runtimes.len();
        for (node_id, mut runtime) in runtimes {
            runtime.session.stop().await;
            if let Err(error) = store.clear_main_port(&node_id).await {
                tracing::warn!(node = %node_id, %error, "could not clear runtime port during shutdown");
            }
        }
        let vip = self.vip.lock().await.take();
        let vip_count = usize::from(vip.is_some());
        if let Some(mut vip) = vip {
            vip.session.stop().await;
        }
        (runtime_count, vip_count)
    }

    pub async fn active_count(&self) -> usize {
        self.inner.lock().await.len()
    }

    pub async fn vip_status(&self) -> Option<(String, f64, DateTime<Utc>)> {
        self.vip
            .lock()
            .await
            .as_ref()
            .map(|vip| (vip.node_id.clone(), vip.score, vip.started_at))
    }

    pub async fn maintain_vip(&self, config: &AppConfig, store: &Store) -> anyhow::Result<()> {
        if !config.vip_port.enabled {
            return Ok(());
        }
        let Some(candidate) = store.best_vip_candidate().await? else {
            return Ok(());
        };
        let current = self.vip_status().await;
        if let Some((node_id, score, started_at)) = current {
            if node_id == candidate.id {
                return Ok(());
            }
            let elapsed = Utc::now() - started_at;
            if elapsed
                < chrono::Duration::seconds(config.vip_port.min_switch_interval_seconds as i64)
                || candidate.score - score < config.vip_port.switch_threshold_score_diff
            {
                return Ok(());
            }
        }
        let proxy = parse_share_url(&candidate.raw_config, "vip")?;
        // Validate the candidate in an isolated test-port process before
        // touching the hot VIP port. A malformed or unsupported candidate can
        // therefore never interrupt the currently serving runtime.
        let reservation = allocate_port(config.ports.test.start..=config.ports.test.end).await?;
        let mut preflight = XraySession::start(&config.xray_bin, &proxy, reservation).await?;
        preflight.stop().await;
        // Retain the previous descriptor so a failed switch is recoverable.
        let mut previous = self.vip.lock().await.take();
        if let Some(old) = previous.as_mut() {
            old.session.stop().await;
        }
        match XraySession::start_fixed_with_listen(
            &config.xray_bin,
            &proxy,
            config.vip_port.port,
            "0.0.0.0",
        )
        .await
        {
            Ok(session) => {
                *self.vip.lock().await = Some(VipRuntime {
                    node_id: candidate.id,
                    score: candidate.score,
                    raw_config: candidate.raw_config,
                    started_at: Utc::now(),
                    session,
                });
                Ok(())
            }
            Err(error) => {
                // A restart of the previous target is best effort; do not hide
                // the original switch failure if it too cannot be recovered.
                if let Some(old) = previous {
                    if let Ok(proxy) = parse_share_url(&old.raw_config, "vip-recovery") {
                        if let Ok(session) = XraySession::start_fixed_with_listen(
                            &config.xray_bin,
                            &proxy,
                            config.vip_port.port,
                            "0.0.0.0",
                        )
                        .await
                        {
                            *self.vip.lock().await = Some(VipRuntime {
                                node_id: old.node_id,
                                score: old.score,
                                raw_config: old.raw_config,
                                started_at: old.started_at,
                                session,
                            });
                        }
                    }
                }
                Err(error)
            }
        }
    }
}
