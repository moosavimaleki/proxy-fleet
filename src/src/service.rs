//! Application services: API handlers delegate business operations here so
//! HTTP remains a compatibility/transport layer rather than a second domain
//! implementation.

use std::sync::Arc;

use crate::{config::AppConfig, parser::parse_subscription, selection, storage::Store};

#[derive(Debug, serde::Serialize)]
pub struct ManualImportResult {
    pub accepted: usize,
    pub rejected: usize,
    pub inserted: u64,
    pub errors: Vec<String>,
}

#[derive(Clone)]
pub struct FleetService {
    store: Store,
    config: Arc<AppConfig>,
}

impl FleetService {
    pub fn new(store: Store, config: Arc<AppConfig>) -> Self {
        Self { store, config }
    }

    pub async fn schedule_manual_test(&self, id: &str) -> anyhow::Result<()> {
        self.store.schedule_manual_test(id).await
    }

    pub async fn revive_node(&self, id: &str) -> anyhow::Result<()> {
        self.store.revive_node(id).await
    }

    pub async fn manual_import(&self, configs: &str) -> anyhow::Result<ManualImportResult> {
        let report = parse_subscription(configs, "manual");
        self.store
            .record_invalid_config_rejections("manual", &report.rejected)
            .await?;
        let mut inserted = 0_u64;
        for proxy in &report.accepted {
            inserted += u64::from(self.store.ingest_proxy(proxy, 0).await?);
        }
        Ok(ManualImportResult {
            accepted: report.accepted.len(),
            rejected: report.rejected.len(),
            inserted,
            errors: report.rejected,
        })
    }

    pub async fn best_for_client(
        &self,
        client: &str,
    ) -> anyhow::Result<Option<selection::BestDecision>> {
        selection::best(&self.store, &self.config, client).await
    }

    pub async fn record_client_feedback(
        &self,
        client: &str,
        node_id: &str,
        status: &str,
    ) -> anyhow::Result<()> {
        selection::feedback(&self.store, &self.config, client, node_id, status).await
    }

    pub async fn revive_dormant(&self) -> anyhow::Result<u64> {
        self.store.revive_dormant().await
    }
}
