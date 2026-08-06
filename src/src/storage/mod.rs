use std::{
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::Context;
use chrono::{DateTime, Utc};
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};

use crate::{
    domain::{
        evidence::TestStage,
        failure::FailureClass,
        proxy::{LifecycleState, NodeSummary},
    },
    health::{HealthInput, decide, event_contribution},
    parser::ParsedProxy,
};

#[derive(Clone)]
pub struct Store {
    pool: SqlitePool,
    database_path: PathBuf,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct FleetCounts {
    pub total: i64,
    pub candidate: i64,
    pub testing: i64,
    pub active: i64,
    pub probation: i64,
    pub dormant: i64,
    pub invalid: i64,
    pub retired: i64,
    pub waiting_for_port: i64,
    pub publishable: i64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct NodePage {
    pub total: i64,
    pub page: u64,
    pub page_size: u64,
    pub nodes: Vec<NodeSummary>,
}

#[derive(Debug, Clone)]
pub struct TestEventInput {
    pub proxy_id: String,
    pub run_id: String,
    pub stage: TestStage,
    pub class: FailureClass,
    pub fast_download: bool,
    pub latency_ms: Option<f64>,
    pub download_bps: Option<f64>,
    pub bytes_transferred: Option<i64>,
    pub duration_ms: Option<i64>,
    pub endpoint: Option<String>,
    pub system_pressure: Option<f64>,
    pub incident_id: Option<String>,
    pub detail_json: serde_json::Value,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TestEventApplied {
    pub lifecycle_state: String,
    pub health_score: f64,
    pub publication_lease_until: Option<DateTime<Utc>>,
    pub next_test_at: DateTime<Utc>,
    pub inconclusive: bool,
}

#[derive(Debug, Clone)]
pub struct CandidateForSelection {
    pub id: String,
    pub main_port: Option<i64>,
    pub relay_delay_ms: Option<i64>,
    pub download_kbps: Option<i64>,
    pub health_score: f64,
    pub active_assignments: i64,
    pub recent_global_usage: i64,
    pub recent_client_usage: i64,
    pub client_success_ewma: Option<f64>,
    pub client_fail_streak: i64,
    pub client_rate_limit_streak: i64,
    pub circuit_open: bool,
}

#[derive(Debug, Clone)]
pub struct VipCandidate {
    pub id: String,
    pub raw_config: String,
    pub score: f64,
}

#[derive(Debug, Clone)]
pub struct RuntimeCandidate {
    pub id: String,
    pub raw_config: String,
}

// Health is an estimate of current behaviour, not an ever-growing lifetime
// counter.  These bounds prevent a busy, long-running ACTIVE node from
// becoming mathematically impossible to demote after a real outage.
const MAX_EFFECTIVE_ALPHA: f64 = 24.0;
const MAX_EFFECTIVE_BETA: f64 = 64.0;
const MAX_EVIDENCE_EVENTS_PER_NODE: i64 = 512;

impl Store {
    pub async fn connect(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))?
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .busy_timeout(std::time::Duration::from_secs(10));
        let pool = SqlitePoolOptions::new()
            .max_connections(6)
            .min_connections(1)
            .connect_with(options)
            .await?;
        Ok(Self {
            pool,
            database_path: path.to_owned(),
        })
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub async fn set_service_state(
        &self,
        key: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<()> {
        sqlx::query("INSERT INTO service_state(key, value_json, updated_at) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at")
            .bind(key)
            .bind(value.to_string())
            .bind(Utc::now().to_rfc3339())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn service_state(&self, key: &str) -> anyhow::Result<Option<serde_json::Value>> {
        let value =
            sqlx::query_scalar::<_, String>("SELECT value_json FROM service_state WHERE key = ?")
                .bind(key)
                .fetch_optional(&self.pool)
                .await?;
        Ok(value
            .map(|value| serde_json::from_str(&value).unwrap_or(serde_json::json!({"raw":value}))))
    }

    pub async fn set_scheduler_state(
        &self,
        key: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<()> {
        sqlx::query("INSERT INTO scheduler_state(key, value_json, updated_at) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at")
            .bind(key)
            .bind(value.to_string())
            .bind(Utc::now().to_rfc3339())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn scheduler_state(&self, key: &str) -> anyhow::Result<Option<serde_json::Value>> {
        let value =
            sqlx::query_scalar::<_, String>("SELECT value_json FROM scheduler_state WHERE key = ?")
                .bind(key)
                .fetch_optional(&self.pool)
                .await?;
        Ok(value
            .map(|value| serde_json::from_str(&value).unwrap_or(serde_json::json!({"raw":value}))))
    }

    /// Makes one side-by-side pre-migration snapshot.  It is intentionally
    /// skipped after the Rust schema marker exists, so normal restarts do not
    /// copy a large production database repeatedly.
    pub async fn backup_before_migrate(&self) -> anyhow::Result<Option<PathBuf>> {
        let marker_exists = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'")
            .fetch_one(&self.pool).await? > 0;
        if marker_exists {
            let migrated = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM schema_migrations WHERE version = 'rust-evidence-v1'",
            )
            .fetch_one(&self.pool)
            .await?
                > 0;
            if migrated {
                return Ok(None);
            }
        }
        if !self.database_path.exists() || self.database_path.metadata()?.len() == 0 {
            return Ok(None);
        }
        let parent = self
            .database_path
            .parent()
            .context("database has no parent")?;
        let stamp = Utc::now().format("%Y%m%d-%H%M%S");
        let backup = parent.join(format!(
            "{}.bak-before-rust-{stamp}",
            self.database_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("app.db")
        ));
        tokio::fs::copy(&self.database_path, &backup).await?;
        for suffix in ["-wal", "-shm"] {
            let sidecar = PathBuf::from(format!("{}{}", self.database_path.display(), suffix));
            if sidecar.exists() {
                let backup_sidecar = PathBuf::from(format!("{}{}", backup.display(), suffix));
                tokio::fs::copy(sidecar, backup_sidecar).await?;
            }
        }
        Ok(Some(backup))
    }

    pub async fn migrate(&self) -> anyhow::Result<()> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(&mut *transaction)
            .await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at TEXT NOT NULL)").execute(&mut *transaction).await?;
        // The old Python schema creates these tables. A fresh Rust deployment needs the same
        // compatibility shape before we apply additive columns.
        sqlx::query(COMPAT_SCHEMA)
            .execute(&mut *transaction)
            .await?;
        let rows = sqlx::query("PRAGMA table_info(nodes)")
            .fetch_all(&mut *transaction)
            .await?;
        let existing: std::collections::HashSet<String> = rows
            .into_iter()
            .map(|row| row.get::<String, _>("name"))
            .collect();
        for (name, definition) in NODE_ADDITIONS {
            if !existing.contains(*name) {
                let sql = format!("ALTER TABLE nodes ADD COLUMN {name} {definition}");
                sqlx::query(&sql)
                    .execute(&mut *transaction)
                    .await
                    .with_context(|| format!("adding nodes.{name}"))?;
            }
        }
        sqlx::query(EVENT_SCHEMA).execute(&mut *transaction).await?;
        let event_columns = sqlx::query("PRAGMA table_info(proxy_test_events)")
            .fetch_all(&mut *transaction)
            .await?;
        let event_columns: std::collections::HashSet<String> = event_columns
            .into_iter()
            .map(|row| row.get("name"))
            .collect();
        if !event_columns.contains("half_life_seconds") {
            sqlx::query("ALTER TABLE proxy_test_events ADD COLUMN half_life_seconds REAL NOT NULL DEFAULT 0")
                .execute(&mut *transaction)
                .await?;
        }
        sqlx::query(UPSTREAM_SCHEMA)
            .execute(&mut *transaction)
            .await?;
        sqlx::query(SCHEDULER_SCHEMA)
            .execute(&mut *transaction)
            .await?;
        for index in INDEXES {
            sqlx::query(index).execute(&mut *transaction).await?;
        }

        sqlx::query(
            "UPDATE nodes SET lifecycle_state = CASE status WHEN 'DEAD' THEN 'DORMANT' WHEN 'REMOVED' THEN 'RETIRED' ELSE status END WHERE lifecycle_state IS NULL OR lifecycle_state = ''"
        ).execute(&mut *transaction).await?;
        sqlx::query("UPDATE nodes SET status = lifecycle_state WHERE lifecycle_state IN ('DORMANT', 'RETIRED', 'INVALID') AND status IN ('DEAD', 'REMOVED')").execute(&mut *transaction).await?;
        sqlx::query(
            "UPDATE nodes SET health_alpha = CASE lifecycle_state WHEN 'ACTIVE' THEN 8.0 WHEN 'PROBATION' THEN 3.0 ELSE 1.0 END WHERE health_alpha IS NULL OR health_alpha <= 0"
        ).execute(&mut *transaction).await?;
        sqlx::query(
            "UPDATE nodes SET health_beta = CASE lifecycle_state WHEN 'ACTIVE' THEN 1.0 WHEN 'PROBATION' THEN 2.0 ELSE 1.0 END WHERE health_beta IS NULL OR health_beta <= 0"
        ).execute(&mut *transaction).await?;
        sqlx::query("UPDATE nodes SET health_score = health_alpha / (health_alpha + health_beta) WHERE health_score IS NULL OR health_score < 0 OR health_score > 1").execute(&mut *transaction).await?;
        sqlx::query("UPDATE nodes SET structurally_valid = CASE WHEN lifecycle_state = 'INVALID' THEN 0 ELSE 1 END WHERE structurally_valid IS NULL").execute(&mut *transaction).await?;
        let migration_now = Utc::now();
        sqlx::query("UPDATE nodes SET last_real_download_at = last_download_test_at WHERE last_real_download_at IS NULL AND last_download_test_at IS NOT NULL")
            .execute(&mut *transaction)
            .await?;
        sqlx::query("UPDATE nodes SET publication_lease_until = ? WHERE lifecycle_state = 'ACTIVE' AND publication_lease_until IS NULL AND last_download_test_at >= ?")
            .bind((migration_now + chrono::Duration::hours(6)).to_rfc3339()).bind((migration_now - chrono::Duration::hours(24)).to_rfc3339()).execute(&mut *transaction).await?;
        sqlx::query("INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES ('rust-evidence-v1', ?)")
            .bind(Utc::now().to_rfc3339()).execute(&mut *transaction).await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn counts(&self) -> anyhow::Result<FleetCounts> {
        let rows = sqlx::query(
            "SELECT lifecycle_state, COUNT(*) AS count FROM nodes GROUP BY lifecycle_state",
        )
        .fetch_all(&self.pool)
        .await?;
        let mut counts = std::collections::HashMap::<String, i64>::new();
        let mut total = 0;
        for row in rows {
            let value = row.get::<i64, _>("count");
            total += value;
            counts.insert(row.get("lifecycle_state"), value);
        }
        let publishable = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM nodes WHERE structurally_valid = 1 AND publication_lease_until > ? AND lifecycle_state NOT IN ('INVALID', 'RETIRED')")
            .bind(Utc::now().to_rfc3339()).fetch_one(&self.pool).await?;
        Ok(FleetCounts {
            total,
            candidate: take(&counts, "CANDIDATE"),
            testing: take(&counts, "TESTING"),
            active: take(&counts, "ACTIVE"),
            probation: take(&counts, "PROBATION"),
            dormant: take(&counts, "DORMANT"),
            invalid: take(&counts, "INVALID"),
            retired: take(&counts, "RETIRED"),
            waiting_for_port: take(&counts, "WAITING_FOR_PORT"),
            publishable,
        })
    }

    pub async fn list_nodes(
        &self,
        page: u64,
        page_size: u64,
        status: Option<&str>,
        country: Option<&str>,
        search: Option<&str>,
    ) -> anyhow::Result<NodePage> {
        let page = page.max(1);
        let page_size = page_size.clamp(1, 200);
        let mut clauses = Vec::new();
        let mut values = Vec::new();
        if let Some(status) = status.filter(|v| !v.is_empty()) {
            clauses.push("lifecycle_state = ?");
            values.push(status.to_owned());
        }
        if let Some(country) = country.filter(|v| !v.is_empty()) {
            clauses.push("exit_country = ?");
            values.push(country.to_owned());
        }
        if let Some(search) = search.filter(|v| !v.is_empty()) {
            clauses.push("(raw_config LIKE ? OR config_hash LIKE ? OR exit_country LIKE ?)");
            let like = format!("%{search}%");
            values.extend([like.clone(), like.clone(), like]);
        }
        let where_sql = if clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", clauses.join(" AND "))
        };
        let count_sql = format!("SELECT COUNT(*) FROM nodes{where_sql}");
        let mut count_query = sqlx::query_scalar::<_, i64>(&count_sql);
        for value in &values {
            count_query = count_query.bind(value);
        }
        let total = count_query.fetch_one(&self.pool).await?;
        let list_sql = format!(
            "SELECT id, config_hash, raw_config, source_subs, lifecycle_state, status, main_port, relay_delay_ms, download_kbps, exit_country, health_success_ewma, health_alpha, health_beta, health_score, next_test_at, publication_lease_until, publication_lease_kind, last_failure_class, last_seen_generation, upstream_missing_generations, created_at, last_test_at FROM nodes{where_sql} ORDER BY CASE lifecycle_state WHEN 'ACTIVE' THEN 0 WHEN 'PROBATION' THEN 1 WHEN 'CANDIDATE' THEN 2 ELSE 3 END, health_score DESC, last_test_at DESC LIMIT ? OFFSET ?"
        );
        let mut query = sqlx::query(&list_sql);
        for value in &values {
            query = query.bind(value);
        }
        let rows = query
            .bind(page_size as i64)
            .bind(((page - 1) * page_size) as i64)
            .fetch_all(&self.pool)
            .await?;
        Ok(NodePage {
            total,
            page,
            page_size,
            nodes: rows.into_iter().map(row_to_summary).collect(),
        })
    }

    /// Dashboard filters must remain bounded even with a large fleet.  This
    /// query is intentionally distinct from the paged node query and returns
    /// only a small list of display values.
    pub async fn list_exit_countries(&self) -> anyhow::Result<Vec<String>> {
        let rows = sqlx::query_scalar::<_, String>(
            "SELECT DISTINCT exit_country FROM nodes WHERE exit_country <> '' ORDER BY exit_country ASC",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn sqlite_size_bytes(&self) -> anyhow::Result<i64> {
        let pages = sqlx::query_scalar::<_, i64>("PRAGMA page_count")
            .fetch_one(&self.pool)
            .await?;
        let page_size = sqlx::query_scalar::<_, i64>("PRAGMA page_size")
            .fetch_one(&self.pool)
            .await?;
        Ok(pages * page_size)
    }

    /// Bounded operational snapshot for the diagnostics API.  This avoids
    /// making the dashboard infer scheduler pressure from an unbounded node
    /// listing.
    pub async fn scheduler_snapshot(&self) -> anyhow::Result<serde_json::Value> {
        let rows = sqlx::query(
            "SELECT lifecycle_state, COUNT(*) AS count, SUM(CASE WHEN next_test_at IS NULL OR next_test_at <= ? THEN 1 ELSE 0 END) AS overdue FROM nodes GROUP BY lifecycle_state ORDER BY lifecycle_state",
        )
        .bind(Utc::now().to_rfc3339())
        .fetch_all(&self.pool)
        .await?;
        let queues: Vec<_> = rows
            .into_iter()
            .map(|row| {
                serde_json::json!({
                    "state": row.get::<Option<String>, _>("lifecycle_state").unwrap_or_else(|| "CANDIDATE".to_owned()),
                    "depth": row.get::<i64, _>("count"),
                    "overdue": row.get::<Option<i64>, _>("overdue").unwrap_or_default(),
                })
            })
            .collect();
        Ok(serde_json::json!({"queues":queues}))
    }

    /// Source-level health and current reconciliation state for diagnostics.
    pub async fn upstream_snapshot(&self) -> anyhow::Result<serde_json::Value> {
        let latest = sqlx::query(
            "SELECT generation, status, finished_at, parsed_count, successful_source_count, source_count FROM upstream_refresh_runs ORDER BY generation DESC LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await?;
        let sources = sqlx::query(
            "SELECT name, url, enabled, last_success_at, failure_streak, etag, last_modified FROM upstream_sources ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await?;
        let missing = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM nodes WHERE upstream_missing_generations > 0 AND source_subs NOT LIKE '%manual%'",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(serde_json::json!({
            "latest": latest.map(|row| serde_json::json!({
                "generation":row.get::<i64,_>("generation"),
                "status":row.get::<String,_>("status"),
                "finished_at":row.get::<Option<String>,_>("finished_at"),
                "parsed_count":row.get::<i64,_>("parsed_count"),
                "successful_sources":row.get::<i64,_>("successful_source_count"),
                "source_count":row.get::<i64,_>("source_count"),
            })),
            "sources": sources.into_iter().map(|row| serde_json::json!({
                "name":row.get::<String,_>("name"),
                "url":row.get::<String,_>("url"),
                "enabled":row.get::<i64,_>("enabled") != 0,
                "last_success_at":row.get::<Option<String>,_>("last_success_at"),
                "failure_streak":row.get::<i64,_>("failure_streak"),
                "etag":row.get::<Option<String>,_>("etag"),
                "last_modified":row.get::<Option<String>,_>("last_modified"),
            })).collect::<Vec<_>>(),
            "nodes_with_missing_generations":missing,
        }))
    }

    pub async fn apply_test_event(
        &self,
        event: TestEventInput,
        active_relay_interval: std::time::Duration,
        active_download_interval: std::time::Duration,
    ) -> anyhow::Result<TestEventApplied> {
        let now = Utc::now();
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query("SELECT lifecycle_state, testing_from_state, status, publication_lease_until, activated_at, failure_streak, independent_failure_count, last_success_at, last_real_download_at, last_failure_run_id, health_score, next_test_at FROM nodes WHERE id = ?")
            .bind(&event.proxy_id)
            .fetch_optional(&mut *transaction)
            .await?
            .context("proxy not found")?;
        let lifecycle: LifecycleState = row
            .get::<Option<String>, _>("lifecycle_state")
            .filter(|state| state != "TESTING")
            .or_else(|| row.get::<Option<String>, _>("testing_from_state"))
            .or_else(|| row.get::<Option<String>, _>("status"))
            .unwrap_or_else(|| "CANDIDATE".to_owned())
            .parse()
            .unwrap_or(LifecycleState::Candidate);
        // A worker may be retried after it has persisted an event but before
        // acknowledging completion.  The event stream must remain append-only
        // *and* idempotent: the same run/stage is one observation, never two.
        let already_applied = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(SELECT 1 FROM proxy_test_events WHERE proxy_id = ? AND run_id = ? AND stage = ?)",
        )
        .bind(&event.proxy_id)
        .bind(&event.run_id)
        .bind(event.stage.as_str())
        .fetch_one(&mut *transaction)
        .await?
            != 0;
        if already_applied {
            transaction.commit().await?;
            return Ok(TestEventApplied {
                lifecycle_state: lifecycle.as_str().to_owned(),
                health_score: row.get::<Option<f64>, _>("health_score").unwrap_or(0.5),
                publication_lease_until: parse_time(row.get("publication_lease_until")),
                next_test_at: parse_time(row.get("next_test_at")).unwrap_or(now),
                inconclusive: event.class.inconclusive(),
            });
        }
        let contribution = event_contribution(event.stage, event.class, event.fast_download);
        let mut detail = event.detail_json;
        if let serde_json::Value::Object(ref mut object) = detail {
            object.insert(
                "half_life_seconds".to_owned(),
                serde_json::json!(contribution.half_life.num_seconds()),
            );
            object.insert(
                "fast_download".to_owned(),
                serde_json::json!(event.fast_download),
            );
        }
        let result = if event.class.inconclusive() {
            "INCONCLUSIVE"
        } else if event.class == FailureClass::Success {
            "SUCCESS"
        } else {
            "FAILURE"
        };
        sqlx::query("INSERT INTO proxy_test_events(proxy_id, run_id, occurred_at, stage, result, failure_class, evidence_alpha, evidence_beta, half_life_seconds, latency_ms, download_bps, bytes_transferred, duration_ms, endpoint, system_pressure, incident_id, detail_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(&event.proxy_id).bind(&event.run_id).bind(now.to_rfc3339()).bind(event.stage.as_str()).bind(result).bind(event.class.as_str())
            .bind(contribution.alpha).bind(contribution.beta).bind(contribution.half_life.num_seconds() as f64).bind(event.latency_ms).bind(event.download_bps).bind(event.bytes_transferred).bind(event.duration_ms).bind(&event.endpoint).bind(event.system_pressure).bind(&event.incident_id).bind(detail.to_string())
            .execute(&mut *transaction).await?;
        let evidence = sqlx::query("SELECT occurred_at, evidence_alpha, evidence_beta, half_life_seconds FROM proxy_test_events WHERE proxy_id = ? AND occurred_at >= ? ORDER BY occurred_at DESC, id DESC LIMIT ?")
            .bind(&event.proxy_id).bind((now - chrono::Duration::days(30)).to_rfc3339()).bind(MAX_EVIDENCE_EVENTS_PER_NODE).fetch_all(&mut *transaction).await?;
        let (alpha, beta) = evidence.iter().fold((1.0, 1.0), |(alpha, beta), row| {
            let occurred = parse_time(row.get("occurred_at")).unwrap_or(now);
            let half_life =
                chrono::Duration::seconds(row.get::<f64, _>("half_life_seconds").max(1.0) as i64);
            let elapsed = now - occurred;
            (
                alpha
                    + crate::domain::evidence::decay(
                        row.get::<f64, _>("evidence_alpha"),
                        elapsed,
                        half_life,
                    ),
                beta + crate::domain::evidence::decay(
                    row.get::<f64, _>("evidence_beta"),
                    elapsed,
                    half_life,
                ),
            )
        });
        let alpha = alpha.min(MAX_EFFECTIVE_ALPHA);
        let beta = beta.min(MAX_EFFECTIVE_BETA);
        let prior_lease = parse_time(row.get("publication_lease_until"));
        let activated_at = parse_time(row.get("activated_at"));
        let input = HealthInput {
            prior_lifecycle: lifecycle,
            prior_lease_until: prior_lease,
            activated_at,
            alpha,
            beta,
            failure_streak: row
                .get::<Option<i64>, _>("failure_streak")
                .unwrap_or_default()
                .max(0) as u32,
            independent_failures: row
                .get::<Option<i64>, _>("independent_failure_count")
                .unwrap_or_default()
                .max(0) as u32,
            new_independent_failure: !event.class.inconclusive()
                && event.class != FailureClass::Success
                && row
                    .get::<Option<String>, _>("last_failure_run_id")
                    .as_deref()
                    != Some(event.run_id.as_str()),
            had_real_download: row
                .get::<Option<String>, _>("last_real_download_at")
                .is_some(),
            stage: event.stage,
            class: event.class,
            fast_download: event.fast_download,
            now,
            active_relay_interval: chrono::Duration::from_std(active_relay_interval)
                .unwrap_or_else(|_| chrono::Duration::seconds(10)),
            active_download_interval: chrono::Duration::from_std(active_download_interval)
                .unwrap_or_else(|_| chrono::Duration::minutes(5)),
        };
        let decision = decide(input);
        let activated_at = if decision.lifecycle == LifecycleState::Active
            && lifecycle != LifecycleState::Active
        {
            Some(now)
        } else {
            activated_at
        };
        let state_entered_at = if decision.lifecycle != lifecycle {
            Some(now)
        } else {
            None
        };
        let last_success_at = if event.class == FailureClass::Success {
            Some(now)
        } else {
            parse_time(row.get("last_success_at"))
        };
        let last_real_download_at =
            if event.class == FailureClass::Success && event.stage == TestStage::Download {
                Some(now)
            } else {
                parse_time(row.get("last_real_download_at"))
            };
        let last_failure_at = if !event.class.inconclusive() && event.class != FailureClass::Success
        {
            Some(now)
        } else {
            None
        };
        sqlx::query("UPDATE nodes SET status = ?, lifecycle_state = ?, testing_from_state = NULL, test_lease_until = NULL, state_entered_at = COALESCE(?, state_entered_at), structurally_valid = CASE WHEN ? = 'INVALID_CONFIG' THEN 0 ELSE COALESCE(structurally_valid, 1) END, health_alpha = ?, health_beta = ?, health_score = ?, evidence_updated_at = ?, publication_lease_until = ?, publication_lease_kind = CASE WHEN ? IS NOT NULL THEN ? ELSE publication_lease_kind END, activated_at = ?, last_success_at = ?, last_real_download_at = ?, last_failure_at = COALESCE(?, last_failure_at), last_failure_class = CASE WHEN ? IN ('SUCCESS', 'LOCAL_OVERLOAD', 'ENDPOINT_FAILURE') THEN last_failure_class ELSE ? END, last_failure_run_id = CASE WHEN ? IN ('SUCCESS', 'LOCAL_OVERLOAD', 'ENDPOINT_FAILURE') THEN last_failure_run_id ELSE ? END, failure_streak = ?, independent_failure_count = ?, next_test_at = ?, last_test_at = ?, last_test_endpoint = COALESCE(?, last_test_endpoint), relay_delay_ms = COALESCE(?, relay_delay_ms), download_kbps = COALESCE(?, download_kbps), updated_at = ? WHERE id = ?")
            .bind(decision.lifecycle.as_str()).bind(decision.lifecycle.as_str()).bind(state_entered_at.map(|time| time.to_rfc3339())).bind(event.class.as_str())
            .bind(alpha).bind(beta).bind(decision.score).bind(now.to_rfc3339()).bind(decision.lease_until.map(|time| time.to_rfc3339())).bind(decision.lease_until.map(|_| "lease".to_owned())).bind(event.stage.as_str())
            .bind(activated_at.map(|time| time.to_rfc3339())).bind(last_success_at.map(|time| time.to_rfc3339())).bind(last_real_download_at.map(|time| time.to_rfc3339())).bind(last_failure_at.map(|time| time.to_rfc3339())).bind(event.class.as_str()).bind(event.class.as_str()).bind(event.class.as_str()).bind(&event.run_id)
            .bind(decision.failure_streak as i64).bind(decision.independent_failures as i64).bind(decision.next_test_at.to_rfc3339()).bind(now.to_rfc3339()).bind(event.endpoint).bind(event.latency_ms.map(|value| value.round() as i64)).bind(event.download_bps.map(|value| (value / 1024.0).round() as i64)).bind(now.to_rfc3339()).bind(&event.proxy_id)
            .execute(&mut *transaction).await?;
        transaction.commit().await?;
        Ok(TestEventApplied {
            lifecycle_state: decision.lifecycle.as_str().to_owned(),
            health_score: decision.score,
            publication_lease_until: decision.lease_until,
            next_test_at: decision.next_test_at,
            inconclusive: event.class.inconclusive(),
        })
    }

    pub async fn list_publishable_raw_configs(&self) -> anyhow::Result<Vec<String>> {
        Ok(sqlx::query_scalar("SELECT raw_config FROM nodes WHERE structurally_valid = 1 AND publication_lease_until > ? AND lifecycle_state NOT IN ('INVALID', 'RETIRED') ORDER BY health_score DESC, config_hash ASC")
            .bind(Utc::now().to_rfc3339()).fetch_all(&self.pool).await?)
    }

    pub async fn ingest_proxy(&self, proxy: &ParsedProxy, generation: i64) -> anyhow::Result<bool> {
        Ok(self
            .ingest_many(std::slice::from_ref(proxy), generation)
            .await?
            > 0)
    }

    /// Upstream refreshes commonly carry tens of thousands of entries.  One
    /// transaction per entry turns that into WAL churn; keep a whole parsed
    /// source generation atomic instead.
    pub async fn ingest_many(
        &self,
        proxies: &[ParsedProxy],
        generation: i64,
    ) -> anyhow::Result<u64> {
        let now = Utc::now().to_rfc3339();
        let mut transaction = self.pool.begin().await?;
        let mut inserted = 0_u64;
        for proxy in proxies {
            let existing = sqlx::query("SELECT id, source_subs FROM nodes WHERE config_hash = ?")
                .bind(&proxy.config_hash)
                .fetch_optional(&mut *transaction)
                .await?;
            if let Some(row) = existing {
                let mut sources: std::collections::BTreeSet<String> =
                    serde_json::from_str(&row.get::<String, _>("source_subs")).unwrap_or_default();
                sources.insert(proxy.source.clone());
                sqlx::query("UPDATE nodes SET raw_config = ?, normalized_config = ?, source_subs = ?, last_seen_generation = ?, upstream_missing_generations = 0, updated_at = ? WHERE id = ?")
                    .bind(&proxy.raw_config).bind(proxy.normalized_config.to_string()).bind(serde_json::to_string(&sources)?).bind(generation).bind(&now).bind(row.get::<String, _>("id")).execute(&mut *transaction).await?;
            } else {
                let id = uuid::Uuid::new_v4().simple().to_string();
                sqlx::query("INSERT INTO nodes(id, config_hash, raw_config, normalized_config, source_subs, status, lifecycle_state, structurally_valid, health_alpha, health_beta, health_score, created_at, updated_at, next_test_at, last_seen_generation, upstream_missing_generations) VALUES (?, ?, ?, ?, ?, 'CANDIDATE', 'CANDIDATE', 1, 1.0, 1.0, 0.5, ?, ?, ?, ?, 0)")
                    .bind(id).bind(&proxy.config_hash).bind(&proxy.raw_config).bind(proxy.normalized_config.to_string()).bind(serde_json::to_string(&vec![proxy.source.clone()])?).bind(&now).bind(&now).bind(&now).bind(generation).execute(&mut *transaction).await?;
                inserted += 1;
            }
            sqlx::query("INSERT OR IGNORE INTO upstream_generation_members(generation, source_name, config_hash, seen_at) VALUES (?, ?, ?, ?)")
                .bind(generation).bind(&proxy.source).bind(&proxy.config_hash).bind(&now).execute(&mut *transaction).await?;
        }
        transaction.commit().await?;
        Ok(inserted)
    }

    pub async fn begin_refresh(&self, source_count: usize) -> anyhow::Result<(String, i64)> {
        let generation = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT MAX(generation) FROM upstream_refresh_runs",
        )
        .fetch_one(&self.pool)
        .await?
        .unwrap_or(0)
            + 1;
        let id = uuid::Uuid::new_v4().simple().to_string();
        sqlx::query("INSERT INTO upstream_refresh_runs(id, generation, started_at, status, source_count) VALUES (?, ?, ?, 'RUNNING', ?)")
            .bind(&id).bind(generation).bind(Utc::now().to_rfc3339()).bind(source_count as i64).execute(&self.pool).await?;
        Ok((id, generation))
    }

    pub async fn finish_refresh(
        &self,
        id: &str,
        generation: i64,
        source_count: usize,
        successful_sources: usize,
        parsed_count: usize,
        minimum_missing_generations: u32,
    ) -> anyhow::Result<bool> {
        let complete = source_count > 0 && successful_sources == source_count;
        let status = if complete { "COMPLETE" } else { "PARTIAL" };
        let now = Utc::now();
        sqlx::query("UPDATE upstream_refresh_runs SET finished_at = ?, status = ?, successful_source_count = ?, parsed_count = ? WHERE id = ?")
            .bind(now.to_rfc3339()).bind(status).bind(successful_sources as i64).bind(parsed_count as i64).bind(id).execute(&self.pool).await?;
        if !complete {
            return Ok(false);
        }
        let mut transaction = self.pool.begin().await?;
        sqlx::query("UPDATE nodes SET upstream_missing_generations = upstream_missing_generations + 1 WHERE COALESCE(last_seen_generation, -1) <> ? AND source_subs NOT LIKE '%manual%'")
            .bind(generation).execute(&mut *transaction).await?;
        sqlx::query("UPDATE nodes SET lifecycle_state = 'RETIRED', status = 'RETIRED', retired_at = ?, tombstone_until = ? WHERE lifecycle_state NOT IN ('INVALID', 'RETIRED') AND upstream_missing_generations >= ? AND created_at <= ? AND (publication_lease_until IS NULL OR publication_lease_until <= ?)")
            .bind(now.to_rfc3339()).bind((now + chrono::Duration::days(30)).to_rfc3339()).bind(minimum_missing_generations as i64).bind((now - chrono::Duration::hours(12)).to_rfc3339()).bind(now.to_rfc3339()).execute(&mut *transaction).await?;
        transaction.commit().await?;
        Ok(true)
    }

    pub async fn source_http_cache(
        &self,
        name: &str,
    ) -> anyhow::Result<(Option<String>, Option<String>)> {
        Ok(sqlx::query_as::<_, (Option<String>, Option<String>)>(
            "SELECT etag, last_modified FROM upstream_sources WHERE name = ?",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await?
        .unwrap_or((None, None)))
    }

    pub async fn record_source_success(
        &self,
        name: &str,
        url: &str,
        etag: Option<&str>,
        last_modified: Option<&str>,
    ) -> anyhow::Result<()> {
        sqlx::query("INSERT INTO upstream_sources(name, url, enabled, last_success_at, etag, last_modified, failure_streak) VALUES (?, ?, 1, ?, ?, ?, 0) ON CONFLICT(name) DO UPDATE SET url = excluded.url, enabled = 1, last_success_at = excluded.last_success_at, etag = COALESCE(excluded.etag, upstream_sources.etag), last_modified = COALESCE(excluded.last_modified, upstream_sources.last_modified), failure_streak = 0")
            .bind(name).bind(url).bind(Utc::now().to_rfc3339()).bind(etag).bind(last_modified).execute(&self.pool).await?;
        Ok(())
    }

    /// A 304 response is a successful observation of a source. Recreate its
    /// membership for the current generation so reconciliation never treats a
    /// cached source as if all of its proxies disappeared.
    pub async fn copy_cached_source_generation(
        &self,
        source_name: &str,
        generation: i64,
    ) -> anyhow::Result<u64> {
        let now = Utc::now().to_rfc3339();
        let mut transaction = self.pool.begin().await?;
        let previous = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT MAX(generation) FROM upstream_generation_members WHERE source_name = ? AND generation < ?",
        )
        .bind(source_name)
        .bind(generation)
        .fetch_one(&mut *transaction)
        .await?;
        let Some(previous) = previous else {
            transaction.commit().await?;
            return Ok(0);
        };
        let copied = sqlx::query("INSERT OR IGNORE INTO upstream_generation_members(generation, source_name, config_hash, seen_at) SELECT ?, source_name, config_hash, ? FROM upstream_generation_members WHERE generation = ? AND source_name = ?")
            .bind(generation).bind(&now).bind(previous).bind(source_name).execute(&mut *transaction).await?.rows_affected();
        sqlx::query("UPDATE nodes SET last_seen_generation = ?, upstream_missing_generations = 0, updated_at = ? WHERE config_hash IN (SELECT config_hash FROM upstream_generation_members WHERE generation = ? AND source_name = ?)")
            .bind(generation).bind(&now).bind(generation).bind(source_name).execute(&mut *transaction).await?;
        transaction.commit().await?;
        Ok(copied)
    }

    pub async fn record_source_failure(&self, name: &str, url: &str) -> anyhow::Result<()> {
        sqlx::query("INSERT INTO upstream_sources(name, url, enabled, failure_streak) VALUES (?, ?, 1, 1) ON CONFLICT(name) DO UPDATE SET url = excluded.url, failure_streak = upstream_sources.failure_streak + 1")
            .bind(name).bind(url).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn release_test_lease(&self, proxy_id: &str) -> anyhow::Result<()> {
        sqlx::query("UPDATE nodes SET lifecycle_state = COALESCE(testing_from_state, lifecycle_state), status = COALESCE(testing_from_state, status), testing_from_state = NULL, test_lease_until = NULL, updated_at = ? WHERE id = ? AND lifecycle_state = 'TESTING'")
            .bind(Utc::now().to_rfc3339()).bind(proxy_id).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn set_main_port(&self, proxy_id: &str, port: u16) -> anyhow::Result<()> {
        sqlx::query("UPDATE nodes SET main_port = ?, updated_at = ? WHERE id = ?")
            .bind(port as i64)
            .bind(Utc::now().to_rfc3339())
            .bind(proxy_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn clear_main_port(&self, proxy_id: &str) -> anyhow::Result<()> {
        sqlx::query("UPDATE nodes SET main_port = NULL, updated_at = ? WHERE id = ?")
            .bind(Utc::now().to_rfc3339())
            .bind(proxy_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Ports are process-local leases. After a restart no persisted port is
    /// considered live until this binary has started and verified its Xray
    /// child again; otherwise `/best` could hand out an orphaned port.
    pub async fn clear_stale_runtime_ports(&self) -> anyhow::Result<u64> {
        Ok(sqlx::query(
            "UPDATE nodes SET main_port = NULL, updated_at = ? WHERE main_port IS NOT NULL",
        )
        .bind(Utc::now().to_rfc3339())
        .execute(&self.pool)
        .await?
        .rows_affected())
    }

    pub async fn active_runtime_candidates(&self) -> anyhow::Result<Vec<RuntimeCandidate>> {
        let rows = sqlx::query(
            "SELECT id, raw_config FROM nodes WHERE lifecycle_state = 'ACTIVE' AND structurally_valid = 1 AND publication_lease_until > ? ORDER BY health_score DESC, config_hash ASC",
        )
        .bind(Utc::now().to_rfc3339())
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| RuntimeCandidate {
                id: row.get("id"),
                raw_config: row.get("raw_config"),
            })
            .collect())
    }

    pub async fn raw_config(&self, proxy_id: &str) -> anyhow::Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT raw_config FROM nodes WHERE id = ?")
                .bind(proxy_id)
                .fetch_optional(&self.pool)
                .await?,
        )
    }

    pub async fn history(
        &self,
        proxy_id: &str,
        limit: u64,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        let exists = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM nodes WHERE id = ?")
            .bind(proxy_id)
            .fetch_one(&self.pool)
            .await?;
        anyhow::ensure!(exists > 0, "node not found");
        let event_rows = sqlx::query("SELECT occurred_at, stage, result, failure_class, latency_ms, download_bps, bytes_transferred, duration_ms, endpoint, system_pressure, detail_json FROM proxy_test_events WHERE proxy_id = ? ORDER BY occurred_at DESC LIMIT ?")
            .bind(proxy_id).bind(limit.min(200) as i64).fetch_all(&self.pool).await?;
        let mut values: Vec<_> = event_rows.into_iter().map(|row| serde_json::json!({"kind":"event","finished_at":row.get::<String,_>("occurred_at"),"test_kind":row.get::<String,_>("stage"),"result":row.get::<String,_>("result"),"failure_class":row.get::<String,_>("failure_class"),"latency_ms":row.get::<Option<f64>,_>("latency_ms"),"download_bps":row.get::<Option<f64>,_>("download_bps"),"bytes_transferred":row.get::<Option<i64>,_>("bytes_transferred"),"duration_ms":row.get::<Option<i64>,_>("duration_ms"),"endpoint":row.get::<Option<String>,_>("endpoint"),"system_pressure":row.get::<Option<f64>,_>("system_pressure"),"details":serde_json::from_str::<serde_json::Value>(&row.get::<String,_>("detail_json")).unwrap_or(serde_json::json!({}))})).collect();
        if values.len() < limit as usize {
            let remaining = limit.min(200) as usize - values.len();
            let old_rows = sqlx::query("SELECT finished_at, test_kind, trigger, ok, latency_ms, download_kbps, error, status_before, status_after, details_json FROM test_history WHERE node_id = ? ORDER BY finished_at DESC LIMIT ?")
                .bind(proxy_id).bind(remaining as i64).fetch_all(&self.pool).await?;
            values.extend(old_rows.into_iter().map(|row| serde_json::json!({"kind":"legacy","finished_at":row.get::<String,_>("finished_at"),"test_kind":row.get::<String,_>("test_kind"),"trigger":row.get::<String,_>("trigger"),"ok":row.get::<i64,_>("ok") != 0,"latency_ms":row.get::<Option<i64>,_>("latency_ms"),"download_kbps":row.get::<Option<i64>,_>("download_kbps"),"error":row.get::<String,_>("error"),"status_before":row.get::<String,_>("status_before"),"status_after":row.get::<String,_>("status_after"),"details":serde_json::from_str::<serde_json::Value>(&row.get::<String,_>("details_json")).unwrap_or(serde_json::json!({}))})));
            values.sort_by(|left, right| {
                right["finished_at"]
                    .as_str()
                    .cmp(&left["finished_at"].as_str())
            });
        }
        values.truncate(limit.min(200) as usize);
        Ok(values)
    }

    pub async fn list_clients(&self) -> anyhow::Result<Vec<String>> {
        Ok(sqlx::query_scalar(
            "SELECT DISTINCT client_id FROM client_node_state ORDER BY client_id",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn client_status(
        &self,
        client: &str,
        page: u64,
        page_size: u64,
    ) -> anyhow::Result<serde_json::Value> {
        let page = page.max(1);
        let page_size = page_size.clamp(1, 200);
        let total = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM client_node_state WHERE client_id = ?",
        )
        .bind(client)
        .fetch_one(&self.pool)
        .await?;
        let rows = sqlx::query("SELECT c.client_id, c.node_id, c.state, c.fail_streak, c.rate_limit_streak, c.cooldown_until, c.usage_count, c.success_count, c.broken_count, c.rate_limited_count, c.success_rate_ewma, c.last_assigned_at, c.last_feedback_at, n.lifecycle_state, n.main_port, n.relay_delay_ms, n.download_kbps, n.health_score FROM client_node_state c LEFT JOIN nodes n ON n.id = c.node_id WHERE c.client_id = ? ORDER BY c.last_feedback_at DESC LIMIT ? OFFSET ?")
            .bind(client).bind(page_size as i64).bind(((page - 1) * page_size) as i64).fetch_all(&self.pool).await?;
        let nodes: Vec<_> = rows.into_iter().map(|row| serde_json::json!({"client":row.get::<String,_>("client_id"),"node_id":row.get::<String,_>("node_id"),"state":row.get::<String,_>("state"),"fail_streak":row.get::<Option<i64>,_>("fail_streak").unwrap_or_default(),"rate_limit_streak":row.get::<Option<i64>,_>("rate_limit_streak").unwrap_or_default(),"cooldown_until":row.get::<Option<String>,_>("cooldown_until"),"usage_count":row.get::<Option<i64>,_>("usage_count").unwrap_or_default(),"success_count":row.get::<Option<i64>,_>("success_count").unwrap_or_default(),"broken_count":row.get::<Option<i64>,_>("broken_count").unwrap_or_default(),"rate_limited_count":row.get::<Option<i64>,_>("rate_limited_count").unwrap_or_default(),"success_rate_ewma":row.get::<Option<f64>,_>("success_rate_ewma").unwrap_or(0.5),"last_assigned_at":row.get::<Option<String>,_>("last_assigned_at"),"last_feedback_at":row.get::<Option<String>,_>("last_feedback_at"),"lifecycle_state":row.get::<Option<String>,_>("lifecycle_state"),"main_port":row.get::<Option<i64>,_>("main_port"),"relay_delay_ms":row.get::<Option<i64>,_>("relay_delay_ms"),"download_kbps":row.get::<Option<i64>,_>("download_kbps"),"health_score":row.get::<Option<f64>,_>("health_score")})).collect();
        Ok(
            serde_json::json!({"client":client,"pagination":{"page":page,"page_size":page_size,"total":total},"nodes":nodes}),
        )
    }

    pub async fn logs(
        &self,
        limit: u64,
        component: Option<&str>,
        level: Option<&str>,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        let mut clauses = Vec::new();
        let mut values = Vec::new();
        if let Some(component) = component.filter(|value| !value.is_empty()) {
            clauses.push("component = ?");
            values.push(component.to_owned());
        }
        if let Some(level) = level.filter(|value| !value.is_empty()) {
            clauses.push("level = ?");
            values.push(level.to_owned());
        }
        let where_sql = if clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", clauses.join(" AND "))
        };
        let sql = format!(
            "SELECT created_at, level, component, event, message, details_json FROM system_events{where_sql} ORDER BY created_at DESC LIMIT ?"
        );
        let mut query = sqlx::query(&sql);
        for value in &values {
            query = query.bind(value);
        }
        let rows = query
            .bind(limit.clamp(1, 1000) as i64)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(|row| serde_json::json!({"created_at":row.get::<String,_>("created_at"),"level":row.get::<String,_>("level"),"component":row.get::<String,_>("component"),"event":row.get::<String,_>("event"),"message":row.get::<String,_>("message"),"details":serde_json::from_str::<serde_json::Value>(&row.get::<String,_>("details_json")).unwrap_or(serde_json::json!({}))})).collect())
    }

    pub async fn record_system_event(
        &self,
        level: &str,
        component: &str,
        event: &str,
        message: &str,
        details: serde_json::Value,
    ) -> anyhow::Result<()> {
        sqlx::query("INSERT INTO system_events(id, created_at, level, component, event, message, details_json) VALUES (?, ?, ?, ?, ?, ?, ?)")
            .bind(uuid::Uuid::new_v4().simple().to_string())
            .bind(Utc::now().to_rfc3339())
            .bind(level)
            .bind(component)
            .bind(event)
            .bind(message)
            .bind(details.to_string())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn revive_dormant(&self) -> anyhow::Result<u64> {
        let now = Utc::now().to_rfc3339();
        Ok(sqlx::query("UPDATE nodes SET lifecycle_state = 'PROBATION', status = 'PROBATION', next_test_at = ?, updated_at = ? WHERE lifecycle_state = 'DORMANT' AND structurally_valid = 1")
            .bind(&now).bind(&now).execute(&self.pool).await?.rows_affected())
    }

    pub async fn revive_node(&self, proxy_id: &str) -> anyhow::Result<()> {
        let now = Utc::now().to_rfc3339();
        let affected = sqlx::query("UPDATE nodes SET lifecycle_state = 'PROBATION', status = 'PROBATION', next_test_at = ?, test_lease_until = NULL, testing_from_state = NULL, updated_at = ? WHERE id = ? AND lifecycle_state IN ('DORMANT', 'PROBATION', 'CANDIDATE') AND structurally_valid = 1")
            .bind(&now).bind(&now).bind(proxy_id).execute(&self.pool).await?.rows_affected();
        anyhow::ensure!(affected == 1, "node not found or cannot be revived");
        Ok(())
    }

    pub async fn schedule_manual_test(&self, proxy_id: &str) -> anyhow::Result<()> {
        // A manual action should make an idle node due immediately, but must
        // never revoke an in-flight lease: the running worker owns that
        // observation and a second worker would double the network cost.
        let affected = sqlx::query("UPDATE nodes SET next_test_at = ?, updated_at = ? WHERE id = ? AND lifecycle_state <> 'TESTING'")
            .bind(Utc::now().to_rfc3339()).bind(Utc::now().to_rfc3339()).bind(proxy_id).execute(&self.pool).await?.rows_affected();
        if affected == 0 {
            let exists = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM nodes WHERE id = ?")
                .bind(proxy_id)
                .fetch_one(&self.pool)
                .await?;
            anyhow::ensure!(exists == 1, "node not found");
        }
        Ok(())
    }

    pub async fn cleanup_retired(&self) -> anyhow::Result<u64> {
        Ok(sqlx::query("DELETE FROM nodes WHERE lifecycle_state = 'RETIRED' AND tombstone_until IS NOT NULL AND tombstone_until <= ?")
            .bind(Utc::now().to_rfc3339()).execute(&self.pool).await?.rows_affected())
    }

    pub async fn candidates_for_client(
        &self,
        client: &str,
        assignment_ttl_seconds: u64,
    ) -> anyhow::Result<Vec<CandidateForSelection>> {
        let now = Utc::now();
        let assignment_since = now - chrono::Duration::seconds(assignment_ttl_seconds as i64);
        let usage_since = now - chrono::Duration::minutes(5);
        let client_usage_since = now - chrono::Duration::minutes(30);
        let rows = sqlx::query("SELECT n.id, n.main_port, n.relay_delay_ms, n.download_kbps, n.health_score, c.state AS client_state, c.cooldown_until, c.success_rate_ewma, c.fail_streak, c.rate_limit_streak, (SELECT COUNT(*) FROM assignment_events a WHERE a.node_id = n.id AND a.assigned_at >= ?) AS active_assignments, (SELECT COUNT(*) FROM usage_events u WHERE u.node_id = n.id AND u.created_at >= ?) AS recent_global_usage, (SELECT COUNT(*) FROM usage_events u WHERE u.node_id = n.id AND u.client_id = ? AND u.created_at >= ?) AS recent_client_usage FROM nodes n LEFT JOIN client_node_state c ON c.node_id = n.id AND c.client_id = ? WHERE n.lifecycle_state = 'ACTIVE' AND n.main_port IS NOT NULL AND n.structurally_valid = 1 AND n.publication_lease_until > ?")
            .bind(assignment_since.to_rfc3339()).bind(usage_since.to_rfc3339()).bind(client).bind(client_usage_since.to_rfc3339()).bind(client).bind(now.to_rfc3339()).fetch_all(&self.pool).await?;
        Ok(rows
            .into_iter()
            .filter_map(|row| {
                let cooldown: Option<String> = row.get("cooldown_until");
                let circuit_open = row.get::<Option<String>, _>("client_state").as_deref()
                    == Some("OPEN")
                    && parse_time(cooldown).map(|time| time > now).unwrap_or(false);
                (!circuit_open).then(|| CandidateForSelection {
                    id: row.get("id"),
                    main_port: row.get("main_port"),
                    relay_delay_ms: row.get("relay_delay_ms"),
                    download_kbps: row.get("download_kbps"),
                    health_score: row.get::<Option<f64>, _>("health_score").unwrap_or(0.5),
                    active_assignments: row.get("active_assignments"),
                    recent_global_usage: row.get("recent_global_usage"),
                    recent_client_usage: row.get("recent_client_usage"),
                    client_success_ewma: row.get("success_rate_ewma"),
                    client_fail_streak: row
                        .get::<Option<i64>, _>("fail_streak")
                        .unwrap_or_default(),
                    client_rate_limit_streak: row
                        .get::<Option<i64>, _>("rate_limit_streak")
                        .unwrap_or_default(),
                    circuit_open,
                })
            })
            .collect())
    }

    pub async fn best_vip_candidate(&self) -> anyhow::Result<Option<VipCandidate>> {
        let row = sqlx::query("SELECT id, raw_config, health_score, relay_delay_ms, download_kbps FROM nodes WHERE lifecycle_state = 'ACTIVE' AND structurally_valid = 1 AND publication_lease_until > ? ORDER BY health_score DESC, COALESCE(download_kbps, 0) DESC, COALESCE(relay_delay_ms, 999999) ASC, config_hash ASC LIMIT 1")
            .bind(Utc::now().to_rfc3339()).fetch_optional(&self.pool).await?;
        Ok(row.map(|row| {
            let health = row.get::<Option<f64>, _>("health_score").unwrap_or(0.5);
            let latency = row.get::<Option<i64>, _>("relay_delay_ms").unwrap_or(3000) as f64;
            let download = row
                .get::<Option<i64>, _>("download_kbps")
                .unwrap_or_default() as f64;
            VipCandidate {
                id: row.get("id"),
                raw_config: row.get("raw_config"),
                score: health * 0.60
                    + (download / 1000.0).clamp(0.0, 1.0) * 0.25
                    + (1.0 - latency / 3000.0).clamp(0.0, 1.0) * 0.15,
            }
        }))
    }

    pub async fn record_assignment(
        &self,
        client: &str,
        node_id: &str,
        port: u16,
    ) -> anyhow::Result<String> {
        let id = uuid::Uuid::new_v4().simple().to_string();
        let now = Utc::now().to_rfc3339();
        let mut transaction = self.pool.begin().await?;
        sqlx::query("INSERT INTO assignment_events(id, client_id, node_id, port, assigned_at) VALUES (?, ?, ?, ?, ?)").bind(&id).bind(client).bind(node_id).bind(port as i64).bind(&now).execute(&mut *transaction).await?;
        sqlx::query("INSERT INTO usage_events(id, client_id, node_id, event_type, created_at) VALUES (?, ?, ?, 'assigned', ?)").bind(uuid::Uuid::new_v4().simple().to_string()).bind(client).bind(node_id).bind(&now).execute(&mut *transaction).await?;
        sqlx::query("INSERT INTO client_node_state(client_id, node_id, last_assigned_at) VALUES (?, ?, ?) ON CONFLICT(client_id, node_id) DO UPDATE SET last_assigned_at = excluded.last_assigned_at").bind(client).bind(node_id).bind(&now).execute(&mut *transaction).await?;
        transaction.commit().await?;
        Ok(id)
    }

    pub async fn apply_client_feedback(
        &self,
        client: &str,
        node_id: &str,
        status: &str,
        config: &crate::config::AppConfig,
    ) -> anyhow::Result<()> {
        let exists = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM nodes WHERE id = ?")
            .bind(node_id)
            .fetch_one(&self.pool)
            .await?;
        anyhow::ensure!(exists > 0, "node not found");
        let now = Utc::now();
        let row = sqlx::query("SELECT state, fail_streak, rate_limit_streak, usage_count, success_count, broken_count, rate_limited_count, success_rate_ewma FROM client_node_state WHERE client_id = ? AND node_id = ?").bind(client).bind(node_id).fetch_optional(&self.pool).await?;
        let mut fail_streak = row
            .as_ref()
            .map(|row| row.get::<Option<i64>, _>("fail_streak").unwrap_or_default())
            .unwrap_or_default();
        let mut rate_limit_streak = row
            .as_ref()
            .map(|row| {
                row.get::<Option<i64>, _>("rate_limit_streak")
                    .unwrap_or_default()
            })
            .unwrap_or_default();
        let mut usage_count = row
            .as_ref()
            .map(|row| row.get::<Option<i64>, _>("usage_count").unwrap_or_default())
            .unwrap_or_default();
        let mut success_count = row
            .as_ref()
            .map(|row| {
                row.get::<Option<i64>, _>("success_count")
                    .unwrap_or_default()
            })
            .unwrap_or_default();
        let mut broken_count = row
            .as_ref()
            .map(|row| {
                row.get::<Option<i64>, _>("broken_count")
                    .unwrap_or_default()
            })
            .unwrap_or_default();
        let mut rate_limited_count = row
            .as_ref()
            .map(|row| {
                row.get::<Option<i64>, _>("rate_limited_count")
                    .unwrap_or_default()
            })
            .unwrap_or_default();
        let mut ewma = row
            .as_ref()
            .map(|row| {
                row.get::<Option<f64>, _>("success_rate_ewma")
                    .unwrap_or(0.5)
            })
            .unwrap_or(0.5);
        let (state, cooldown, successful) = match status {
            "used" => {
                fail_streak = 0;
                rate_limit_streak = 0;
                usage_count += 1;
                success_count += 1;
                ewma = ewma * 0.7 + 0.3;
                ("CLOSED", None, true)
            }
            "broken" => {
                fail_streak += 1;
                broken_count += 1;
                ewma *= 0.7;
                (
                    "OPEN",
                    Some(full_jitter_cooldown(
                        config.client_penalty.broken.base_cooldown_seconds,
                        config.client_penalty.broken.max_cooldown_seconds,
                        fail_streak as u32,
                    )),
                    false,
                )
            }
            "rate_limited" => {
                rate_limit_streak += 1;
                rate_limited_count += 1;
                ewma *= 0.7;
                (
                    "OPEN",
                    Some(full_jitter_cooldown(
                        config.client_penalty.rate_limited.base_cooldown_seconds,
                        config.client_penalty.rate_limited.max_cooldown_seconds,
                        rate_limit_streak as u32,
                    )),
                    false,
                )
            }
            _ => anyhow::bail!("invalid feedback status"),
        };
        let mut transaction = self.pool.begin().await?;
        sqlx::query("INSERT INTO client_node_state(client_id, node_id, state, fail_streak, rate_limit_streak, cooldown_until, usage_count, success_count, broken_count, rate_limited_count, success_rate_ewma, last_feedback_at, last_failure_at, last_success_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(client_id, node_id) DO UPDATE SET state=excluded.state, fail_streak=excluded.fail_streak, rate_limit_streak=excluded.rate_limit_streak, cooldown_until=excluded.cooldown_until, usage_count=excluded.usage_count, success_count=excluded.success_count, broken_count=excluded.broken_count, rate_limited_count=excluded.rate_limited_count, success_rate_ewma=excluded.success_rate_ewma, last_feedback_at=excluded.last_feedback_at, last_failure_at=excluded.last_failure_at, last_success_at=excluded.last_success_at")
            .bind(client).bind(node_id).bind(state).bind(fail_streak).bind(rate_limit_streak).bind(cooldown.map(|value| (now + value).to_rfc3339())).bind(usage_count).bind(success_count).bind(broken_count).bind(rate_limited_count).bind(ewma).bind(now.to_rfc3339()).bind((!successful).then(|| now.to_rfc3339())).bind(successful.then(|| now.to_rfc3339())).execute(&mut *transaction).await?;
        sqlx::query("UPDATE assignment_events SET feedback_status = ?, feedback_at = ? WHERE id = (SELECT id FROM assignment_events WHERE client_id = ? AND node_id = ? AND feedback_status IS NULL ORDER BY assigned_at DESC LIMIT 1)").bind(status).bind(now.to_rfc3339()).bind(client).bind(node_id).execute(&mut *transaction).await?;
        sqlx::query("INSERT INTO usage_events(id, client_id, node_id, event_type, created_at) VALUES (?, ?, ?, ?, ?)").bind(uuid::Uuid::new_v4().simple().to_string()).bind(client).bind(node_id).bind(status).bind(now.to_rfc3339()).execute(&mut *transaction).await?;
        transaction.commit().await?;
        Ok(())
    }
}

fn full_jitter_cooldown(base: u64, cap: u64, streak: u32) -> chrono::Duration {
    use rand::Rng;
    let ceiling = base
        .saturating_mul(1_u64 << streak.saturating_sub(1).min(20))
        .min(cap);
    chrono::Duration::seconds(rand::rng().random_range(0..=ceiling) as i64)
}

fn take(values: &std::collections::HashMap<String, i64>, key: &str) -> i64 {
    *values.get(key).unwrap_or(&0)
}
fn parse_time(value: Option<String>) -> Option<DateTime<Utc>> {
    value
        .and_then(|item| chrono::DateTime::parse_from_rfc3339(&item).ok())
        .map(|item| item.with_timezone(&Utc))
}

fn row_to_summary(row: sqlx::sqlite::SqliteRow) -> NodeSummary {
    let raw = row.get::<String, _>("raw_config");
    let protocol = raw.split("://").next().unwrap_or_default().to_owned();
    let parsed = url::Url::parse(&raw).ok();
    let server = parsed
        .as_ref()
        .and_then(url::Url::host_str)
        .unwrap_or_default()
        .to_owned();
    let remote_port = parsed.as_ref().and_then(url::Url::port).map(i64::from);
    let remark = parsed
        .and_then(|url| url.fragment().map(str::to_owned))
        .unwrap_or_default();
    let health_alpha = row.get::<Option<f64>, _>("health_alpha").unwrap_or(1.0);
    let health_beta = row.get::<Option<f64>, _>("health_beta").unwrap_or(1.0);
    let health_score = row.get::<Option<f64>, _>("health_score").unwrap_or(0.5);
    let last_failure_class = row.get::<Option<String>, _>("last_failure_class");
    NodeSummary {
        id: row.get("id"),
        config_hash: row.get("config_hash"),
        raw_config: raw,
        protocol,
        server,
        remote_port,
        remark,
        source_subs: serde_json::from_str(&row.get::<String, _>("source_subs")).unwrap_or_default(),
        status: row.get("status"),
        lifecycle_state: row.get("lifecycle_state"),
        main_port: row.get("main_port"),
        relay_delay_ms: row.get("relay_delay_ms"),
        download_kbps: row.get("download_kbps"),
        exit_country: row.get("exit_country"),
        health_success_ewma: row
            .get::<Option<f64>, _>("health_success_ewma")
            .unwrap_or(0.5),
        health_alpha,
        health_beta,
        health_score,
        next_test_at: parse_time(row.get("next_test_at")),
        publication_lease_until: parse_time(row.get("publication_lease_until")),
        publication_lease_kind: row.get("publication_lease_kind"),
        last_failure_class: last_failure_class.clone(),
        last_seen_generation: row.get("last_seen_generation"),
        upstream_missing_generations: row
            .get::<Option<i64>, _>("upstream_missing_generations")
            .unwrap_or_default(),
        evidence_summary: crate::domain::proxy::EvidenceSummary {
            alpha: health_alpha,
            beta: health_beta,
            score: health_score,
            last_failure_class,
        },
        created_at: parse_time(row.get("created_at")),
        last_test_at: parse_time(row.get("last_test_at")),
    }
}

const NODE_ADDITIONS: &[(&str, &str)] = &[
    ("lifecycle_state", "TEXT"),
    ("state_entered_at", "TEXT"),
    ("structurally_valid", "INTEGER"),
    ("health_alpha", "REAL"),
    ("health_beta", "REAL"),
    ("health_score", "REAL"),
    ("evidence_updated_at", "TEXT"),
    ("testing_from_state", "TEXT"),
    ("test_lease_until", "TEXT"),
    ("publication_lease_until", "TEXT"),
    ("publication_lease_kind", "TEXT"),
    ("activated_at", "TEXT"),
    ("last_success_at", "TEXT"),
    ("last_real_download_at", "TEXT"),
    ("last_failure_at", "TEXT"),
    ("last_failure_class", "TEXT"),
    ("last_failure_run_id", "TEXT"),
    ("last_download_test_at", "TEXT"),
    ("failure_streak", "INTEGER NOT NULL DEFAULT 0"),
    ("independent_failure_count", "INTEGER NOT NULL DEFAULT 0"),
    ("last_test_endpoint", "TEXT"),
    ("last_seen_generation", "INTEGER"),
    ("upstream_missing_generations", "INTEGER NOT NULL DEFAULT 0"),
    ("retired_at", "TEXT"),
    ("tombstone_until", "TEXT"),
];

const COMPAT_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS nodes (
 id TEXT PRIMARY KEY, config_hash TEXT UNIQUE NOT NULL, raw_config TEXT NOT NULL, normalized_config TEXT NOT NULL,
 source_subs TEXT NOT NULL, status TEXT NOT NULL, main_port INTEGER, relay_delay_ms INTEGER, download_kbps INTEGER,
 exit_country TEXT NOT NULL DEFAULT '', health_success_ewma REAL DEFAULT 1.0, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 last_test_at TEXT, last_download_test_at TEXT, next_test_at TEXT
);
CREATE TABLE IF NOT EXISTS client_node_state (client_id TEXT NOT NULL, node_id TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'CLOSED', fail_streak INTEGER DEFAULT 0, rate_limit_streak INTEGER DEFAULT 0, cooldown_until TEXT, usage_count INTEGER DEFAULT 0, success_count INTEGER DEFAULT 0, broken_count INTEGER DEFAULT 0, rate_limited_count INTEGER DEFAULT 0, recent_usage_score REAL DEFAULT 0, success_rate_ewma REAL DEFAULT 0.5, last_assigned_at TEXT, last_feedback_at TEXT, last_failure_at TEXT, last_success_at TEXT, PRIMARY KEY(client_id, node_id));
CREATE TABLE IF NOT EXISTS assignment_events (id TEXT PRIMARY KEY, client_id TEXT NOT NULL, node_id TEXT NOT NULL, port INTEGER NOT NULL, assigned_at TEXT NOT NULL, feedback_status TEXT, feedback_at TEXT);
CREATE TABLE IF NOT EXISTS usage_events (id TEXT PRIMARY KEY, client_id TEXT NOT NULL, node_id TEXT NOT NULL, event_type TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS test_history (id TEXT PRIMARY KEY, node_id TEXT NOT NULL, test_kind TEXT NOT NULL, trigger TEXT NOT NULL, started_at TEXT NOT NULL, finished_at TEXT NOT NULL, network_online INTEGER NOT NULL, ok INTEGER NOT NULL, latency_ms INTEGER, download_kbps INTEGER, error TEXT NOT NULL DEFAULT '', status_before TEXT NOT NULL DEFAULT '', status_after TEXT NOT NULL DEFAULT '', details_json TEXT NOT NULL DEFAULT '{}');
CREATE TABLE IF NOT EXISTS system_events (id TEXT PRIMARY KEY, created_at TEXT NOT NULL, level TEXT NOT NULL, component TEXT NOT NULL, event TEXT NOT NULL, message TEXT NOT NULL, details_json TEXT NOT NULL DEFAULT '{}');
"#;

const EVENT_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS proxy_test_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT, proxy_id TEXT NOT NULL, run_id TEXT NOT NULL, occurred_at TEXT NOT NULL,
 stage TEXT NOT NULL, result TEXT NOT NULL, failure_class TEXT NOT NULL, evidence_alpha REAL NOT NULL DEFAULT 0,
 evidence_beta REAL NOT NULL DEFAULT 0, latency_ms REAL, download_bps REAL, bytes_transferred INTEGER,
 duration_ms INTEGER, endpoint TEXT, system_pressure REAL, incident_id TEXT, detail_json TEXT NOT NULL DEFAULT '{}', half_life_seconds REAL NOT NULL DEFAULT 0
);
"#;
const UPSTREAM_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS upstream_refresh_runs (id TEXT PRIMARY KEY, generation INTEGER UNIQUE NOT NULL, started_at TEXT NOT NULL, finished_at TEXT, status TEXT NOT NULL, source_count INTEGER NOT NULL DEFAULT 0, successful_source_count INTEGER NOT NULL DEFAULT 0, parsed_count INTEGER NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '');
CREATE TABLE IF NOT EXISTS upstream_sources (name TEXT PRIMARY KEY, url TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1, last_success_at TEXT, etag TEXT, last_modified TEXT, failure_streak INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS upstream_generation_members (generation INTEGER NOT NULL, source_name TEXT NOT NULL, config_hash TEXT NOT NULL, seen_at TEXT NOT NULL, PRIMARY KEY(generation, source_name, config_hash));
"#;
const SCHEDULER_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS scheduler_state (key TEXT PRIMARY KEY, value_json TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS service_state (key TEXT PRIMARY KEY, value_json TEXT NOT NULL, updated_at TEXT NOT NULL);
"#;
const INDEXES: &[&str] = &[
    "CREATE INDEX IF NOT EXISTS idx_nodes_lifecycle_next_test ON nodes(lifecycle_state, next_test_at)",
    "CREATE INDEX IF NOT EXISTS idx_nodes_publishable ON nodes(publication_lease_until, structurally_valid, lifecycle_state)",
    "CREATE INDEX IF NOT EXISTS idx_nodes_test_lease ON nodes(test_lease_until)",
    "CREATE INDEX IF NOT EXISTS idx_nodes_upstream_generation ON nodes(last_seen_generation, upstream_missing_generations)",
    "CREATE INDEX IF NOT EXISTS idx_proxy_test_events_proxy_time ON proxy_test_events(proxy_id, occurred_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_proxy_test_events_run ON proxy_test_events(run_id)",
    "CREATE INDEX IF NOT EXISTS idx_system_events_created_at ON system_events(created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_test_history_node_finished ON test_history(node_id, finished_at DESC)",
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_share_url;

    #[tokio::test]
    async fn fresh_database_migrates_with_evidence_columns() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = Store::connect(temp.path().join("fleet.db"))
            .await
            .expect("connect");
        store.migrate().await.expect("migrate");
        let columns = sqlx::query("PRAGMA table_info(nodes)")
            .fetch_all(store.pool())
            .await
            .expect("columns");
        let names: std::collections::HashSet<String> =
            columns.into_iter().map(|row| row.get("name")).collect();
        assert!(names.contains("last_real_download_at"));
        assert!(names.contains("publication_lease_until"));
        assert!(names.contains("last_failure_run_id"));
        assert_eq!(store.counts().await.expect("counts").total, 0);
    }

    #[tokio::test]
    async fn service_state_is_persistent_and_idempotent() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = Store::connect(temp.path().join("fleet.db"))
            .await
            .expect("connect");
        store.migrate().await.expect("migrate");
        store
            .set_service_state("binary_version", serde_json::json!({"version":"one"}))
            .await
            .expect("write state");
        store
            .set_service_state("binary_version", serde_json::json!({"version":"two"}))
            .await
            .expect("overwrite state");
        assert_eq!(
            store
                .service_state("binary_version")
                .await
                .expect("read state"),
            Some(serde_json::json!({"version":"two"}))
        );
    }

    #[tokio::test]
    async fn scheduler_state_is_persistent_and_idempotent() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = Store::connect(temp.path().join("fleet.db"))
            .await
            .expect("connect");
        store.migrate().await.expect("migrate");
        store
            .set_scheduler_state("quota_debt", serde_json::json!({"candidate": 0.4}))
            .await
            .expect("first write");
        store
            .set_scheduler_state("quota_debt", serde_json::json!({"candidate": 0.8}))
            .await
            .expect("replacement write");
        assert_eq!(
            store.scheduler_state("quota_debt").await.expect("read"),
            Some(serde_json::json!({"candidate": 0.8}))
        );
    }

    #[tokio::test]
    async fn manual_test_does_not_revoke_an_active_test_lease() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = Store::connect(temp.path().join("fleet.db"))
            .await
            .expect("connect");
        store.migrate().await.expect("migrate");
        sqlx::query("INSERT INTO nodes(id, config_hash, raw_config, normalized_config, source_subs, status, lifecycle_state, structurally_valid, health_alpha, health_beta, health_score, created_at, updated_at, test_lease_until, testing_from_state) VALUES ('node', 'hash', 'vless://demo', '{}', '[]', 'TESTING', 'TESTING', 1, 1, 1, 0.5, ?, ?, ?, 'CANDIDATE')")
            .bind(Utc::now().to_rfc3339()).bind(Utc::now().to_rfc3339()).bind((Utc::now() + chrono::Duration::minutes(5)).to_rfc3339()).execute(store.pool()).await.expect("node");
        store
            .schedule_manual_test("node")
            .await
            .expect("manual test");
        let row = sqlx::query("SELECT lifecycle_state, test_lease_until, testing_from_state FROM nodes WHERE id = 'node'")
            .fetch_one(store.pool()).await.expect("node row");
        assert_eq!(row.get::<String, _>("lifecycle_state"), "TESTING");
        assert_eq!(row.get::<String, _>("testing_from_state"), "CANDIDATE");
        assert!(row.get::<Option<String>, _>("test_lease_until").is_some());
    }

    async fn test_store() -> (tempfile::TempDir, Store, String) {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = Store::connect(temp.path().join("fleet.db"))
            .await
            .expect("connect");
        store.migrate().await.expect("migrate");
        let proxy = parse_share_url(
            "vless://123e4567-e89b-12d3-a456-426614174000@example.com:443?security=tls&sni=example.com#display",
            "test",
        )
        .expect("parse");
        store.ingest_proxy(&proxy, 1).await.expect("ingest");
        let id = sqlx::query_scalar::<_, String>("SELECT id FROM nodes WHERE config_hash = ?")
            .bind(proxy.config_hash)
            .fetch_one(store.pool())
            .await
            .expect("node id");
        (temp, store, id)
    }

    fn event(id: &str, run_id: &str, stage: TestStage, class: FailureClass) -> TestEventInput {
        TestEventInput {
            proxy_id: id.to_owned(),
            run_id: run_id.to_owned(),
            stage,
            class,
            fast_download: true,
            latency_ms: Some(10.0),
            download_bps: Some(1_000_000.0),
            bytes_transferred: Some(1_000_000),
            duration_ms: Some(1000),
            endpoint: Some("https://example.test".to_owned()),
            system_pressure: Some(0.1),
            incident_id: None,
            detail_json: serde_json::json!({}),
        }
    }

    #[tokio::test]
    async fn repeated_stage_in_one_run_is_idempotent() {
        let (_temp, store, id) = test_store().await;
        let first = store
            .apply_test_event(
                event(&id, "one-run", TestStage::Download, FailureClass::Success),
                std::time::Duration::from_secs(10),
                std::time::Duration::from_secs(300),
            )
            .await
            .expect("first event");
        let second = store
            .apply_test_event(
                event(&id, "one-run", TestStage::Download, FailureClass::Success),
                std::time::Duration::from_secs(10),
                std::time::Duration::from_secs(300),
            )
            .await
            .expect("duplicate event");
        let count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM proxy_test_events")
            .fetch_one(store.pool())
            .await
            .expect("event count");
        assert_eq!(count, 1);
        assert_eq!(first.lifecycle_state, "ACTIVE");
        assert_eq!(first.lifecycle_state, second.lifecycle_state);
        assert!((first.health_score - second.health_score).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn frequent_successes_do_not_make_health_evidence_unbounded() {
        let (_temp, store, id) = test_store().await;
        for run in 0..40 {
            store
                .apply_test_event(
                    event(
                        &id,
                        &format!("relay-{run}"),
                        TestStage::Relay,
                        FailureClass::Success,
                    ),
                    std::time::Duration::from_secs(10),
                    std::time::Duration::from_secs(300),
                )
                .await
                .expect("relay success");
        }
        let alpha = sqlx::query_scalar::<_, f64>("SELECT health_alpha FROM nodes WHERE id = ?")
            .bind(id)
            .fetch_one(store.pool())
            .await
            .expect("alpha");
        assert!(alpha <= MAX_EFFECTIVE_ALPHA);
    }

    #[tokio::test]
    async fn endpoint_incident_does_not_demote_or_shorten_an_active_lease() {
        let (_temp, store, id) = test_store().await;
        let active = store
            .apply_test_event(
                event(&id, "download", TestStage::Download, FailureClass::Success),
                std::time::Duration::from_secs(10),
                std::time::Duration::from_secs(300),
            )
            .await
            .expect("activate");
        let incident = store
            .apply_test_event(
                event(
                    &id,
                    "incident",
                    TestStage::Http,
                    FailureClass::EndpointFailure,
                ),
                std::time::Duration::from_secs(10),
                std::time::Duration::from_secs(300),
            )
            .await
            .expect("incident event");
        assert_eq!(incident.lifecycle_state, "ACTIVE");
        assert!(incident.inconclusive);
        assert_eq!(
            incident.publication_lease_until,
            active.publication_lease_until
        );
    }

    #[tokio::test]
    async fn cached_source_members_are_carried_into_a_not_modified_generation() {
        let (_temp, store, id) = test_store().await;
        let copied = store
            .copy_cached_source_generation("test", 2)
            .await
            .expect("copy membership");
        assert_eq!(copied, 1);
        let generation = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT last_seen_generation FROM nodes WHERE id = ?",
        )
        .bind(id)
        .fetch_one(store.pool())
        .await
        .expect("generation");
        assert_eq!(generation, Some(2));
        let memberships = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM upstream_generation_members WHERE generation = 2 AND source_name = 'test'",
        )
        .fetch_one(store.pool())
        .await
        .expect("memberships");
        assert_eq!(memberships, 1);
    }

    #[tokio::test]
    async fn paged_nodes_never_serialize_the_raw_proxy_credential() {
        let (_temp, store, _id) = test_store().await;
        let page = store
            .list_nodes(1, 1, None, None, None)
            .await
            .expect("page");
        let value = serde_json::to_value(&page.nodes[0]).expect("node JSON");
        assert!(value.get("raw_config").is_none());
        assert_eq!(value["server"], "example.com");
    }

    #[tokio::test]
    async fn recently_downloaded_active_is_reconciled_as_runtime_candidate() {
        let (_temp, store, id) = test_store().await;
        store
            .apply_test_event(
                event(&id, "download", TestStage::Download, FailureClass::Success),
                std::time::Duration::from_secs(10),
                std::time::Duration::from_secs(300),
            )
            .await
            .expect("activate");
        let candidates = store.active_runtime_candidates().await.expect("candidates");
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].id, id);
    }

    #[tokio::test]
    async fn partial_refresh_never_counts_missing_and_leased_active_survives_complete_misses() {
        let (_temp, store, id) = test_store().await;
        store
            .apply_test_event(
                event(&id, "download", TestStage::Download, FailureClass::Success),
                std::time::Duration::from_secs(10),
                std::time::Duration::from_secs(300),
            )
            .await
            .expect("activate");
        // Make the fixture eligible for retirement except for its publication
        // lease.  A partial refresh must not touch its missing counter.
        sqlx::query("UPDATE nodes SET created_at = ? WHERE id = ?")
            .bind((Utc::now() - chrono::Duration::days(2)).to_rfc3339())
            .bind(&id)
            .execute(store.pool())
            .await
            .expect("age fixture");

        let (partial_id, partial_generation) = store.begin_refresh(1).await.expect("partial run");
        assert!(
            !store
                .finish_refresh(&partial_id, partial_generation, 1, 0, 0, 3)
                .await
                .expect("finish partial")
        );
        let missing = sqlx::query_scalar::<_, i64>(
            "SELECT upstream_missing_generations FROM nodes WHERE id = ?",
        )
        .bind(&id)
        .fetch_one(store.pool())
        .await
        .expect("missing after partial");
        assert_eq!(missing, 0);

        // Three successful-but-empty source generations are enough to mark a
        // config stale, but never enough to remove a currently leased ACTIVE
        // proxy from the public subscription.
        for _ in 0..3 {
            let (run_id, generation) = store.begin_refresh(1).await.expect("complete run");
            assert!(
                store
                    .finish_refresh(&run_id, generation, 1, 1, 0, 3)
                    .await
                    .expect("finish complete")
            );
        }
        let state =
            sqlx::query_scalar::<_, String>("SELECT lifecycle_state FROM nodes WHERE id = ?")
                .bind(&id)
                .fetch_one(store.pool())
                .await
                .expect("state with lease");
        assert_eq!(state, "ACTIVE");

        // Retirement becomes possible only after lease expiry and another
        // complete generation; it is never caused by a single missed fetch.
        sqlx::query("UPDATE nodes SET publication_lease_until = ? WHERE id = ?")
            .bind((Utc::now() - chrono::Duration::seconds(1)).to_rfc3339())
            .bind(&id)
            .execute(store.pool())
            .await
            .expect("expire lease");
        let (run_id, generation) = store.begin_refresh(1).await.expect("expiry run");
        store
            .finish_refresh(&run_id, generation, 1, 1, 0, 3)
            .await
            .expect("finish expiry run");
        let retired =
            sqlx::query_scalar::<_, String>("SELECT lifecycle_state FROM nodes WHERE id = ?")
                .bind(&id)
                .fetch_one(store.pool())
                .await
                .expect("retired state");
        assert_eq!(retired, "RETIRED");
    }

    #[tokio::test]
    async fn diagnostics_snapshots_are_bounded_and_include_source_health() {
        let (_temp, store, _id) = test_store().await;
        store
            .record_source_success("test", "https://example.test/sub", Some("tag"), None)
            .await
            .expect("source success");
        let (run_id, generation) = store.begin_refresh(1).await.expect("refresh");
        store
            .finish_refresh(&run_id, generation, 1, 1, 1, 3)
            .await
            .expect("finish");
        let scheduler = store
            .scheduler_snapshot()
            .await
            .expect("scheduler snapshot");
        assert_eq!(scheduler["queues"].as_array().map(Vec::len), Some(1));
        let upstream = store.upstream_snapshot().await.expect("upstream snapshot");
        assert_eq!(upstream["latest"]["generation"], generation);
        assert_eq!(upstream["sources"][0]["name"], "test");
        assert_eq!(upstream["sources"][0]["etag"], "tag");
    }

    #[tokio::test]
    async fn node_page_exposes_evidence_and_generation_fields_with_a_hard_page_cap() {
        let (_temp, store, _id) = test_store().await;
        let page = store
            .list_nodes(0, 10_000, None, None, None)
            .await
            .expect("node page");
        assert_eq!(page.page, 1);
        assert_eq!(page.page_size, 200);
        let node = page.nodes.first().expect("node");
        assert_eq!(node.last_seen_generation, Some(1));
        assert_eq!(node.upstream_missing_generations, 0);
        assert_eq!(node.evidence_summary.score, node.health_score);
        assert_eq!(node.evidence_summary.alpha, node.health_alpha);
    }
}
