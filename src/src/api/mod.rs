//! HTTP compatibility surface for the existing panel and local clients.
//!
//! The API deliberately exposes stable JSON rather than binding the UI to
//! SQLite.  This lets the scheduler and state machine change without breaking
//! v2rayN-facing tools or the dashboard.

use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::json;
use tower_http::timeout::TimeoutLayer;

use crate::{SERVICE_VERSION, app::AppState, parser::parse_subscription, selection, upstream};

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/clients", get(index))
        .route("/diag", get(index))
        .route("/docs", get(index))
        .route("/logs", get(index))
        .route("/history", get(index))
        .route("/manual-import", get(index))
        .route("/health", get(health))
        .route("/api/v1/nodes", get(nodes))
        .route("/api/v1/nodes/{id}/config", get(node_config))
        .route("/api/v1/nodes/{id}/history", get(node_history))
        .route("/api/v1/nodes/{id}/test", post(node_test))
        .route("/api/v1/nodes/{id}/revive", post(node_revive))
        .route("/api/v1/network", get(network))
        .route("/api/v1/vip", get(vip))
        .route("/api/v1/scheduler", get(scheduler))
        .route("/api/v1/health-model", get(health_model))
        .route("/api/v1/upstream", get(upstream_status))
        .route("/api/v1/incidents", get(incidents))
        .route("/api/v1/publisher", get(publisher_status))
        .route("/api/v1/clients", get(clients))
        .route("/api/v1/client-status", get(client_status))
        .route("/api/v1/logs", get(logs))
        .route("/api/v1/best", post(best))
        .route("/api/v1/feedback", post(feedback))
        .route("/api/v1/manual-import", post(manual_import))
        .route("/api/v1/nodes/dead/clear", post(revive_dormant))
        .route("/api/v1/subscriptions/reload", post(reload_subscriptions))
        .route("/api/v1/db/cleanup", post(cleanup_database))
        // An HTTP request must have its own deadline; probe/Xray workers use
        // separate cancellation and timeout ownership.
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(20),
        ))
        .with_state(state)
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../../assets/index.html"))
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    // `/health` is deliberately read-only and O(1): the heartbeat refreshes
    // this bounded snapshot off the request path every five seconds.
    let runtime = state.runtime.read().await;
    (
        StatusCode::OK,
        Json(json!({"status":"ok", "service": state.config.service.name, "version": SERVICE_VERSION, "counts": runtime.fleet_counts, "last_tick_at": runtime.last_tick_at})),
    )
        .into_response()
}

#[derive(Deserialize)]
struct NodeQuery {
    page: Option<u64>,
    page_size: Option<u64>,
    status: Option<String>,
    country: Option<String>,
    search: Option<String>,
    source: Option<String>,
    protocol: Option<String>,
    failure_class: Option<String>,
}

async fn nodes(State(state): State<AppState>, Query(query): Query<NodeQuery>) -> impl IntoResponse {
    let page = query.page.unwrap_or(1);
    let page_size = query.page_size.unwrap_or(50);
    match state
        .store
        .list_nodes(
            page,
            page_size,
            crate::storage::NodeFilters {
                status: non_empty(query.status),
                country: non_empty(query.country),
                search: non_empty(query.search),
                source: non_empty(query.source),
                protocol: non_empty(query.protocol),
                failure_class: non_empty(query.failure_class),
            },
        )
        .await
    {
        Ok(result) => {
            let counts = match state.store.counts().await {
                Ok(counts) => counts,
                Err(error) => return api_error(error),
            };
            let countries = match state.store.list_exit_countries().await {
                Ok(countries) => countries,
                Err(error) => return api_error(error),
            };
            let vip = state.xray_runtimes.vip_status().await;
            let vip_id = vip.as_ref().map(|(id, _, _)| id.as_str());
            let mut nodes = Vec::with_capacity(result.nodes.len());
            for node in result.nodes {
                let port = state.xray_runtimes.port_for(&node.id).await;
                let mut value = match serde_json::to_value(node) {
                    Ok(serde_json::Value::Object(value)) => value,
                    Ok(_) => unreachable!("node summary must serialize as an object"),
                    Err(error) => return api_error(anyhow::Error::from(error)),
                };
                value.insert("runtime_running".to_owned(), json!(port.is_some()));
                value.insert("runtime_port".to_owned(), json!(port));
                value.insert(
                    "is_vip".to_owned(),
                    json!(vip_id == value.get("id").and_then(|id| id.as_str())),
                );
                // Compatibility keys of the Python dashboard. Values without
                // an equivalent historical source remain explicit zero/null;
                // clients never need to infer a missing field from a Rust
                // response.
                for key in [
                    "open_assignments",
                    "total_assignments",
                    "used_count",
                    "broken_count",
                    "rate_limited_count",
                    "total_clients",
                    "open_clients",
                    "half_open_clients",
                    "closed_clients",
                    "consecutive_relay_failures",
                ] {
                    value.insert(key.to_owned(), json!(0));
                }
                nodes.push(serde_json::Value::Object(value));
            }
            let total_pages = (result.total.max(1) as u64)
                .div_ceil(result.page_size)
                .max(1);
            let status_counts = json!({
                "ACTIVE": counts.active,
                "PROBATION": counts.probation,
                "CANDIDATE": counts.candidate,
                "TESTING": counts.testing,
                "DORMANT": counts.dormant,
                "DEAD": counts.dormant,
                "INVALID": counts.invalid,
                "RETIRED": counts.retired,
                "REMOVED": counts.retired,
                "WAITING_FOR_PORT": counts.waiting_for_port,
            });
            let runtime = state.runtime.read().await;
            let network = json!({
                "enabled": state.config.network_guard.enabled,
                "online": !runtime.network_incident,
                "status": if runtime.network_incident { "INCIDENT" } else { "HEALTHY" },
                "message": runtime.network_message,
            });
            let vip = match vip {
                Some((node_id, score, started_at)) => {
                    json!({"enabled":true,"running":true,"port":state.config.vip_port.port,"node_id":node_id,"score":score,"started_at":started_at})
                }
                None => {
                    json!({"enabled":state.config.vip_port.enabled,"running":false,"port":state.config.vip_port.port,"node_id":null,"score":null})
                }
            };
            Json(json!({
                "service": state.config.service.name,
                "environment": state.config.service.environment,
                "generated_at": chrono::Utc::now(),
                "total_nodes": counts.total,
                "filtered_total": result.total,
                "page": result.page,
                "page_size": result.page_size,
                "total_pages": total_pages,
                "status_counts": status_counts,
                "countries": countries,
                "network": network,
                "vip": vip,
                "pagination":{"page":result.page,"page_size":result.page_size,"total":result.total},
                "nodes":nodes,
            }))
            .into_response()
        }
        Err(error) => api_error(error),
    }
}

fn non_empty(value: Option<String>) -> Option<String> {
    value.and_then(|value| (!value.trim().is_empty()).then(|| value.trim().to_owned()))
}

async fn node_config(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    match state.store.raw_config(&id).await {
        Ok(Some(raw_config)) => Json(json!({"node_id":id,"raw_config":raw_config})).into_response(),
        Ok(None) => not_found("NODE_NOT_FOUND"),
        Err(error) => api_error(error),
    }
}

#[derive(Deserialize)]
struct HistoryQuery {
    limit: Option<u64>,
}

async fn node_history(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<HistoryQuery>,
) -> impl IntoResponse {
    match state.store.history(&id, query.limit.unwrap_or(50)).await {
        Ok(history) => Json(json!({"node_id":id,"history":history})).into_response(),
        Err(error) if error.to_string().contains("node not found") => not_found("NODE_NOT_FOUND"),
        Err(error) => api_error(error),
    }
}

async fn node_test(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    match state.store.schedule_manual_test(&id).await {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({"ok":true,"node_id":id,"scheduled":true})),
        )
            .into_response(),
        Err(error) if error.to_string().contains("node not found") => not_found("NODE_NOT_FOUND"),
        Err(error) => api_error(error),
    }
}

async fn node_revive(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    match state.store.revive_node(&id).await {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({"ok":true,"node_id":id,"state":"PROBATION"})),
        )
            .into_response(),
        Err(error)
            if error.to_string().contains("node not found")
                || error.to_string().contains("cannot be revived") =>
        {
            not_found("NODE_NOT_REVIVABLE")
        }
        Err(error) => api_error(error),
    }
}

async fn network(State(state): State<AppState>) -> Json<serde_json::Value> {
    let runtime = state.runtime.read().await;
    Json(
        json!({"enabled": state.config.network_guard.enabled, "status": if runtime.network_incident { "INCIDENT" } else { "HEALTHY" }, "message":runtime.network_message}),
    )
}
async fn vip(State(state): State<AppState>) -> Json<serde_json::Value> {
    let runtime = state.xray_runtimes.vip_status().await;
    match runtime {
        Some((node_id, score, started_at)) => Json(
            json!({"enabled":true,"running":true,"port":state.config.vip_port.port,"node_id":node_id,"score":score,"started_at":started_at.to_rfc3339()}),
        ),
        None => Json(
            json!({"enabled":state.config.vip_port.enabled,"running":false,"port":state.config.vip_port.port,"node_id":null,"score":null}),
        ),
    }
}
async fn scheduler(State(state): State<AppState>) -> impl IntoResponse {
    let runtime = state.runtime.read().await.clone();
    match state.store.scheduler_snapshot().await {
        Ok(snapshot) => Json(
            json!({"runtime":runtime,"policy":{"new":40,"successful_probation":30,"recoverable_dormant":20,"exploration":10},"concurrency":{"xray_current":runtime.xray_concurrency,"download_current":runtime.download_concurrency},"snapshot":snapshot}),
        )
        .into_response(),
        Err(error) => api_error(error),
    }
}
async fn health_model() -> Json<serde_json::Value> {
    Json(
        json!({"model":"beta-bayesian-decay", "activation":"real_download", "lease_hours":{"fast_download":12,"acceptable_download":6,"http":2,"relay_minutes":30}}),
    )
}
async fn upstream_status(State(state): State<AppState>) -> impl IntoResponse {
    match state.store.upstream_snapshot().await {
        Ok(snapshot) => Json(snapshot).into_response(),
        Err(error) => api_error(error),
    }
}
async fn incidents(State(state): State<AppState>) -> impl IntoResponse {
    match state.store.logs(100, Some("incident"), None).await {
        Ok(recent) => {
            let runtime = state.runtime.read().await;
            Json(json!({"active":if runtime.network_incident { vec![json!({"kind":"network","message":runtime.network_message})] } else { vec![] },"recent":recent})).into_response()
        }
        Err(error) => api_error(error),
    }
}
async fn publisher_status(State(state): State<AppState>) -> impl IntoResponse {
    match state.store.service_state("last_publisher").await {
        Ok(status) => {
            Json(json!({"enabled":state.config.publishing.enabled,"status":status})).into_response()
        }
        Err(error) => api_error(error),
    }
}

async fn clients(State(state): State<AppState>) -> impl IntoResponse {
    match state.store.list_clients().await {
        Ok(clients) => Json(json!({"clients":clients})).into_response(),
        Err(error) => api_error(error),
    }
}
#[derive(Deserialize)]
struct ClientQuery {
    client: Option<String>,
    page: Option<u64>,
    page_size: Option<u64>,
}
async fn client_status(
    State(state): State<AppState>,
    Query(query): Query<ClientQuery>,
) -> impl IntoResponse {
    let client = query.client.unwrap_or_default();
    if client.trim().is_empty() {
        return bad_request("INVALID_CLIENT");
    }
    match state
        .store
        .client_status(
            &client,
            query.page.unwrap_or(1),
            query.page_size.unwrap_or(100),
        )
        .await
    {
        Ok(value) => Json(value).into_response(),
        Err(error) => api_error(error),
    }
}
#[derive(Deserialize)]
struct LogQuery {
    limit: Option<u64>,
    component: Option<String>,
    level: Option<String>,
}
async fn logs(State(state): State<AppState>, Query(query): Query<LogQuery>) -> impl IntoResponse {
    match state
        .store
        .logs(
            query.limit.unwrap_or(200),
            query.component.as_deref(),
            query.level.as_deref(),
        )
        .await
    {
        Ok(logs) => Json(json!({"logs":logs})).into_response(),
        Err(error) => api_error(error),
    }
}

#[derive(Deserialize)]
struct BestRequest {
    client: String,
}
async fn best(
    State(state): State<AppState>,
    Json(payload): Json<BestRequest>,
) -> impl IntoResponse {
    let client = payload.client.trim();
    if client.is_empty() {
        return bad_request("INVALID_CLIENT");
    }
    match selection::best(&state.store, &state.config, client).await {
        Ok(Some(decision)) => Json(json!({"node_id":decision.node_id,"port":decision.port,"client":client,"assignment_id":decision.assignment_id,"relay_delay_ms":decision.relay_delay_ms,"expires_in_seconds":decision.expires_in_seconds})).into_response(),
        Ok(None) => (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error":"NO_AVAILABLE_NODE","message":"No healthy node is currently available for this client."}))).into_response(),
        Err(error) => api_error(error),
    }
}
#[derive(Deserialize)]
struct FeedbackRequest {
    client: String,
    node_id: String,
    status: String,
}
async fn feedback(
    State(state): State<AppState>,
    Json(payload): Json<FeedbackRequest>,
) -> impl IntoResponse {
    if payload.client.trim().is_empty() || payload.node_id.trim().is_empty() {
        return bad_request("INVALID_FEEDBACK");
    }
    match selection::feedback(
        &state.store,
        &state.config,
        payload.client.trim(),
        payload.node_id.trim(),
        payload.status.trim(),
    )
    .await
    {
        Ok(()) => Json(json!({"ok":true})).into_response(),
        Err(error) if error.to_string().contains("invalid feedback") => {
            bad_request("INVALID_FEEDBACK")
        }
        Err(error) => api_error(error),
    }
}
#[derive(Deserialize)]
struct ManualImportRequest {
    #[serde(alias = "content")]
    configs: String,
}
async fn manual_import(
    State(state): State<AppState>,
    Json(payload): Json<ManualImportRequest>,
) -> impl IntoResponse {
    let report = parse_subscription(&payload.configs, "manual");
    if let Err(error) = state
        .store
        .record_invalid_config_rejections("manual", &report.rejected)
        .await
    {
        return api_error(error);
    }
    let mut inserted = 0_u64;
    for proxy in &report.accepted {
        match state.store.ingest_proxy(proxy, 0).await {
            Ok(true) => inserted += 1,
            Ok(false) => {}
            Err(error) => return api_error(error),
        }
    }
    Json(json!({"ok":true,"accepted":report.accepted.len(),"rejected":report.rejected.len(),"inserted":inserted,"errors":report.rejected})).into_response()
}
async fn revive_dormant(State(state): State<AppState>) -> impl IntoResponse {
    match state.store.revive_dormant().await {
        Ok(count) => Json(json!({"ok":true,"revived":count})).into_response(),
        Err(error) => api_error(error),
    }
}
async fn reload_subscriptions(State(state): State<AppState>) -> impl IntoResponse {
    const OPERATION_ID: &str = "upstream-refresh";
    if state
        .upstream_refresh_in_progress
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return (
            StatusCode::ACCEPTED,
            Json(json!({"ok":true,"scheduled":false,"operation_id":OPERATION_ID,"message":"already running"})),
        ).into_response();
    }
    let store = state.store.clone();
    let config: Arc<crate::config::AppConfig> = state.config.clone();
    let in_progress = state.upstream_refresh_in_progress.clone();
    tokio::spawn(async move {
        if let Err(error) = upstream::refresh(&store, config).await {
            tracing::warn!(%error, "manual upstream refresh failed");
        }
        in_progress.store(false, Ordering::Release);
    });
    (
        StatusCode::ACCEPTED,
        Json(json!({"ok":true,"scheduled":true,"operation_id":OPERATION_ID})),
    )
        .into_response()
}
async fn cleanup_database(State(state): State<AppState>) -> impl IntoResponse {
    match state.store.cleanup_retired().await {
        Ok(count) => Json(json!({"ok":true,"deleted_retired":count})).into_response(),
        Err(error) => api_error(error),
    }
}

fn api_error(error: anyhow::Error) -> axum::response::Response {
    error_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal_error",
        error.to_string(),
        json!({}),
    )
}

fn bad_request(code: &str) -> axum::response::Response {
    error_response(
        StatusCode::BAD_REQUEST,
        code,
        "request validation failed",
        json!({}),
    )
}
fn not_found(code: &str) -> axum::response::Response {
    error_response(StatusCode::NOT_FOUND, code, "resource not found", json!({}))
}

fn error_response(
    status: StatusCode,
    code: &str,
    message: impl Into<String>,
    details: serde_json::Value,
) -> axum::response::Response {
    (
        status,
        Json(json!({"error":{"code":code,"message":message.into(),"details":details}})),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, atomic::Ordering};

    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use crate::{app::AppState, config::AppConfig, parser::parse_share_url, storage::Store};

    async fn test_router_with_store() -> (tempfile::TempDir, Store, axum::Router) {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = Store::connect(temp.path().join("fleet.db"))
            .await
            .expect("connect");
        store.migrate().await.expect("migrate");
        let state = AppState::new(
            Arc::new(AppConfig::default()),
            store.clone(),
            tokio_util::sync::CancellationToken::new(),
        );
        (temp, store, super::router(state))
    }

    async fn test_router() -> (tempfile::TempDir, axum::Router) {
        let (temp, _store, app) = test_router_with_store().await;
        (temp, app)
    }

    #[tokio::test]
    async fn nodes_endpoint_keeps_legacy_pagination_without_exposing_credentials() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = Store::connect(temp.path().join("fleet.db"))
            .await
            .expect("store");
        store.migrate().await.expect("migrate");
        let proxy = parse_share_url(
            "vless://123e4567-e89b-12d3-a456-426614174000:secret@example.com:443?security=tls#display",
            "fixture",
        )
        .expect("proxy");
        store.ingest_proxy(&proxy, 1).await.expect("ingest");
        let state = AppState::new(
            Arc::new(AppConfig::default()),
            store,
            tokio_util::sync::CancellationToken::new(),
        );
        let response = super::router(state)
            .oneshot(
                Request::builder()
                    .uri("/api/v1/nodes?page=1&page_size=1")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert!(response.status().is_success());
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("JSON");
        assert_eq!(payload["page"], 1);
        assert_eq!(payload["pagination"]["page_size"], 1);
        assert!(payload.get("filtered_total").is_some());
        assert_eq!(payload["nodes"].as_array().expect("nodes").len(), 1);
        assert!(payload["nodes"][0].get("raw_config").is_none());
    }

    #[tokio::test]
    async fn compatibility_routes_are_available_and_bounded() {
        let (_temp, app) = test_router().await;
        for uri in [
            "/health",
            "/api/v1/nodes?page=1&page_size=100000",
            "/api/v1/network",
            "/api/v1/vip",
            "/api/v1/scheduler",
            "/api/v1/health-model",
            "/api/v1/upstream",
            "/api/v1/incidents",
            "/api/v1/publisher",
            "/api/v1/clients",
            "/api/v1/logs",
            "/",
            "/clients",
            "/diag",
            "/logs",
            "/history",
            "/manual-import",
            "/docs",
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::OK, "{uri}");
        }
    }

    #[tokio::test]
    async fn compatibility_payload_shapes_keep_legacy_keys_and_only_add_fields() {
        let (_temp, app) = test_router().await;
        for (uri, required) in [
            ("/health", &["status", "service", "version", "counts"][..]),
            (
                "/api/v1/nodes?page=1&page_size=1",
                &["nodes", "page", "page_size", "pagination", "filtered_total"][..],
            ),
            ("/api/v1/clients", &["clients"][..]),
            ("/api/v1/network", &["enabled", "status", "message"][..]),
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::OK, "{uri}");
            let bytes = response
                .into_body()
                .collect()
                .await
                .expect("body")
                .to_bytes();
            let payload: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON");
            for key in required {
                assert!(payload.get(*key).is_some(), "{uri} is missing {key}");
            }
        }
    }

    #[tokio::test]
    async fn manual_import_and_head_health_follow_contract() {
        let (_temp, app) = test_router().await;
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/manual-import")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"configs":"vless://123e4567-e89b-12d3-a456-426614174000@example.com:443?security=tls#demo"}"#,
            ))
            .unwrap();
        let response = app.clone().oneshot(request).await.expect("import response");
        assert_eq!(response.status(), StatusCode::OK);
        let head = app
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("head response");
        assert_eq!(head.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn manual_import_accepts_legacy_content_field() {
        let (_temp, app) = test_router().await;
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/manual-import")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"content":"vless://123e4567-e89b-12d3-a456-426614174000@example.com:443?security=tls#demo"}"#,
            ))
            .unwrap();
        let response = app.oneshot(request).await.expect("import response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn dashboard_mutation_actions_smoke_without_external_refresh() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = Store::connect(temp.path().join("fleet.db"))
            .await
            .expect("store");
        store.migrate().await.expect("migrate");
        let proxy = parse_share_url(
            "vless://123e4567-e89b-12d3-a456-426614174000@example.com:443?security=tls#action-smoke",
            "fixture",
        ).expect("proxy");
        store.ingest_proxy(&proxy, 1).await.expect("ingest");
        let id = sqlx::query_scalar::<_, String>("SELECT id FROM nodes WHERE config_hash = ?")
            .bind(proxy.config_hash)
            .fetch_one(store.pool())
            .await
            .expect("node id");
        sqlx::query("UPDATE nodes SET lifecycle_state = 'DORMANT', status = 'DORMANT'")
            .execute(store.pool())
            .await
            .expect("mark dormant");
        let state = AppState::new(
            Arc::new(AppConfig::default()),
            store,
            tokio_util::sync::CancellationToken::new(),
        );
        // Exercise the no-overlap response rather than starting a real network
        // refresh from a unit test.
        state
            .upstream_refresh_in_progress
            .store(true, Ordering::Release);
        let app = super::router(state);
        for (method, uri, expected) in [
            ("POST", format!("/api/v1/nodes/{id}/revive"), StatusCode::OK),
            ("POST", format!("/api/v1/nodes/{id}/test"), StatusCode::OK),
            (
                "POST",
                "/api/v1/nodes/dead/clear".to_owned(),
                StatusCode::OK,
            ),
            ("POST", "/api/v1/db/cleanup".to_owned(), StatusCode::OK),
            (
                "POST",
                "/api/v1/subscriptions/reload".to_owned(),
                StatusCode::ACCEPTED,
            ),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(uri)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .expect("action response");
            assert_eq!(response.status(), expected);
        }
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/best")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"client":"smoke-client"}"#))
                    .unwrap(),
            )
            .await
            .expect("best response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn manual_import_success_flows_to_active_publication_snapshot() {
        let (_temp, store, app) = test_router_with_store().await;
        let raw = "vless://123e4567-e89b-12d3-a456-426614174000@example.com:443?security=tls#e2e";
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/manual-import")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::json!({"configs":raw}).to_string()))
                    .expect("request"),
            )
            .await
            .expect("import response");
        assert_eq!(response.status(), StatusCode::OK);
        let id = sqlx::query_scalar::<_, String>("SELECT id FROM nodes WHERE raw_config = ?")
            .bind(raw)
            .fetch_one(store.pool())
            .await
            .expect("imported node");
        store
            .apply_test_event(
                crate::storage::TestEventInput {
                    proxy_id: id,
                    run_id: "e2e-download".to_owned(),
                    stage: crate::domain::evidence::TestStage::Download,
                    class: crate::domain::failure::FailureClass::Success,
                    fast_download: true,
                    latency_ms: Some(10.0),
                    download_bps: Some(1_000_000.0),
                    bytes_transferred: Some(1_000_000),
                    duration_ms: Some(1_000),
                    endpoint: Some("https://example.test".to_owned()),
                    system_pressure: Some(0.1),
                    incident_id: None,
                    detail_json: serde_json::json!({"test":"e2e"}),
                },
                std::time::Duration::from_secs(10),
                std::time::Duration::from_secs(300),
                std::time::Duration::from_secs(1800),
            )
            .await
            .expect("download success");
        let snapshot = store.publication_snapshot().await.expect("snapshot");
        assert_eq!(snapshot.raw_configs, vec![raw.to_owned()]);
    }

    #[tokio::test]
    async fn errors_use_a_consistent_json_contract() {
        let (_temp, app) = test_router().await;
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/best")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"client":""}"#))
                    .unwrap(),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error body");
        assert_eq!(body["error"]["code"], "INVALID_CLIENT");
        assert!(body["error"]["message"].is_string());
        assert!(body["error"].get("details").is_some());
    }

    #[tokio::test]
    async fn health_stays_responsive_while_sqlite_writer_is_locked() {
        let (_temp, store, app) = test_router_with_store().await;
        let mut connection = store.pool().acquire().await.expect("connection");
        sqlx::query("BEGIN EXCLUSIVE")
            .execute(&mut *connection)
            .await
            .expect("exclusive writer lock");
        let response = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            app.oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            ),
        )
        .await
        .expect("health must not wait for SQLite writer")
        .expect("health response");
        assert_eq!(response.status(), StatusCode::OK);
        sqlx::query("ROLLBACK")
            .execute(&mut *connection)
            .await
            .expect("release writer lock");
    }
}
