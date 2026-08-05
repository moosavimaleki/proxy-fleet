use std::{env, path::PathBuf, sync::Arc};

use anyhow::Context;
use proxy_fleet::{api, app::AppState, config::AppConfig, storage::Store};
use tokio_util::sync::CancellationToken;
use tower_http::{compression::CompressionLayer, cors::CorsLayer, trace::TraceLayer};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,tower_http=warn".into()),
        )
        .json()
        .init();

    let config_path = parse_config_path()?;
    let config = Arc::new(AppConfig::load(&config_path).context("loading configuration")?);
    let store = Store::connect(&config.database.path)
        .await
        .context("opening SQLite database")?;
    if let Some(backup) = store.backup_before_migrate().await? {
        info!(backup = %backup.display(), "created pre-migration database snapshot");
    }
    store.migrate().await.context("migrating SQLite database")?;
    store
        .set_service_state(
            "binary_version",
            serde_json::json!({"version": proxy_fleet::SERVICE_VERSION}),
        )
        .await
        .context("recording binary version")?;
    store
        .set_service_state(
            "schema_version",
            serde_json::json!({"version": "rust-evidence-v1"}),
        )
        .await
        .context("recording schema version")?;
    let stale_ports = store.clear_stale_runtime_ports().await?;
    if stale_ports > 0 {
        info!(
            stale_ports,
            "cleared persisted runtime ports before reconciliation"
        );
    }

    let shutdown = CancellationToken::new();
    let state = AppState::new(config.clone(), store, shutdown.clone());
    let (restored_runtimes, failed_runtimes) = state.reconcile_active_runtimes().await;
    info!(
        restored_runtimes,
        failed_runtimes, "reconciled ACTIVE Xray runtimes"
    );
    state.spawn_background_services();

    let app = api::router(state.clone())
        .layer(CompressionLayer::new())
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http());
    let address = format!("{}:{}", config.api.host, config.api.port);
    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .with_context(|| format!("binding API on {address}"))?;

    info!(service = %config.service.name, version = proxy_fleet::SERVICE_VERSION, address = %address, "proxy fleet started");
    let result = axum::serve(listener, app)
        .with_graceful_shutdown(wait_for_shutdown(shutdown))
        .await;
    state.shutdown_runtimes().await;
    result.context("HTTP server failed")
}

fn parse_config_path() -> anyhow::Result<PathBuf> {
    let mut args = env::args().skip(1);
    let mut path = PathBuf::from("/app/config/config.yml");
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                let value = args.next().context("--config requires a path")?;
                path = PathBuf::from(value);
            }
            "--help" | "-h" => {
                println!("proxy-fleet --config /path/to/config.yml");
                std::process::exit(0);
            }
            other => anyhow::bail!("unknown argument: {other}"),
        }
    }
    Ok(path)
}

async fn wait_for_shutdown(shutdown: CancellationToken) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    result = tokio::signal::ctrl_c() => {
                        if let Err(error) = result { warn!(%error, "could not listen for Ctrl-C"); }
                    }
                    _ = terminate.recv() => info!("SIGTERM received"),
                }
            }
            Err(error) => {
                warn!(%error, "could not listen for SIGTERM; falling back to Ctrl-C");
                if let Err(error) = tokio::signal::ctrl_c().await {
                    warn!(%error, "could not listen for Ctrl-C");
                }
            }
        }
    }
    #[cfg(not(unix))]
    if let Err(error) = tokio::signal::ctrl_c().await {
        warn!(%error, "could not listen for Ctrl-C");
    }
    shutdown.cancel();
    info!("shutdown requested");
}
