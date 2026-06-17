mod api;
mod auth;
mod config;
mod snapshots;

use std::sync::Arc;

use anyhow::{Context, Result};
use axum::{
    routing::{get, post},
    Router,
};
use config::DashboardConfig;
use tower_http::{services::ServeDir, trace::TraceLayer};
use tracing::info;

pub struct AppState {
    config: DashboardConfig,
    auth: auth::AuthStore,
    snapshots: snapshots::SnapshotStore,
}

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::args().nth(1).as_deref() == Some("--hash-password") {
        let password = std::env::var("COPYBOT_OPS_DASHBOARD_PASSWORD")
            .context("set COPYBOT_OPS_DASHBOARD_PASSWORD to hash")?;
        println!("{}", auth::hash_password(&password)?);
        return Ok(());
    }

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = DashboardConfig::from_env()?;
    let state = Arc::new(AppState {
        auth: auth::AuthStore::open(&config.auth_db_path)?,
        snapshots: snapshots::SnapshotStore::new(
            config.report_dir.clone(),
            config.snapshot_max_age,
        ),
        config: config.clone(),
    });
    let static_dir = config.static_dir.clone();
    let app = Router::new()
        .route("/login", post(api::login))
        .route("/logout", post(api::logout))
        .route("/api/session", get(api::session))
        .route("/api/overview", get(api::overview))
        .route("/api/storage", get(api::storage))
        .route("/api/discovery", get(api::discovery))
        .route("/api/execution", get(api::execution))
        .route("/api/strategy", get(api::strategy))
        .route("/api/alerts", get(api::alerts))
        .route("/api/reports/latest", get(api::reports))
        .fallback_service(ServeDir::new(static_dir))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    info!(addr = %config.bind_addr, "dashboard listening");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;
    Ok(())
}
