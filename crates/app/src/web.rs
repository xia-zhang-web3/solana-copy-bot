use anyhow::{Context, Result};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{OriginalUri, Query, State};
use axum::http::header::AUTHORIZATION;
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use copybot_config::{AppConfig, WebConfig};
use copybot_ingestion::IngestionRuntimeSnapshot;
use copybot_shadow::{FollowSnapshot, ShadowSnapshot};
use copybot_storage::{
    CopySignalViewRow, DashboardKpis, LatestWalletMetricRow, ShadowClosedTradeViewRow, SqliteStore,
    UiEventRow,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;
use tokio::time::{self, MissedTickBehavior};
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

pub const UI_EVENTS_RETENTION_HOURS: i64 = 48;
const WS_HEARTBEAT_SECONDS: u64 = 10;

#[derive(Debug, Clone, Serialize)]
pub struct LiveEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub ts: DateTime<Utc>,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Default, PartialEq)]
pub struct RuntimeRiskSnapshot {
    pub killswitch_enabled: bool,
    pub hard_stop_reason: Option<String>,
    pub exposure_hard_blocked: bool,
    pub exposure_hard_detail: Option<String>,
    pub pause_until: Option<DateTime<Utc>>,
    pub pause_reason: Option<String>,
    pub universe_breach_streak: u64,
    pub universe_blocked: bool,
    pub infra_block_reason: Option<String>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct RuntimeIngestionSnapshot {
    pub ts_utc: Option<DateTime<Utc>>,
    pub ws_notifications_enqueued: u64,
    pub ws_notifications_replaced_oldest: u64,
    pub rpc_429: u64,
    pub rpc_5xx: u64,
    pub ingestion_lag_ms_p95: u64,
    pub last_swap_ts: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct RuntimeDiscoverySnapshot {
    pub last_cycle_ts: Option<DateTime<Utc>>,
    pub eligible_wallets: usize,
    pub active_follow_wallets: usize,
    pub top_wallets: Vec<String>,
    pub follow_wallets: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct RuntimeSchedulerSnapshot {
    pub pending_tasks: usize,
    pub pending_capacity: usize,
    pub inflight_keys: usize,
    pub worker_count: usize,
    pub held_sells: usize,
    pub queue_backpressure_active: bool,
    pub scheduler_needs_reset: bool,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct RuntimeShadowSnapshot {
    pub closed_trades_24h: u64,
    pub realized_pnl_sol_24h: f64,
    pub open_lots: u64,
    pub last_snapshot_ts: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct RuntimeSnapshotBundle {
    pub risk: RuntimeRiskSnapshot,
    pub ingestion: RuntimeIngestionSnapshot,
    pub discovery: RuntimeDiscoverySnapshot,
    pub scheduler: RuntimeSchedulerSnapshot,
    pub shadow: RuntimeShadowSnapshot,
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthAlert {
    pub code: String,
    pub severity: String,
    pub message: String,
}

#[derive(Clone)]
pub struct WebRuntimeHandle {
    inner: Arc<WebRuntimeInner>,
}

struct WebRuntimeInner {
    sqlite_path: String,
    auth_token: String,
    config_redacted: Value,
    started_at: DateTime<Utc>,
    discovery_stale_threshold_seconds: u64,
    ingestion_stale_threshold_seconds: u64,
    shadow_stale_threshold_seconds: u64,
    infra_lag_threshold_ms: u64,
    risk: RwLock<RuntimeRiskSnapshot>,
    ingestion: RwLock<RuntimeIngestionSnapshot>,
    discovery: RwLock<RuntimeDiscoverySnapshot>,
    scheduler: RwLock<RuntimeSchedulerSnapshot>,
    shadow: RwLock<RuntimeShadowSnapshot>,
    live_tx: broadcast::Sender<LiveEvent>,
}

impl WebRuntimeHandle {
    pub fn new(sqlite_path: String, config: &AppConfig) -> Self {
        let mut config_redacted = serde_json::to_value(config).unwrap_or_else(|_| json!({}));
        redact_sensitive_values(&mut config_redacted);

        let discovery_stale_threshold_seconds = config.discovery.refresh_seconds.max(10) * 2 + 15;
        let ingestion_stale_threshold_seconds = config.system.heartbeat_seconds.max(15) * 6;
        let shadow_stale_threshold_seconds = config.shadow.refresh_seconds.max(10) * 2 + 15;
        let infra_lag_threshold_ms = config.risk.shadow_infra_lag_p95_threshold_ms.max(1_000);

        let (live_tx, _) = broadcast::channel(4096);
        Self {
            inner: Arc::new(WebRuntimeInner {
                sqlite_path,
                auth_token: config.web.auth_token.clone(),
                config_redacted,
                started_at: Utc::now(),
                discovery_stale_threshold_seconds,
                ingestion_stale_threshold_seconds,
                shadow_stale_threshold_seconds,
                infra_lag_threshold_ms,
                risk: RwLock::new(RuntimeRiskSnapshot {
                    killswitch_enabled: config.risk.shadow_killswitch_enabled,
                    ..RuntimeRiskSnapshot::default()
                }),
                ingestion: RwLock::new(RuntimeIngestionSnapshot::default()),
                discovery: RwLock::new(RuntimeDiscoverySnapshot::default()),
                scheduler: RwLock::new(RuntimeSchedulerSnapshot::default()),
                shadow: RwLock::new(RuntimeShadowSnapshot::default()),
                live_tx,
            }),
        }
    }

    pub fn auth_token_is_configured(&self) -> bool {
        let token = self.inner.auth_token.trim();
        !token.is_empty() && !token.contains("REPLACE_ME")
    }

    pub fn sqlite_path(&self) -> String {
        self.inner.sqlite_path.clone()
    }

    pub fn set_risk_snapshot(&self, snapshot: RuntimeRiskSnapshot) {
        if let Ok(mut guard) = self.inner.risk.write() {
            *guard = snapshot;
        }
    }

    pub fn set_ingestion_snapshot(
        &self,
        snapshot: Option<IngestionRuntimeSnapshot>,
        last_swap_ts: Option<DateTime<Utc>>,
    ) {
        if let Ok(mut guard) = self.inner.ingestion.write() {
            if let Some(snapshot) = snapshot {
                guard.ts_utc = Some(snapshot.ts_utc);
                guard.ws_notifications_enqueued = snapshot.ws_notifications_enqueued;
                guard.ws_notifications_replaced_oldest = snapshot.ws_notifications_replaced_oldest;
                guard.rpc_429 = snapshot.rpc_429;
                guard.rpc_5xx = snapshot.rpc_5xx;
                guard.ingestion_lag_ms_p95 = snapshot.ingestion_lag_ms_p95;
            }
            if let Some(ts) = last_swap_ts {
                guard.last_swap_ts = Some(ts);
            }
            guard.updated_at = Some(Utc::now());
        }
    }

    pub fn set_discovery_snapshot(
        &self,
        cycle_ts: DateTime<Utc>,
        eligible_wallets: usize,
        active_follow_wallets: usize,
        top_wallets: Vec<String>,
    ) {
        if let Ok(mut guard) = self.inner.discovery.write() {
            guard.last_cycle_ts = Some(cycle_ts);
            guard.eligible_wallets = eligible_wallets;
            guard.active_follow_wallets = active_follow_wallets;
            guard.top_wallets = top_wallets;
        }
    }

    pub fn set_follow_snapshot(&self, follow_snapshot: &FollowSnapshot) {
        if let Ok(mut guard) = self.inner.discovery.write() {
            let mut wallets: Vec<String> = follow_snapshot.active.iter().cloned().collect();
            wallets.sort();
            guard.active_follow_wallets = wallets.len();
            guard.follow_wallets = wallets;
        }
    }

    pub fn set_scheduler_snapshot(
        &self,
        pending_tasks: usize,
        pending_capacity: usize,
        inflight_keys: usize,
        worker_count: usize,
        held_sells: usize,
        queue_backpressure_active: bool,
        scheduler_needs_reset: bool,
    ) {
        if let Ok(mut guard) = self.inner.scheduler.write() {
            guard.pending_tasks = pending_tasks;
            guard.pending_capacity = pending_capacity;
            guard.inflight_keys = inflight_keys;
            guard.worker_count = worker_count;
            guard.held_sells = held_sells;
            guard.queue_backpressure_active = queue_backpressure_active;
            guard.scheduler_needs_reset = scheduler_needs_reset;
            guard.updated_at = Some(Utc::now());
        }
    }

    pub fn set_shadow_snapshot(&self, snapshot: ShadowSnapshot, ts: DateTime<Utc>) {
        if let Ok(mut guard) = self.inner.shadow.write() {
            guard.closed_trades_24h = snapshot.closed_trades_24h;
            guard.realized_pnl_sol_24h = snapshot.realized_pnl_sol_24h;
            guard.open_lots = snapshot.open_lots;
            guard.last_snapshot_ts = Some(ts);
        }
    }

    pub fn runtime_bundle(&self) -> RuntimeSnapshotBundle {
        RuntimeSnapshotBundle {
            risk: self
                .inner
                .risk
                .read()
                .map(|value| value.clone())
                .unwrap_or_default(),
            ingestion: self
                .inner
                .ingestion
                .read()
                .map(|value| value.clone())
                .unwrap_or_default(),
            discovery: self
                .inner
                .discovery
                .read()
                .map(|value| value.clone())
                .unwrap_or_default(),
            scheduler: self
                .inner
                .scheduler
                .read()
                .map(|value| value.clone())
                .unwrap_or_default(),
            shadow: self
                .inner
                .shadow
                .read()
                .map(|value| value.clone())
                .unwrap_or_default(),
        }
    }

    pub fn compute_health_alerts(&self, now: DateTime<Utc>) -> Vec<HealthAlert> {
        let bundle = self.runtime_bundle();
        let mut alerts = Vec::new();

        if bundle.scheduler.queue_backpressure_active {
            alerts.push(HealthAlert {
                code: "shadow_queue_saturated".to_string(),
                severity: "warn".to_string(),
                message: format!(
                    "shadow queue saturated: pending={} capacity={}",
                    bundle.scheduler.pending_tasks, bundle.scheduler.pending_capacity
                ),
            });
        }

        if let Some(reason) = bundle.risk.hard_stop_reason.as_ref() {
            alerts.push(HealthAlert {
                code: "risk_hard_stop".to_string(),
                severity: "error".to_string(),
                message: reason.clone(),
            });
        }

        if let Some(reason) = bundle.risk.pause_reason.as_ref() {
            alerts.push(HealthAlert {
                code: "risk_pause".to_string(),
                severity: "warn".to_string(),
                message: reason.clone(),
            });
        }

        if let Some(reason) = bundle.risk.infra_block_reason.as_ref() {
            alerts.push(HealthAlert {
                code: "risk_infra".to_string(),
                severity: "warn".to_string(),
                message: reason.clone(),
            });
        }

        if let Some(last_swap_ts) = bundle.ingestion.last_swap_ts {
            let stale_for = (now - last_swap_ts).num_seconds().max(0) as u64;
            if stale_for > self.inner.ingestion_stale_threshold_seconds {
                alerts.push(HealthAlert {
                    code: "ingestion_stale".to_string(),
                    severity: "warn".to_string(),
                    message: format!(
                        "no swaps observed for {}s (threshold {}s)",
                        stale_for, self.inner.ingestion_stale_threshold_seconds
                    ),
                });
            }
        } else {
            alerts.push(HealthAlert {
                code: "ingestion_no_data".to_string(),
                severity: "warn".to_string(),
                message: "no ingested swaps yet".to_string(),
            });
        }

        if let Some(last_cycle_ts) = bundle.discovery.last_cycle_ts {
            let stale_for = (now - last_cycle_ts).num_seconds().max(0) as u64;
            if stale_for > self.inner.discovery_stale_threshold_seconds {
                alerts.push(HealthAlert {
                    code: "discovery_stale".to_string(),
                    severity: "warn".to_string(),
                    message: format!(
                        "discovery cycle is stale for {}s (threshold {}s)",
                        stale_for, self.inner.discovery_stale_threshold_seconds
                    ),
                });
            }
        }

        if let Some(last_shadow_ts) = bundle.shadow.last_snapshot_ts {
            let stale_for = (now - last_shadow_ts).num_seconds().max(0) as u64;
            if stale_for > self.inner.shadow_stale_threshold_seconds {
                alerts.push(HealthAlert {
                    code: "shadow_snapshot_stale".to_string(),
                    severity: "warn".to_string(),
                    message: format!(
                        "shadow snapshot stale for {}s (threshold {}s)",
                        stale_for, self.inner.shadow_stale_threshold_seconds
                    ),
                });
            }
        }

        if bundle.ingestion.ingestion_lag_ms_p95 > self.inner.infra_lag_threshold_ms {
            alerts.push(HealthAlert {
                code: "ingestion_lag_high".to_string(),
                severity: "warn".to_string(),
                message: format!(
                    "ingestion lag p95={}ms > threshold={}ms",
                    bundle.ingestion.ingestion_lag_ms_p95, self.inner.infra_lag_threshold_ms
                ),
            });
        }

        alerts
    }

    pub fn publish_event(&self, event_type: impl Into<String>, payload: Value) {
        let event = LiveEvent {
            event_type: event_type.into(),
            ts: Utc::now(),
            payload,
        };
        let _ = self.inner.live_tx.send(event);
    }

    pub async fn run_server(self, web_config: WebConfig) -> Result<()> {
        let app = build_router(self.clone());
        let bind = format!("{}:{}", web_config.host, web_config.port);
        let listener = tokio::net::TcpListener::bind(&bind)
            .await
            .with_context(|| format!("failed to bind web server on {}", bind))?;
        info!(bind = %bind, "web server started");
        axum::serve(listener, app)
            .await
            .context("axum web server failed")?;
        Ok(())
    }
}

fn build_router(state: WebRuntimeHandle) -> Router {
    let cors = CorsLayer::new()
        .allow_methods(Any)
        .allow_headers(Any)
        .allow_origin(Any);
    Router::new()
        .route("/", get(index_html))
        .route("/healthz", get(healthz))
        .route("/api/dashboard", get(api_dashboard))
        .route("/api/lots", get(api_lots))
        .route("/api/trades", get(api_trades))
        .route("/api/signals", get(api_signals))
        .route("/api/risk", get(api_risk))
        .route("/api/ingestion", get(api_ingestion))
        .route("/api/discovery", get(api_discovery))
        .route("/api/config", get(api_config))
        .route("/ws/live", get(ws_live))
        .layer(cors)
        .with_state(state)
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: message.into(),
        }
    }

    fn service_unavailable(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({
                "error": self.message,
                "status": self.status.as_u16(),
            })),
        )
            .into_response()
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
struct PaginationQuery {
    limit: Option<u32>,
    offset: Option<u32>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct WsAuthQuery {
    token: Option<String>,
}

#[derive(Debug, Serialize)]
struct DashboardResponse {
    ts: DateTime<Utc>,
    started_at: DateTime<Utc>,
    kpis: DashboardKpis,
    runtime: RuntimeSnapshotBundle,
    health_alerts: Vec<HealthAlert>,
}

#[derive(Debug, Serialize)]
struct LotView {
    id: i64,
    wallet_id: String,
    token: String,
    qty: f64,
    cost_sol: f64,
    opened_ts: DateTime<Utc>,
    age_seconds: i64,
    entry_price_sol_per_token: f64,
    mark_price_sol_per_token: Option<f64>,
    unrealized_pnl_sol: Option<f64>,
    unrealized_return_pct: Option<f64>,
}

#[derive(Debug, Serialize)]
struct LotsResponse {
    ts: DateTime<Utc>,
    limit: u32,
    offset: u32,
    lots: Vec<LotView>,
}

#[derive(Debug, Serialize)]
struct ClosedTradeView {
    id: i64,
    signal_id: String,
    wallet_id: String,
    token: String,
    qty: f64,
    entry_cost_sol: f64,
    exit_value_sol: f64,
    pnl_sol: f64,
    return_pct: f64,
    opened_ts: DateTime<Utc>,
    closed_ts: DateTime<Utc>,
    hold_seconds: i64,
    entry_price_sol_per_token: f64,
    exit_price_sol_per_token: f64,
}

#[derive(Debug, Serialize)]
struct TradesResponse {
    ts: DateTime<Utc>,
    limit: u32,
    offset: u32,
    trades: Vec<ClosedTradeView>,
}

#[derive(Debug, Serialize)]
struct SignalTimelineItem {
    kind: String,
    source: String,
    ts: DateTime<Utc>,
    signal_id: Option<String>,
    wallet_id: Option<String>,
    token: Option<String>,
    side: Option<String>,
    status: Option<String>,
    stage: Option<String>,
    reason: Option<String>,
    signature: Option<String>,
    notional_sol: Option<f64>,
    payload_json: Option<Value>,
}

#[derive(Debug, Serialize)]
struct SignalsResponse {
    ts: DateTime<Utc>,
    limit: u32,
    items: Vec<SignalTimelineItem>,
}

#[derive(Debug, Serialize)]
struct RiskResponse {
    ts: DateTime<Utc>,
    runtime: RuntimeRiskSnapshot,
    recent_events: Vec<RiskEventView>,
}

#[derive(Debug, Serialize)]
struct RiskEventView {
    event_id: String,
    event_type: String,
    severity: String,
    ts: DateTime<Utc>,
    details_json: Option<String>,
}

#[derive(Debug, Serialize)]
struct IngestionResponse {
    ts: DateTime<Utc>,
    runtime: RuntimeIngestionSnapshot,
    scheduler: RuntimeSchedulerSnapshot,
    health_alerts: Vec<HealthAlert>,
    recent_events: Vec<UiEventView>,
}

#[derive(Debug, Serialize)]
struct UiEventView {
    id: i64,
    event_type: String,
    stage: Option<String>,
    reason: Option<String>,
    wallet_id: Option<String>,
    token: Option<String>,
    signature: Option<String>,
    ts: DateTime<Utc>,
    payload_json: Option<Value>,
}

#[derive(Debug, Serialize)]
struct DiscoveryResponse {
    ts: DateTime<Utc>,
    runtime: RuntimeDiscoverySnapshot,
    top_metrics: Vec<TopWalletMetricView>,
}

#[derive(Debug, Serialize)]
struct TopWalletMetricView {
    wallet_id: String,
    window_start: DateTime<Utc>,
    score: f64,
    trades: u32,
    pnl: f64,
    win_rate: f64,
    buy_total: u32,
    tradable_ratio: f64,
    rug_ratio: f64,
}

async fn healthz() -> impl IntoResponse {
    Json(json!({"status": "ok", "ts": Utc::now()}))
}

async fn index_html() -> impl IntoResponse {
    Html(include_str!("web_ui.html"))
}

async fn api_dashboard(
    State(state): State<WebRuntimeHandle>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Result<Json<DashboardResponse>, ApiError> {
    ensure_authorized_request(&state, &headers, &uri)?;
    let now = Utc::now();
    let sqlite_path = state.sqlite_path();
    let kpis = read_only_db(sqlite_path, move |store| store.dashboard_kpis(now)).await?;
    let runtime = state.runtime_bundle();
    let health_alerts = state.compute_health_alerts(now);
    Ok(Json(DashboardResponse {
        ts: now,
        started_at: state.inner.started_at,
        kpis,
        runtime,
        health_alerts,
    }))
}

async fn api_lots(
    State(state): State<WebRuntimeHandle>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Query(query): Query<PaginationQuery>,
) -> Result<Json<LotsResponse>, ApiError> {
    ensure_authorized_request(&state, &headers, &uri)?;
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let offset = query.offset.unwrap_or(0);
    let now = Utc::now();
    let sqlite_path = state.sqlite_path();

    let lots = read_only_db(sqlite_path, move |store| {
        let raw_lots = store.list_open_shadow_lots(limit, offset)?;
        let unique_tokens: BTreeSet<String> =
            raw_lots.iter().map(|lot| lot.token.clone()).collect();
        let mut prices: HashMap<String, Option<f64>> = HashMap::new();
        for token in unique_tokens {
            let price = store.latest_token_sol_price(&token, now)?;
            prices.insert(token, price);
        }

        let lots = raw_lots
            .into_iter()
            .map(|lot| {
                let entry_price = if lot.qty > 1e-12 {
                    lot.cost_sol / lot.qty
                } else {
                    0.0
                };
                let mark_price = prices.get(&lot.token).copied().unwrap_or(None);
                let unrealized = mark_price.map(|price| lot.qty * price - lot.cost_sol);
                let unrealized_return_pct = unrealized.map(|value| {
                    if lot.cost_sol.abs() > 1e-12 {
                        value / lot.cost_sol
                    } else {
                        0.0
                    }
                });
                LotView {
                    id: lot.id,
                    wallet_id: lot.wallet_id,
                    token: lot.token,
                    qty: lot.qty,
                    cost_sol: lot.cost_sol,
                    opened_ts: lot.opened_ts,
                    age_seconds: (now - lot.opened_ts).num_seconds().max(0),
                    entry_price_sol_per_token: entry_price,
                    mark_price_sol_per_token: mark_price,
                    unrealized_pnl_sol: unrealized,
                    unrealized_return_pct,
                }
            })
            .collect::<Vec<_>>();
        Ok(lots)
    })
    .await?;

    Ok(Json(LotsResponse {
        ts: now,
        limit,
        offset,
        lots,
    }))
}

async fn api_trades(
    State(state): State<WebRuntimeHandle>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Query(query): Query<PaginationQuery>,
) -> Result<Json<TradesResponse>, ApiError> {
    ensure_authorized_request(&state, &headers, &uri)?;
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let offset = query.offset.unwrap_or(0);
    let now = Utc::now();
    let sqlite_path = state.sqlite_path();

    let trades = read_only_db(sqlite_path, move |store| {
        let raw = store.list_recent_shadow_closed_trades(limit, offset)?;
        Ok(raw
            .into_iter()
            .map(to_closed_trade_view)
            .collect::<Vec<_>>())
    })
    .await?;

    Ok(Json(TradesResponse {
        ts: now,
        limit,
        offset,
        trades,
    }))
}

async fn api_signals(
    State(state): State<WebRuntimeHandle>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Query(query): Query<PaginationQuery>,
) -> Result<Json<SignalsResponse>, ApiError> {
    ensure_authorized_request(&state, &headers, &uri)?;
    let limit = query.limit.unwrap_or(200).clamp(1, 1_000);
    let now = Utc::now();
    let sqlite_path = state.sqlite_path();

    let mut items = read_only_db(sqlite_path, move |store| {
        let recorded = store.list_recent_copy_signals(limit, 0)?;
        let events = store.list_recent_ui_events(limit.saturating_mul(4))?;
        let mut timeline = Vec::new();
        timeline.extend(recorded.into_iter().map(to_recorded_signal_timeline_item));
        timeline.extend(events.into_iter().filter_map(to_ui_signal_timeline_item));
        timeline.sort_by(|a, b| b.ts.cmp(&a.ts));
        timeline.truncate(limit as usize);
        Ok(timeline)
    })
    .await?;

    items.sort_by(|a, b| b.ts.cmp(&a.ts));
    Ok(Json(SignalsResponse {
        ts: now,
        limit,
        items,
    }))
}

async fn api_risk(
    State(state): State<WebRuntimeHandle>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Result<Json<RiskResponse>, ApiError> {
    ensure_authorized_request(&state, &headers, &uri)?;
    let now = Utc::now();
    let runtime = state.runtime_bundle().risk;
    let sqlite_path = state.sqlite_path();
    let events = read_only_db(sqlite_path, move |store| store.list_recent_risk_events(200)).await?;
    let recent_events = events
        .into_iter()
        .map(|event| RiskEventView {
            event_id: event.event_id,
            event_type: event.event_type,
            severity: event.severity,
            ts: event.ts,
            details_json: event.details_json,
        })
        .collect::<Vec<_>>();
    Ok(Json(RiskResponse {
        ts: now,
        runtime,
        recent_events,
    }))
}

async fn api_ingestion(
    State(state): State<WebRuntimeHandle>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Result<Json<IngestionResponse>, ApiError> {
    ensure_authorized_request(&state, &headers, &uri)?;
    let now = Utc::now();
    let runtime = state.runtime_bundle();
    let sqlite_path = state.sqlite_path();
    let recent_events = read_only_db(sqlite_path, move |store| {
        let events = store.list_recent_ui_events(300)?;
        Ok(events
            .into_iter()
            .filter(|event| {
                matches!(
                    event.event_type.as_str(),
                    "ingestion_event" | "queue_event" | "shadow_error"
                )
            })
            .map(to_ui_event_view)
            .collect::<Vec<_>>())
    })
    .await?;
    let health_alerts = state.compute_health_alerts(now);
    Ok(Json(IngestionResponse {
        ts: now,
        runtime: runtime.ingestion,
        scheduler: runtime.scheduler,
        health_alerts,
        recent_events,
    }))
}

async fn api_discovery(
    State(state): State<WebRuntimeHandle>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Result<Json<DiscoveryResponse>, ApiError> {
    ensure_authorized_request(&state, &headers, &uri)?;
    let now = Utc::now();
    let runtime = state.runtime_bundle().discovery;
    let sqlite_path = state.sqlite_path();
    let top_metrics = read_only_db(sqlite_path, move |store| {
        store.list_latest_wallet_metrics(50)
    })
    .await?;
    let top_metrics = top_metrics
        .into_iter()
        .map(to_top_wallet_metric_view)
        .collect::<Vec<_>>();
    Ok(Json(DiscoveryResponse {
        ts: now,
        runtime,
        top_metrics,
    }))
}

async fn api_config(
    State(state): State<WebRuntimeHandle>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    ensure_authorized_request(&state, &headers, &uri)?;
    Ok(Json(state.inner.config_redacted.clone()))
}

async fn ws_live(
    ws: WebSocketUpgrade,
    State(state): State<WebRuntimeHandle>,
    headers: HeaderMap,
    Query(query): Query<WsAuthQuery>,
) -> Result<impl IntoResponse, ApiError> {
    ensure_authorized(&state, &headers, query.token.as_deref())?;
    Ok(ws.on_upgrade(move |socket| websocket_loop(socket, state)))
}

async fn websocket_loop(mut socket: WebSocket, state: WebRuntimeHandle) {
    let mut rx = state.inner.live_tx.subscribe();
    let mut heartbeat = time::interval(std::time::Duration::from_secs(WS_HEARTBEAT_SECONDS));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            event = rx.recv() => {
                match event {
                    Ok(event) => {
                        if send_ws_event(&mut socket, &event).await.is_err() {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        let lagged = LiveEvent {
                            event_type: "lagged".to_string(),
                            ts: Utc::now(),
                            payload: json!({
                                "dropped_messages": skipped,
                                "action": "resync_required"
                            }),
                        };
                        if send_ws_event(&mut socket, &lagged).await.is_err() {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
            inbound = socket.recv() => {
                match inbound {
                    Some(Ok(Message::Close(_))) | None => return,
                    Some(Ok(Message::Ping(payload))) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            return;
                        }
                    }
                    Some(Ok(_)) => {}
                    Some(Err(_)) => return,
                }
            }
            _ = heartbeat.tick() => {
                let heartbeat_event = LiveEvent {
                    event_type: "heartbeat".to_string(),
                    ts: Utc::now(),
                    payload: json!({
                        "status": "alive",
                        "alerts": state.compute_health_alerts(Utc::now()).len(),
                    }),
                };
                if send_ws_event(&mut socket, &heartbeat_event).await.is_err() {
                    return;
                }
            }
        }
    }
}

async fn send_ws_event(socket: &mut WebSocket, event: &LiveEvent) -> Result<()> {
    let payload = serde_json::to_string(event).context("failed to serialize ws live event")?;
    socket
        .send(Message::Text(payload.into()))
        .await
        .context("failed to send ws live event")?;
    Ok(())
}

fn ensure_authorized(
    state: &WebRuntimeHandle,
    headers: &HeaderMap,
    query_token: Option<&str>,
) -> Result<(), ApiError> {
    let expected = state.inner.auth_token.trim();
    if expected.is_empty() || expected.contains("REPLACE_ME") {
        return Err(ApiError::service_unavailable(
            "web auth token is not configured",
        ));
    }

    if let Some(header_value) = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
    {
        if header_value == expected {
            return Ok(());
        }
    }

    if query_token.is_some_and(|value| value == expected) {
        return Ok(());
    }

    Err(ApiError::unauthorized("invalid bearer token"))
}

fn ensure_authorized_request(
    state: &WebRuntimeHandle,
    headers: &HeaderMap,
    uri: &Uri,
) -> Result<(), ApiError> {
    let query_token = query_token_from_uri(uri);
    ensure_authorized(state, headers, query_token.as_deref())
}

fn query_token_from_uri(uri: &Uri) -> Option<String> {
    uri.query().and_then(|query| {
        query.split('&').find_map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next()?;
            if key != "token" {
                return None;
            }
            Some(parts.next().unwrap_or_default().to_string())
        })
    })
}

async fn read_only_db<T, F>(sqlite_path: String, action: F) -> Result<T, ApiError>
where
    T: Send + 'static,
    F: FnOnce(SqliteStore) -> Result<T> + Send + 'static,
{
    let join = tokio::task::spawn_blocking(move || {
        let store = SqliteStore::open_read_only(Path::new(&sqlite_path))
            .with_context(|| format!("failed opening read-only sqlite: {}", sqlite_path))?;
        action(store)
    })
    .await
    .map_err(|error| ApiError::internal(format!("sqlite read task failed: {error}")))?;

    join.map_err(|error| ApiError::internal(error.to_string()))
}

fn to_closed_trade_view(trade: ShadowClosedTradeViewRow) -> ClosedTradeView {
    let hold_seconds = (trade.closed_ts - trade.opened_ts).num_seconds().max(0);
    let entry_price = if trade.qty > 1e-12 {
        trade.entry_cost_sol / trade.qty
    } else {
        0.0
    };
    let exit_price = if trade.qty > 1e-12 {
        trade.exit_value_sol / trade.qty
    } else {
        0.0
    };
    let return_pct = if trade.entry_cost_sol.abs() > 1e-12 {
        trade.pnl_sol / trade.entry_cost_sol
    } else {
        0.0
    };
    ClosedTradeView {
        id: trade.id,
        signal_id: trade.signal_id,
        wallet_id: trade.wallet_id,
        token: trade.token,
        qty: trade.qty,
        entry_cost_sol: trade.entry_cost_sol,
        exit_value_sol: trade.exit_value_sol,
        pnl_sol: trade.pnl_sol,
        return_pct,
        opened_ts: trade.opened_ts,
        closed_ts: trade.closed_ts,
        hold_seconds,
        entry_price_sol_per_token: entry_price,
        exit_price_sol_per_token: exit_price,
    }
}

fn to_recorded_signal_timeline_item(signal: CopySignalViewRow) -> SignalTimelineItem {
    SignalTimelineItem {
        kind: "recorded".to_string(),
        source: "copy_signals".to_string(),
        ts: signal.ts,
        signal_id: Some(signal.signal_id),
        wallet_id: Some(signal.wallet_id),
        token: Some(signal.token),
        side: Some(signal.side),
        status: Some(signal.status),
        stage: Some("recorded".to_string()),
        reason: None,
        signature: None,
        notional_sol: Some(signal.notional_sol),
        payload_json: None,
    }
}

fn to_ui_signal_timeline_item(event: UiEventRow) -> Option<SignalTimelineItem> {
    if event.event_type != "signal_dropped" && event.event_type != "signal_recorded" {
        return None;
    }
    let payload_json = event
        .payload_json
        .as_ref()
        .and_then(|value| serde_json::from_str::<Value>(value).ok());
    let signal_id = payload_json
        .as_ref()
        .and_then(|payload| payload.get("signal_id"))
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let side = payload_json
        .as_ref()
        .and_then(|payload| payload.get("side"))
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let status = payload_json
        .as_ref()
        .and_then(|payload| payload.get("status"))
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let notional_sol = payload_json
        .as_ref()
        .and_then(|payload| payload.get("notional_sol"))
        .and_then(Value::as_f64);
    Some(SignalTimelineItem {
        kind: if event.event_type == "signal_dropped" {
            "dropped".to_string()
        } else {
            "recorded".to_string()
        },
        source: "ui_events".to_string(),
        ts: event.ts,
        signal_id,
        wallet_id: event.wallet_id,
        token: event.token,
        side,
        status,
        stage: event.stage,
        reason: event.reason,
        signature: event.signature,
        notional_sol,
        payload_json,
    })
}

fn to_ui_event_view(event: UiEventRow) -> UiEventView {
    UiEventView {
        id: event.id,
        event_type: event.event_type,
        stage: event.stage,
        reason: event.reason,
        wallet_id: event.wallet_id,
        token: event.token,
        signature: event.signature,
        ts: event.ts,
        payload_json: event
            .payload_json
            .as_ref()
            .and_then(|value| serde_json::from_str::<Value>(value).ok()),
    }
}

fn to_top_wallet_metric_view(row: LatestWalletMetricRow) -> TopWalletMetricView {
    TopWalletMetricView {
        wallet_id: row.wallet_id,
        window_start: row.window_start,
        score: row.score,
        trades: row.trades,
        pnl: row.pnl,
        win_rate: row.win_rate,
        buy_total: row.buy_total,
        tradable_ratio: row.tradable_ratio,
        rug_ratio: row.rug_ratio,
    }
}

fn redact_sensitive_values(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                let key_lc = key.to_ascii_lowercase();
                if child.is_string()
                    && (key_lc.contains("token")
                        || key_lc.contains("key")
                        || key_lc.contains("auth")
                        || key_lc.contains("secret")
                        || key_lc.contains("url"))
                {
                    *child = Value::String("<REDACTED>".to_string());
                    continue;
                }
                redact_sensitive_values(child);
            }
        }
        Value::Array(items) => {
            for item in items.iter_mut() {
                redact_sensitive_values(item);
            }
        }
        _ => {}
    }
}
