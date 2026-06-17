use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};

use crate::{
    auth::{
        expired_cookie, session_cookie, session_from_headers, unauthorized, with_cookie,
        LoginRequest, SessionResponse,
    },
    AppState,
};

pub async fn login(
    State(state): State<Arc<AppState>>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(request): Json<LoginRequest>,
) -> Response {
    let rate_key = crate::auth::rate_limit_key(&headers, remote, state.config.trust_proxy_headers);
    if state.auth.login_limited(&rate_key) {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    }
    if request.username != state.config.username
        || !crate::auth::AuthStore::verify_password(&state.config.password_hash, &request.password)
    {
        state.auth.record_failure(&rate_key);
        return unauthorized();
    }
    match state
        .auth
        .create_session(&request.username, state.config.session_ttl)
    {
        Ok(session_id) => with_cookie(
            session_cookie(
                &session_id,
                state.config.session_ttl,
                state.config.cookie_secure,
            ),
            SessionResponse {
                authenticated: true,
                username: Some(request.username),
            },
        ),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

pub async fn logout(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let Some(session_id) = session_from_headers(&headers) {
        let _ = state.auth.delete_session(&session_id);
    }
    with_cookie(
        expired_cookie(state.config.cookie_secure),
        SessionResponse {
            authenticated: false,
            username: None,
        },
    )
}

pub async fn session(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    match require_session(&state, &headers) {
        Ok(Some(username)) => Json(SessionResponse {
            authenticated: true,
            username: Some(username),
        })
        .into_response(),
        _ => unauthorized(),
    }
}

pub async fn overview(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    authenticated_snapshot(&state, &headers, "overview", overview_fallback())
}

pub async fn storage(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    authenticated_snapshot(&state, &headers, "storage", rows_fallback("unknown"))
}

pub async fn discovery(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    authenticated_snapshot(&state, &headers, "discovery", rows_fallback("unknown"))
}

pub async fn execution(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    authenticated_snapshot(&state, &headers, "execution", rows_fallback("unknown"))
}

pub async fn strategy(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    authenticated_snapshot(&state, &headers, "strategy", rows_fallback("not_green"))
}

pub async fn alerts(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    authenticated_snapshot(
        &state,
        &headers,
        "alerts",
        json!({
            "status": "unknown",
            "events": [{
                "title": "Snapshot missing",
                "detail": "No alerts.json report snapshot is available.",
                "state": "active",
                "level": "warning"
            }]
        }),
    )
}

pub async fn reports(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    authenticated_snapshot(&state, &headers, "reports", rows_fallback("unknown"))
}

fn authenticated_snapshot(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    name: &str,
    fallback: Value,
) -> Response {
    match require_session(state, headers) {
        Ok(Some(_)) => Json(state.snapshots.load(name, fallback)).into_response(),
        _ => unauthorized(),
    }
}

fn require_session(state: &Arc<AppState>, headers: &HeaderMap) -> anyhow::Result<Option<String>> {
    let Some(session_id) = session_from_headers(headers) else {
        return Ok(None);
    };
    state
        .auth
        .validate_session(&session_id, state.config.idle_timeout)
}

fn overview_fallback() -> Value {
    json!({
        "status": "unknown",
        "entries": "unknown",
        "sells": "unknown",
        "open_positions": "unknown",
        "disk_runway_days": "unknown",
        "candidates": "unknown",
        "candidate_floor": "unknown",
        "latest_blocker": "snapshot_missing"
    })
}

fn rows_fallback(status: &str) -> Value {
    json!({
        "status": status,
        "rows": [["snapshot", "missing"]]
    })
}
