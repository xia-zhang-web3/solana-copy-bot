use super::b58_fixture;
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_shadow::ShadowSignalResult;
use serde_json::{json, Value};
use std::{fs, os::unix::fs::PermissionsExt, path::Path};

pub(super) fn config(url: String) -> ExecutionConfig {
    let mut c = b58_fixture::config(url);
    c.enabled = false;
    c.canary_tiny_submit_enabled = false;
    c.priority_fee_canary_enabled = false;
    c.quote_canary_public_parallel_enabled = false;
    c.quote_canary_pump_fun_parallel_enabled = false;
    c.quote_canary_api_key.clear();
    c.priority_fee_canary_rpc_url.clear();
    assert!(!c.enabled && !c.canary_tiny_submit_enabled);
    c
}
pub(super) fn signal(token: &str) -> ShadowSignalResult {
    ShadowSignalResult {
        signal_id: b58_fixture::signal_id(token, "buy"),
        wallet_id: "leader-b58".into(),
        side: "buy".into(),
        token: token.into(),
        notional_sol: 0.2,
        latency_ms: 0,
        closed_qty: 0.0,
        realized_pnl_sol: 0.0,
        has_open_lots_after_signal: Some(true),
    }
}
pub(super) fn body(token: &str) -> String {
    // Deliberately generic Metis: decimals must resolve through observed BUY, not JSON.
    json!({"inputMint":b58_fixture::SOL,"outputMint":token,
        "inAmount":"200000000","outAmount":"1000000",
        "priceImpactPct":"0","routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}]})
    .to_string()
}
pub(super) fn row_json(r: &copybot_storage_core::ExecutionQuoteCanaryEventInsert) -> Value {
    json!({"event_id":r.event_id,"signal_id":r.signal_id,"wallet_id":r.wallet_id,
        "token":r.token,"side":r.side,"request_ts":r.request_ts,
        "signal_ts":r.signal_ts,"http_request_started_ts":r.http_request_started_ts,
        "quote_response_available_ts":r.quote_response_available_ts,
        "quote_latency_ms":r.quote_latency_ms,"decision_delay_ms":r.decision_delay_ms,
        "quote_status":r.quote_status,"quote_in_amount_raw":r.quote_in_amount_raw,
        "quote_out_amount_raw":r.quote_out_amount_raw,"quote_response_json":r.quote_response_json,
        "shadow_price_sol":r.shadow_price_sol,"quote_price_sol":r.quote_price_sol,
        "slippage_bps":r.slippage_bps,"decision_status":r.decision_status,
        "decision_reason":r.decision_reason,"error":r.error})
}
pub(super) fn freeze(path: &Path, destination: &Path) -> Result<()> {
    let c = rusqlite::Connection::open(path)?;
    c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE;")?;
    drop(c);
    fs::copy(path, destination)?;
    fs::set_permissions(destination, fs::Permissions::from_mode(0o444))?;
    Ok(())
}
pub(super) fn initial_time() -> DateTime<Utc> {
    Utc::now()
}
