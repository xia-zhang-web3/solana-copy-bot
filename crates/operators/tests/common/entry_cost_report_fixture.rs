#![allow(dead_code)]
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_storage_core::ExecutionQuoteCanaryEventInsert;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::{fs, path::Path};
pub fn quote_event(ts: DateTime<Utc>) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        http_request_started_ts: None,
        quote_response_available_ts: None,
        event_id: "quote:buy:gate-submit-prime".to_string(),
        signal_id: Some("shadow:sig-gate-submit:signed-pending:TokenMint".to_string()),
        shadow_closed_trade_id: None,
        wallet_id: "leader-wallet".to_string(),
        token: "TokenMint".to_string(),
        side: "buy".to_string(),
        quote_status: "ok".to_string(),
        request_ts: ts + Duration::milliseconds(10),
        signal_ts: Some(ts),
        decision_delay_ms: Some(10),
        quote_latency_ms: Some(20),
        leader_notional_sol: Some(0.2),
        quote_in_amount_raw: Some("10000000".to_string()),
        quote_out_amount_raw: Some("10000".to_string()),
        quote_response_json: Some(r#"{"outAmount":"10000"}"#.to_string()),
        quote_price_sol: Some(0.001),
        shadow_price_sol: Some(0.001),
        slippage_bps: Some(10.0),
        price_impact_pct: Some(0.01),
        route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Metis\"}}]".to_string()),
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports: Some(10_000),
        priority_fee_json: Some("{\"recommended\":10000}".to_string()),
        decision_status: Some("would_execute".to_string()),
        decision_reason: Some("inside_limits".to_string()),
        error: None,
    }
}

pub fn tiny_config(
    db_path: &Path,
    signer_path: &Path,
    submit_token_path: &Path,
    signer_pubkey: &str,
) -> String {
    format!(
        r#"
[sqlite]
path = "{}"

[execution]
enabled = false
canary_enabled = true
canary_dry_run = true
canary_tiny_submit_enabled = true
canary_entry_submit_enabled = true
canary_route = "metis-swap-instructions-dry-run"
canary_buy_size_sol = 0.01
canary_max_open_positions = 1
canary_max_daily_loss_sol = 0.02
canary_kill_switch_path = "state/execution_canary.stop"
canary_wallet_pubkey = "{}"
execution_signer_pubkey = "{}"
execution_signer_keypair_path = "{}"
submit_adapter_http_url = "http://127.0.0.1:8787/submit"
submit_adapter_auth_token_file = "{}"
submit_timeout_ms = 3000
max_confirm_seconds = 15
max_submit_attempts = 1
simulate_before_submit = true
pretrade_min_sol_reserve = 0.05
pretrade_max_priority_fee_lamports = 1000000
slippage_bps = 500.0
quote_canary_enabled = true
quote_canary_base_url = "https://jupiter-swap-api.quiknode.pro/test/"
swap_instructions_dry_run_enabled = true
swap_transaction_dry_run_enabled = true
priority_fee_canary_enabled = true
priority_fee_canary_rpc_url = "https://example.com/rpc"
priority_fee_canary_account = "{}"
"#,
        db_path.display(),
        signer_pubkey,
        signer_pubkey,
        signer_path.display(),
        submit_token_path.display(),
        signer_pubkey
    )
}

pub fn write_test_keypair(path: &Path) -> Result<String> {
    let signing_key = ed25519_dalek::SigningKey::from_bytes(&[77_u8; 32]);
    let mut bytes = signing_key.to_bytes().to_vec();
    bytes.extend(signing_key.verifying_key().to_bytes());
    fs::write(path, serde_json::to_string(&bytes)?)?;
    set_owner_only_permissions(path)?;
    Ok(bs58::encode(&bytes[32..64]).into_string())
}

#[cfg(unix)]
fn set_owner_only_permissions(path: &Path) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_owner_only_permissions(_path: &Path) -> Result<()> {
    Ok(())
}
