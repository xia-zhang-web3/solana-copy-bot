use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{CopySignalRow, Lamports, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS};
use copybot_operators::execution_canary_quote_pnl::build_report_from_config_path;
use copybot_storage_core::{
    ExecutionQuoteCanaryEventInsert, SqliteStore, EXECUTION_SIMULATION_STATUS_PASSED,
};
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use tempfile::tempdir;

fn ts(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .expect("timestamp")
        .with_timezone(&Utc)
}

#[test]
fn tiny_execution_gate_blocks_pending_submit_and_retry_ready_orders() -> Result<()> {
    let dir = tempdir()?;
    let state_dir = dir.path().join("state");
    fs::create_dir(&state_dir)?;
    let db_path = state_dir.join("runtime.db");
    let signer_path = dir.path().join("tiny-signer.json");
    let submit_token_path = dir.path().join("submit-adapter.token");
    let signer_pubkey = write_test_keypair(&signer_path)?;
    fs::write(&submit_token_path, "test-token")?;

    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = ts("2026-06-08T12:00:00Z");
    mark_submitted_order(&store, "signed-pending", now, Some("tx-signed"))?;
    mark_submitted_order(&store, "unknown-pending", now + Duration::seconds(10), None)?;
    mark_retry_ready_order(&store, "retry-ready", now + Duration::seconds(20))?;
    store.record_execution_quote_canary_event(&quote_event(now))?;
    drop(store);

    let config_path = dir.path().join("live.toml");
    fs::write(
        &config_path,
        tiny_config(&db_path, &signer_path, &submit_token_path, &signer_pubkey),
    )?;
    let report = build_report_from_config_path(&config_path, now + Duration::seconds(40));
    let gate = report
        .tiny_execution_gate
        .as_ref()
        .unwrap_or_else(|| panic!("tiny execution gate should exist: {:?}", report.error));

    assert_eq!(gate.active_submit_orders, 3);
    assert_eq!(gate.pending_submit_orders, 2);
    assert_eq!(gate.retry_ready_orders, 1);
    assert_eq!(gate.retry_budget_blocked_orders, 2);
    assert_gate_check(gate, "pending_signed_submit_orders", "block");
    assert_gate_check(gate, "pending_unknown_submit_orders", "block");
    assert_gate_check(gate, "retry_ready_orders", "block");
    assert_gate_check(gate, "retry_budget_blocked_orders", "block");
    Ok(())
}

fn mark_submitted_order(
    store: &SqliteStore,
    name: &str,
    now: DateTime<Utc>,
    tx_signature: Option<&str>,
) -> Result<String> {
    let signal = signal(name, now);
    store.insert_copy_signal(&signal)?;
    let reserve = store.reserve_execution_canary_order(
        &signal.signal_id,
        "metis-swap-instructions-dry-run",
        now,
    )?;
    store.mark_execution_canary_built(&reserve.order.order_id, now + Duration::seconds(1))?;
    store.mark_execution_canary_simulated(
        &reserve.order.order_id,
        now + Duration::seconds(2),
        EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    match tx_signature {
        Some(tx) => {
            store.mark_execution_canary_submitted(
                &reserve.order.order_id,
                now + Duration::seconds(3),
                tx,
            )?;
        }
        None => {
            store.mark_execution_canary_submitted_unknown(
                &reserve.order.order_id,
                now + Duration::seconds(3),
                "submitted_without_signature",
            )?;
        }
    }
    Ok(reserve.order.order_id)
}

fn mark_retry_ready_order(store: &SqliteStore, name: &str, now: DateTime<Utc>) -> Result<String> {
    let order_id = mark_submitted_order(store, name, now, None)?;
    store.mark_execution_canary_retry_after_submit_timeout(
        &order_id,
        now + Duration::seconds(10),
        Duration::seconds(1),
        "retry_after_unknown_submit_timeout",
    )?;
    Ok(order_id)
}

fn signal(name: &str, ts: DateTime<Utc>) -> CopySignalRow {
    CopySignalRow {
        signal_id: format!("shadow:sig-gate-submit:{name}:TokenMint"),
        wallet_id: "leader-wallet".to_string(),
        side: "buy".to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}

fn quote_event(ts: DateTime<Utc>) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
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

fn tiny_config(
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

fn write_test_keypair(path: &Path) -> Result<String> {
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

fn assert_gate_check(
    gate: &copybot_operators::execution_canary_quote_pnl::TinyExecutionGate,
    name: &str,
    status: &str,
) {
    assert!(
        gate.checks
            .iter()
            .any(|check| check.name == name && check.status == status),
        "missing gate check {name} with status {status}: {:?}",
        gate.checks
    );
}
