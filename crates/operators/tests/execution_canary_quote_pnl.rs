use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_operators::execution_canary_quote_pnl::{
    build_report_from_config_path, build_report_from_db_path, parse_args_from,
};
use copybot_storage_core::{ExecutionQuoteCanaryEventInsert, SqliteStore};
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
fn execution_canary_quote_pnl_operator_reports_adjusted_pnl() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let opened = ts("2026-06-02T12:00:00Z");
    let closed = opened + Duration::seconds(30);
    store.insert_shadow_closed_trade_exact(
        "sell-operator",
        "leader-wallet",
        "TokenMint",
        50.0,
        None,
        0.10,
        0.13,
        0.03,
        opened,
        closed,
    )?;
    let close_id = store
        .list_execution_quote_canary_close_candidates_for_signal("sell-operator", 10)?
        .into_iter()
        .next()
        .expect("close candidate")
        .id;
    store.record_execution_quote_canary_event(&quote_event(
        "quote:buy:operator",
        Some("buy-operator"),
        None,
        "buy",
        opened,
        "200000000",
        "100",
    ))?;
    store.record_execution_quote_canary_event(&quote_event(
        "quote:sell:operator",
        Some("sell-operator"),
        Some(close_id),
        "sell",
        opened + Duration::seconds(30),
        "50",
        "125000000",
    ))?;
    drop(store);

    let report = build_report_from_db_path(&db_path, closed + Duration::seconds(1));
    let gate = report
        .tiny_execution_gate
        .as_ref()
        .expect("tiny execution gate should exist");
    let provider_selection = report
        .provider_selection
        .as_ref()
        .expect("provider selection should exist");
    let summary = report.summary.expect("operator summary should exist");

    assert_eq!(report.reason_class, "execution_canary_quote_pnl_loaded");
    assert!(report.db_opened);
    assert_eq!(gate.status, "blocked");
    assert_gate_check(gate, "quote_readiness_gate", "block");
    assert_gate_check(gate, "latest_canary_order", "block");
    assert_eq!(summary.shadow_close_breakdown.market_closed_trades, 1);
    assert_eq!(summary.shadow_close_breakdown.stale_closed_trades, 0);
    assert_eq!(summary.pnl_counted_trades, 1);
    assert_close(summary.quote_adjusted_pnl_sol, 0.025);
    assert_close(summary.quote_adjusted_pnl_after_priority_fee_sol, 0.02498);
    assert_close(summary.quote_vs_shadow_delta_sol, -0.005);
    assert_close(summary.quote_after_fee_vs_shadow_delta_sol, -0.00502);
    assert_eq!(summary.quote_diagnostics.entry_counted.events, 1);
    assert_close(
        summary.quote_diagnostics.entry_all.quote_latency_ms_avg,
        20.0,
    );
    assert_eq!(summary.threshold_summaries.len(), 4);
    assert_eq!(summary.threshold_summaries[0].threshold_bps, 150);
    assert_eq!(summary.threshold_summaries[0].counted_trades, 1);
    assert_eq!(summary.buy_slippage_buckets[0].bucket, "<=150");
    assert_eq!(summary.buy_slippage_buckets[0].trades, 1);
    assert_eq!(summary.entry_decision_delay_buckets[0].bucket, "<2s");
    assert_eq!(summary.buy_leader_notional_buckets[0].bucket, "<0.5");
    assert_eq!(summary.route_counts[0].label, "Metis");
    assert_eq!(summary.priority_fee_status_counts[0].status, "ok");
    assert_eq!(summary.force_exit_counted_trades, 0);
    assert_eq!(summary.force_exit_skipped_entry_trades, 0);
    assert_eq!(provider_selection.selected_generic_metis_events, 2);
    assert_eq!(provider_selection.selected_pump_fun_paid_events, 0);
    Ok(())
}

#[test]
fn execution_canary_quote_pnl_gate_checks_tiny_config_preflight() -> Result<()> {
    let dir = tempdir()?;
    let state_dir = dir.path().join("state");
    fs::create_dir(&state_dir)?;
    let db_path = state_dir.join("runtime.db");
    let signer_path = dir.path().join("tiny-signer.json");
    let submit_token_path = dir.path().join("submit-adapter.token");
    let signer_pubkey = write_test_keypair(&signer_path)?;
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    store.record_execution_quote_canary_event(&quote_event(
        "quote:buy:schema-prime",
        Some("buy-schema-prime"),
        None,
        "buy",
        ts("2026-05-01T00:00:00Z"),
        "200000000",
        "100",
    ))?;
    drop(store);
    fs::write(&submit_token_path, "test-token")?;

    let config_path = dir.path().join("live.toml");
    fs::write(
        &config_path,
        tiny_config(&db_path, &signer_path, &submit_token_path, &signer_pubkey),
    )?;
    let report = build_report_from_config_path(&config_path, ts("2026-06-02T13:00:00Z"));
    let gate = report
        .tiny_execution_gate
        .as_ref()
        .unwrap_or_else(|| panic!("tiny execution gate should exist: {:?}", report.error));

    assert_eq!(report.reason_class, "execution_canary_quote_pnl_loaded");
    assert!(report.config_loaded);
    assert!(report.db_opened);
    assert_gate_check(gate, "execution_disabled", "pass");
    assert_gate_check(gate, "canary_enabled", "pass");
    assert_gate_check(gate, "canary_dry_run", "pass");
    assert_gate_check(gate, "canary_wallet_pubkey", "pass");
    assert_gate_check(gate, "canary_buy_size", "pass");
    assert_gate_check(gate, "canary_max_open_positions", "pass");
    assert_gate_check(gate, "canary_daily_loss_cap", "pass");
    assert_gate_check(gate, "canary_kill_switch_inactive", "pass");
    assert_gate_check(gate, "execution_signer_pubkey", "pass");
    assert_gate_check(gate, "execution_signer_matches_canary_wallet", "pass");
    assert_gate_check(gate, "execution_signer_keypair_path", "pass");
    assert_gate_check(gate, "execution_signer_keypair_file", "pass");
    assert_gate_check(gate, "execution_signer_keypair_format", "pass");
    assert_gate_check(gate, "execution_signer_keypair_pubkey_match", "pass");
    assert_gate_check(gate, "execution_signer_keypair_permissions", "pass");
    assert_gate_check(gate, "execution_signer_can_sign_preflight", "pass");
    assert_gate_check(gate, "submit_adapter_http_url", "pass");
    assert_gate_check(gate, "submit_adapter_auth_token_file", "pass");
    assert_gate_check(gate, "submit_adapter_auth_token_file_exists", "pass");
    assert_gate_check(gate, "submit_timeout_ms", "pass");
    assert_gate_check(gate, "max_confirm_seconds", "pass");
    assert_gate_check(gate, "max_submit_attempts", "pass");
    assert_gate_check(gate, "simulate_before_submit", "pass");
    assert_gate_check(gate, "pretrade_min_sol_reserve", "pass");
    assert_gate_check(gate, "pretrade_max_priority_fee_lamports", "pass");
    assert_gate_check(gate, "execution_slippage_bps", "pass");
    assert_gate_check(gate, "quote_canary_enabled", "pass");
    assert_gate_check(gate, "swap_instructions_dry_run_enabled", "pass");
    assert_gate_check(gate, "swap_transaction_dry_run_enabled", "pass");
    assert_gate_check(gate, "priority_fee_canary_enabled", "pass");
    Ok(())
}

#[test]
fn execution_canary_quote_pnl_gate_blocks_keypair_secret_public_mismatch() -> Result<()> {
    let dir = tempdir()?;
    let state_dir = dir.path().join("state");
    fs::create_dir(&state_dir)?;
    let db_path = state_dir.join("runtime.db");
    let signer_path = dir.path().join("tiny-signer.json");
    let submit_token_path = dir.path().join("submit-adapter.token");
    let signer_pubkey = write_mismatched_test_keypair(&signer_path)?;
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    store.record_execution_quote_canary_event(&quote_event(
        "quote:buy:mismatch-prime",
        Some("buy-mismatch-prime"),
        None,
        "buy",
        ts("2026-05-01T00:00:00Z"),
        "200000000",
        "100",
    ))?;
    drop(store);
    fs::write(&submit_token_path, "test-token")?;

    let config_path = dir.path().join("live.toml");
    fs::write(
        &config_path,
        tiny_config(&db_path, &signer_path, &submit_token_path, &signer_pubkey),
    )?;
    let report = build_report_from_config_path(&config_path, ts("2026-06-02T13:00:00Z"));
    let gate = report
        .tiny_execution_gate
        .as_ref()
        .unwrap_or_else(|| panic!("tiny execution gate should exist: {:?}", report.error));

    assert_gate_check(gate, "execution_signer_keypair_format", "pass");
    assert_gate_check(gate, "execution_signer_keypair_pubkey_match", "pass");
    assert_gate_check(gate, "execution_signer_can_sign_preflight", "block");
    Ok(())
}

#[test]
fn execution_canary_quote_pnl_gate_redacts_malformed_keypair_content() -> Result<()> {
    let dir = tempdir()?;
    let state_dir = dir.path().join("state");
    fs::create_dir(&state_dir)?;
    let db_path = state_dir.join("runtime.db");
    let signer_path = dir.path().join("tiny-signer.json");
    let submit_token_path = dir.path().join("submit-adapter.token");
    let signer_pubkey = test_signer_pubkey();
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    store.record_execution_quote_canary_event(&quote_event(
        "quote:buy:redaction-prime",
        Some("buy-redaction-prime"),
        None,
        "buy",
        ts("2026-05-01T00:00:00Z"),
        "200000000",
        "100",
    ))?;
    drop(store);

    fs::write(&signer_path, r#"["SUPER_SECRET_DO_NOT_LEAK"]"#)?;
    set_owner_only_permissions(&signer_path)?;
    fs::write(&submit_token_path, "test-token")?;
    let config_path = dir.path().join("live.toml");
    fs::write(
        &config_path,
        tiny_config(&db_path, &signer_path, &submit_token_path, &signer_pubkey),
    )?;

    let report = build_report_from_config_path(&config_path, ts("2026-06-02T13:00:00Z"));
    let gate = report
        .tiny_execution_gate
        .as_ref()
        .unwrap_or_else(|| panic!("tiny execution gate should exist: {:?}", report.error));

    assert_gate_check(gate, "execution_signer_keypair_format", "block");
    assert_gate_does_not_contain(gate, "SUPER_SECRET_DO_NOT_LEAK");
    Ok(())
}

#[test]
fn execution_canary_quote_pnl_cli_accepts_window_flags() -> Result<()> {
    let cli = parse_args_from([
        "--db-path",
        "state/live_runtime.db",
        "--json",
        "--limit",
        "25",
        "--since-hours",
        "6",
    ])?;
    let by_since = parse_args_from([
        "--config",
        "/etc/solana-copy-bot/live.toml",
        "--json",
        "--since",
        "2026-06-02T12:00:00+00:00",
    ])?;

    assert!(cli.db_path.is_some());
    assert_eq!(cli.limit, 25);
    assert_eq!(cli.since_hours, 6);
    assert!(by_since.config_path.is_some());
    assert!(by_since.since.is_some());
    assert!(parse_args_from(["--json"]).is_err());
    assert!(parse_args_from(["--db-path", "db.sqlite", "--json", "--limit", "0"]).is_err());
    Ok(())
}

fn tiny_config(
    db_path: &std::path::Path,
    signer_path: &std::path::Path,
    submit_token_path: &std::path::Path,
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
    let bytes = test_keypair_bytes();
    fs::write(path, serde_json::to_string(&bytes)?)?;
    set_owner_only_permissions(path)?;
    Ok(bs58::encode(&bytes[32..64]).into_string())
}

fn test_signer_pubkey() -> String {
    let bytes = test_keypair_bytes();
    bs58::encode(&bytes[32..64]).into_string()
}

fn test_keypair_bytes() -> Vec<u8> {
    let signing_key = ed25519_dalek::SigningKey::from_bytes(&[42_u8; 32]);
    let mut bytes = signing_key.to_bytes().to_vec();
    bytes.extend(signing_key.verifying_key().to_bytes());
    bytes
}

fn write_mismatched_test_keypair(path: &Path) -> Result<String> {
    let signing_key = ed25519_dalek::SigningKey::from_bytes(&[42_u8; 32]);
    let mismatched_public_key = [7_u8; 32];
    let mut bytes = signing_key.to_bytes().to_vec();
    bytes.extend(mismatched_public_key);
    fs::write(path, serde_json::to_string(&bytes)?)?;
    set_owner_only_permissions(path)?;
    Ok(bs58::encode(mismatched_public_key).into_string())
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

fn quote_event(
    event_id: &str,
    signal_id: Option<&str>,
    close_id: Option<i64>,
    side: &str,
    signal_ts: DateTime<Utc>,
    in_raw: &str,
    out_raw: &str,
) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        event_id: event_id.to_string(),
        signal_id: signal_id.map(ToString::to_string),
        shadow_closed_trade_id: close_id,
        wallet_id: "leader-wallet".to_string(),
        token: "TokenMint".to_string(),
        side: side.to_string(),
        quote_status: "ok".to_string(),
        request_ts: signal_ts + Duration::milliseconds(10),
        signal_ts: Some(signal_ts),
        decision_delay_ms: Some(10),
        quote_latency_ms: Some(20),
        leader_notional_sol: Some(0.2),
        quote_in_amount_raw: Some(in_raw.to_string()),
        quote_out_amount_raw: Some(out_raw.to_string()),
        quote_response_json: None,
        quote_price_sol: Some(0.002),
        shadow_price_sol: Some(0.002),
        slippage_bps: Some(15.0),
        price_impact_pct: Some(0.02),
        route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Metis\"}}]".to_string()),
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports: Some(10_000),
        priority_fee_json: Some("{\"recommended\":10000}".to_string()),
        decision_status: Some("would_execute".to_string()),
        decision_reason: Some("inside_limits".to_string()),
        error: None,
    }
}

fn assert_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 0.000_000_001,
        "actual={actual} expected={expected}"
    );
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

fn assert_gate_does_not_contain(
    gate: &copybot_operators::execution_canary_quote_pnl::TinyExecutionGate,
    needle: &str,
) {
    for check in &gate.checks {
        assert!(
            !check.name.contains(needle)
                && !check.status.contains(needle)
                && !check.value.contains(needle)
                && !check.threshold.contains(needle)
                && !check.reason.contains(needle),
            "gate leaked {needle}: {check:?}"
        );
    }
}
