use super::*;

#[tokio::test]
async fn execution_canary_entry_gate_blocks_missing_quote_before_reserve() -> Result<()> {
    let db_path = unique_entry_gate_test_path("missing-quote");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = entry_gate_signal("buy-missing-quote", now);
    store.insert_copy_signal(&signal)?;
    let state_machine = entry_gate_state_machine();

    let summary = state_machine
        .process_buy_candidate(&store, &signal, now)
        .await?;

    assert_eq!(summary.entry_gate_blocked, 1);
    assert_eq!(summary.skipped_reason, Some("missing_quote_metadata"));
    assert!(store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn execution_canary_entry_gate_blocks_would_skip_quote_before_reserve() -> Result<()> {
    let db_path = unique_entry_gate_test_path("would-skip");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = entry_gate_signal("buy-would-skip", now);
    store.insert_copy_signal(&signal)?;
    record_entry_gate_quote(&store, &signal, now, "would_skip", Some("ok"), Some(12_345))?;
    let state_machine = entry_gate_state_machine();

    let summary = state_machine
        .process_buy_candidate(&store, &signal, now)
        .await?;

    assert_eq!(summary.entry_gate_blocked, 1);
    assert_eq!(summary.skipped_reason, Some("entry_decision_not_execute"));
    assert!(store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn execution_canary_entry_gate_blocks_priority_fee_error_before_reserve() -> Result<()> {
    let db_path = unique_entry_gate_test_path("priority-error");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = entry_gate_signal("buy-priority-error", now);
    store.insert_copy_signal(&signal)?;
    record_entry_gate_quote(
        &store,
        &signal,
        now,
        "would_execute",
        Some("error"),
        Some(12_345),
    )?;
    let state_machine = entry_gate_state_machine();

    let summary = state_machine
        .process_buy_candidate(&store, &signal, now)
        .await?;

    assert_eq!(summary.entry_gate_blocked, 1);
    assert_eq!(summary.skipped_reason, Some("priority_fee_not_ok"));
    assert!(store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn execution_canary_entry_gate_blocks_missing_priority_fee_lamports_before_reserve(
) -> Result<()> {
    let db_path = unique_entry_gate_test_path("priority-missing-lamports");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = entry_gate_signal("buy-priority-missing-lamports", now);
    store.insert_copy_signal(&signal)?;
    record_entry_gate_quote(&store, &signal, now, "would_execute", Some("ok"), None)?;
    let state_machine = entry_gate_state_machine();

    let summary = state_machine
        .process_buy_candidate(&store, &signal, now)
        .await?;

    assert_eq!(summary.entry_gate_blocked, 1);
    assert_eq!(summary.skipped_reason, Some("missing_priority_fee"));
    assert!(store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

fn entry_gate_state_machine() -> crate::execution_canary_state_machine::ExecutionCanaryStateMachine<
    crate::execution_submit_adapter::NoSubmitExecutionAdapter,
> {
    crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(
        entry_gate_config(),
        crate::execution_submit_adapter::NoSubmitExecutionAdapter,
    )
}

fn entry_gate_config() -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_route = "metis-canary".to_string();
    config.canary_buy_size_sol = 0.01;
    config.quote_canary_buy_slippage_bps = 500;
    config
}

fn entry_gate_signal(
    signal_id: &str,
    ts: chrono::DateTime<Utc>,
) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: format!("shadow:sig-entry-gate:leader-wallet:{signal_id}:TokenMint"),
        wallet_id: "leader-wallet".to_string(),
        side: "buy".to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}

fn record_entry_gate_quote(
    store: &SqliteStore,
    signal: &copybot_core_types::CopySignalRow,
    now: chrono::DateTime<Utc>,
    decision_status: &str,
    priority_fee_status: Option<&str>,
    priority_fee_lamports: Option<u64>,
) -> Result<()> {
    store.record_execution_quote_canary_event(
        &copybot_storage_core::ExecutionQuoteCanaryEventInsert {
            event_id: format!("quote:entry:{}", signal.signal_id),
            signal_id: Some(signal.signal_id.clone()),
            shadow_closed_trade_id: None,
            wallet_id: signal.wallet_id.clone(),
            token: signal.token.clone(),
            side: "buy".to_string(),
            quote_status: "ok".to_string(),
            request_ts: now,
            signal_ts: Some(signal.ts),
            decision_delay_ms: Some(7),
            quote_latency_ms: Some(11),
            leader_notional_sol: Some(signal.notional_sol),
            quote_in_amount_raw: Some("10000000".to_string()),
            quote_out_amount_raw: Some("123456".to_string()),
            quote_response_json: None,
            quote_price_sol: Some(0.081),
            shadow_price_sol: Some(0.08),
            slippage_bps: Some(125.0),
            price_impact_pct: Some(0.01),
            route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Metis\"}}]".to_string()),
            priority_fee_status: priority_fee_status.map(str::to_string),
            priority_fee_lamports,
            priority_fee_json: Some("{\"recommended\":12345}".to_string()),
            decision_status: Some(decision_status.to_string()),
            decision_reason: Some("within_slippage_limit".to_string()),
            error: None,
        },
    )?;
    Ok(())
}

fn unique_entry_gate_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-execution-entry-gate-{name}-{}-{nanos}.db",
        std::process::id()
    ))
}
