use super::*;

#[tokio::test]
async fn execution_canary_runner_uses_swap_blueprint_route_without_legacy_insert() -> Result<()> {
    let db_path = unique_swap_blueprint_runner_test_path("runner-route");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = swap_blueprint_runner_signal("buy-route", now);
    store.insert_copy_signal(&signal)?;
    record_swap_blueprint_runner_quote(&store, &signal, now)?;

    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_route =
        crate::execution_submit_adapter::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN.to_string();
    config.canary_buy_size_sol = 0.01;
    config.canary_batch_limit = 5;
    config.canary_wallet_pubkey = "DryRunWallet11111111111111111111111111111111".to_string();
    config.quote_canary_buy_slippage_bps = 500;
    let runner = ExecutionCanaryRunner::new(config);

    let first = runner.process_tick(&store, now).await?;
    let second = runner.process_tick(&store, now).await?;
    let order = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .expect("canary order should exist");

    assert_eq!(first.candidates, 1);
    assert_eq!(first.inserted, 0);
    assert_eq!(first.existing, 0);
    assert_eq!(first.state_machine_reserved, 1);
    assert_eq!(first.state_machine_built, 1);
    assert_eq!(first.state_machine_simulated, 1);
    assert_eq!(first.state_machine_submit_disabled, 1);
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMIT_DISABLED
    );
    assert_eq!(
        order.simulation_status.as_deref(),
        Some(copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED)
    );
    assert!(order.simulation_error.is_none());
    assert!(store.latest_execution_dry_run_order()?.is_none());
    assert_eq!(second.candidates, 0);
    assert_eq!(second.state_machine_reserved, 0);

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

fn swap_blueprint_runner_signal(
    signal_id: &str,
    ts: chrono::DateTime<Utc>,
) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: format!("shadow:sig-swap-blueprint-runner:leader-wallet:{signal_id}:TokenMint"),
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

fn record_swap_blueprint_runner_quote(
    store: &SqliteStore,
    signal: &copybot_core_types::CopySignalRow,
    now: chrono::DateTime<Utc>,
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
            decision_delay_ms: Some(9),
            quote_latency_ms: Some(13),
            leader_notional_sol: Some(signal.notional_sol),
            quote_in_amount_raw: Some("10000000".to_string()),
            quote_out_amount_raw: Some("123456".to_string()),
            quote_price_sol: Some(0.081),
            shadow_price_sol: Some(0.08),
            slippage_bps: Some(125.0),
            price_impact_pct: Some(0.01),
            route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Pump.fun Amm\"}}]".to_string()),
            priority_fee_status: Some("ok".to_string()),
            priority_fee_lamports: Some(22_000),
            priority_fee_json: Some("{\"recommended\":22000}".to_string()),
            decision_status: Some("would_execute".to_string()),
            decision_reason: Some("within_slippage_limit".to_string()),
            error: None,
        },
    )?;
    Ok(())
}

fn unique_swap_blueprint_runner_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-swap-blueprint-runner-{name}-{}-{nanos}.db",
        std::process::id()
    ))
}
