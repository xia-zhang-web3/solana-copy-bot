use super::*;

#[tokio::test]
async fn execution_canary_swap_blueprint_adapter_passes_simulation_without_submit() -> Result<()> {
    let db_path = unique_swap_blueprint_test_path("passed");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = swap_blueprint_signal("buy-passed", now);
    store.insert_copy_signal(&signal)?;
    record_swap_blueprint_quote(
        &store,
        &signal,
        now,
        "[{\"swapInfo\":{\"label\":\"Pump.fun Amm\"}}]",
        Some(22_000),
    )?;
    let config = swap_blueprint_config();
    let adapter =
        crate::execution_submit_adapter::JupiterMetisDryRunExecutionAdapter::new(config.clone());
    let state_machine =
        crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(config, adapter);

    let summary = state_machine
        .process_buy_candidate(&store, &signal, now)
        .await?;
    let order = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .expect("canary order should exist");
    let metadata = store
        .load_execution_canary_build_plan_metadata(&order.order_id)?
        .expect("build metadata should be recorded");

    assert_eq!(summary.reserved, 1);
    assert_eq!(summary.built, 1);
    assert_eq!(summary.simulated, 1);
    assert_eq!(summary.submit_disabled, 1);
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMIT_DISABLED
    );
    assert_eq!(
        order.simulation_status.as_deref(),
        Some(copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED)
    );
    assert!(order.simulation_error.is_none());
    assert!(order.tx_signature.is_none());
    assert_eq!(metadata.priority_fee_lamports, Some(22_000));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn execution_canary_swap_blueprint_adapter_fails_build_on_bad_route_json() -> Result<()> {
    let db_path = unique_swap_blueprint_test_path("bad-route-json");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = swap_blueprint_signal("buy-bad-route-json", now);
    store.insert_copy_signal(&signal)?;
    record_swap_blueprint_quote(&store, &signal, now, "not-json", Some(22_000))?;
    let config = swap_blueprint_config();
    let adapter =
        crate::execution_submit_adapter::JupiterMetisDryRunExecutionAdapter::new(config.clone());
    let state_machine =
        crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(config, adapter);

    let summary = state_machine
        .process_buy_candidate(&store, &signal, now)
        .await?;
    let order = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .expect("canary order should exist");

    assert_eq!(summary.reserved, 1);
    assert_eq!(summary.built, 0);
    assert_eq!(summary.failed, 1);
    assert_eq!(summary.submit_disabled, 0);
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED
    );
    assert_eq!(
        order.err_code.as_deref(),
        Some(copybot_storage_core::EXECUTION_ERROR_BUILD_FAILED)
    );
    assert!(order
        .simulation_error
        .as_deref()
        .unwrap_or_default()
        .contains("invalid route_plan_json"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[test]
fn execution_swap_blueprint_requires_priority_fee_lamports() {
    let request = swap_blueprint_request(None);

    let error = crate::execution_swap_blueprint::build_execution_swap_blueprint(&request)
        .expect_err("missing priority fee lamports should fail blueprint build");

    assert!(error.to_string().contains("missing priority_fee_lamports"));
}

#[test]
fn execution_swap_blueprint_uses_tolerance_not_quote_delta() -> Result<()> {
    let mut request = swap_blueprint_request(Some(22_000));
    request.slippage_tolerance_bps = 500;
    request.metadata.slippage_bps = Some(-4710.0);

    let blueprint = crate::execution_swap_blueprint::build_execution_swap_blueprint(&request)?;

    assert_eq!(blueprint.slippage_bps, 500.0);
    Ok(())
}

fn swap_blueprint_config() -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_route = "metis-swap-instructions-dry-run".to_string();
    config.canary_buy_size_sol = 0.01;
    config.canary_wallet_pubkey = "DryRunWallet11111111111111111111111111111111".to_string();
    config.quote_canary_buy_slippage_bps = 500;
    config
}

fn swap_blueprint_signal(
    signal_id: &str,
    ts: chrono::DateTime<Utc>,
) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: format!("shadow:sig-swap-blueprint:leader-wallet:{signal_id}:TokenMint"),
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

fn swap_blueprint_request(
    priority_fee_lamports: Option<u64>,
) -> crate::execution_submit_adapter::ExecutionSubmitRequest {
    crate::execution_submit_adapter::ExecutionSubmitRequest {
        order_id: "order-blueprint-direct".to_string(),
        signal_id: "signal-blueprint-direct".to_string(),
        client_order_id: "client-blueprint-direct".to_string(),
        route: "metis-swap-instructions-dry-run".to_string(),
        wallet_id: "leader-wallet".to_string(),
        token: "TokenMint".to_string(),
        side: "buy".to_string(),
        buy_size_sol: 0.01,
        slippage_tolerance_bps: 500,
        wallet_pubkey: "DryRunWallet11111111111111111111111111111111".to_string(),
        metadata: crate::execution_submit_adapter::ExecutionBuildPlanMetadata {
            quote_source: Some("execution_quote_canary_event".to_string()),
            quote_event_id: Some("quote:entry:direct".to_string()),
            quote_status: Some("ok".to_string()),
            quote_in_amount_raw: Some("10000000".to_string()),
            quote_out_amount_raw: Some("123456".to_string()),
            quote_response_json: None,
            quote_price_sol: Some(0.081),
            price_impact_pct: Some(0.01),
            route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Pump.fun Amm\"}}]".to_string()),
            priority_fee_source: Some("execution_quote_canary_event".to_string()),
            priority_fee_status: Some("ok".to_string()),
            priority_fee_lamports,
            priority_fee_json: Some("{\"recommended\":22000}".to_string()),
            slippage_bps: Some(125.0),
            decision_status: Some("would_execute".to_string()),
            decision_reason: Some("within_slippage_limit".to_string()),
        },
    }
}

fn record_swap_blueprint_quote(
    store: &SqliteStore,
    signal: &copybot_core_types::CopySignalRow,
    now: chrono::DateTime<Utc>,
    route_plan_json: &str,
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
            route_plan_json: Some(route_plan_json.to_string()),
            priority_fee_status: Some("ok".to_string()),
            priority_fee_lamports,
            priority_fee_json: Some("{\"recommended\":22000}".to_string()),
            decision_status: Some("would_execute".to_string()),
            decision_reason: Some("within_slippage_limit".to_string()),
            error: None,
        },
    )?;
    Ok(())
}

fn unique_swap_blueprint_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-execution-swap-blueprint-{name}-{}-{nanos}.db",
        std::process::id()
    ))
}
