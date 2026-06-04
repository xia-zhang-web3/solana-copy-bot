use super::*;

#[tokio::test]
async fn execution_canary_state_machine_no_submit_reserves_and_disables_order() -> Result<()> {
    let db_path = unique_execution_state_machine_test_path("no-submit");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = copybot_core_types::CopySignalRow {
        signal_id: "shadow:sig-state-machine:leader-wallet:buy:TokenMint".to_string(),
        wallet_id: "leader-wallet".to_string(),
        side: "buy".to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts: now,
        status: "shadow_recorded".to_string(),
    };
    store.insert_copy_signal(&signal)?;
    record_state_machine_entry_quote(&store, &signal, now, "would_execute", "ok")?;

    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_route = "metis-canary".to_string();
    config.canary_buy_size_sol = 0.01;
    config.quote_canary_buy_slippage_bps = 500;
    let state_machine = crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(
        config,
        crate::execution_submit_adapter::NoSubmitExecutionAdapter,
    );

    let first = state_machine.process_buy_candidate(&store, &signal, now)?;
    let second = state_machine.process_buy_candidate(&store, &signal, now)?;
    let order = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .expect("canary order should exist");

    assert_eq!(first.candidates, 1);
    assert_eq!(first.reserved, 1);
    assert_eq!(first.built, 1);
    assert_eq!(first.simulated, 1);
    assert_eq!(first.submit_disabled, 1);
    assert_eq!(first.existing, 0);
    assert_eq!(second.reserved, 0);
    assert_eq!(second.built, 0);
    assert_eq!(second.simulated, 0);
    assert_eq!(second.submit_disabled, 0);
    assert_eq!(second.existing, 1);
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMIT_DISABLED
    );
    assert_eq!(
        order.err_code.as_deref(),
        Some(copybot_storage_core::EXECUTION_ERROR_SUBMIT_DISABLED)
    );
    assert_eq!(
        order.simulation_status.as_deref(),
        Some(copybot_storage_core::EXECUTION_SIMULATION_STATUS_SKIPPED_NO_SUBMIT)
    );
    assert_eq!(
        order.simulation_error.as_deref(),
        Some("no_submit_adapter_simulation_skipped")
    );
    assert!(order.tx_signature.is_none());
    assert_eq!(
        order.client_order_id,
        format!("copybot:canary:{}", signal.signal_id)
    );

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn execution_canary_state_machine_skips_non_buy_signal() -> Result<()> {
    let db_path = unique_execution_state_machine_test_path("skip-sell");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = copybot_core_types::CopySignalRow {
        signal_id: "shadow:sig-state-machine:leader-wallet:sell:TokenMint".to_string(),
        wallet_id: "leader-wallet".to_string(),
        side: "sell".to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts: now,
        status: "shadow_recorded".to_string(),
    };
    store.insert_copy_signal(&signal)?;

    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_route = "metis-canary".to_string();
    let state_machine = crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(
        config,
        crate::execution_submit_adapter::NoSubmitExecutionAdapter,
    );

    let summary = state_machine.process_buy_candidate(&store, &signal, now)?;

    assert_eq!(summary.skipped_reason, Some("not_buy"));
    assert_eq!(summary.reserved, 0);
    assert!(store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn execution_canary_state_machine_records_build_failure() -> Result<()> {
    let db_path = unique_execution_state_machine_test_path("build-failure");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = state_machine_signal("buy-build-failure", "buy", now);
    store.insert_copy_signal(&signal)?;
    record_state_machine_entry_quote(&store, &signal, now, "would_execute", "ok")?;
    let state_machine = crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(
        canary_config(),
        BuildFailureAdapter,
    );

    let summary = state_machine.process_buy_candidate(&store, &signal, now)?;
    let order = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .expect("canary order should exist");

    assert_eq!(summary.reserved, 1);
    assert_eq!(summary.built, 0);
    assert_eq!(summary.simulated, 0);
    assert_eq!(summary.failed, 1);
    assert_eq!(summary.submit_disabled, 0);
    assert_eq!(summary.last_error.as_deref(), Some("build failed"));
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED
    );
    assert_eq!(
        order.err_code.as_deref(),
        Some(copybot_storage_core::EXECUTION_ERROR_BUILD_FAILED)
    );
    assert_eq!(order.simulation_error.as_deref(), Some("build failed"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn execution_canary_state_machine_records_simulation_failure() -> Result<()> {
    let db_path = unique_execution_state_machine_test_path("simulation-failure");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = state_machine_signal("buy-simulation-failure", "buy", now);
    store.insert_copy_signal(&signal)?;
    record_state_machine_entry_quote(&store, &signal, now, "would_execute", "ok")?;
    let state_machine = crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(
        canary_config(),
        SimulationFailureAdapter,
    );

    let summary = state_machine.process_buy_candidate(&store, &signal, now)?;
    let order = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .expect("canary order should exist");

    assert_eq!(summary.reserved, 1);
    assert_eq!(summary.built, 1);
    assert_eq!(summary.simulated, 1);
    assert_eq!(summary.failed, 1);
    assert_eq!(summary.submit_disabled, 0);
    assert_eq!(summary.last_error.as_deref(), Some("simulation failed"));
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED
    );
    assert_eq!(
        order.err_code.as_deref(),
        Some(copybot_storage_core::EXECUTION_ERROR_SIMULATION_FAILED)
    );
    assert_eq!(
        order.simulation_status.as_deref(),
        Some(copybot_storage_core::EXECUTION_SIMULATION_STATUS_FAILED)
    );
    assert_eq!(order.simulation_error.as_deref(), Some("simulation failed"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn execution_canary_state_machine_restart_after_built_does_not_rebuild() -> Result<()> {
    let db_path = unique_execution_state_machine_test_path("restart-built");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = state_machine_signal("buy-built-before-restart", "buy", now);
    store.insert_copy_signal(&signal)?;
    let reserved = store.reserve_execution_canary_order(&signal.signal_id, "metis-canary", now)?;
    store.mark_execution_canary_built(&reserved.order.order_id, now)?;
    let state_machine = crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(
        canary_config(),
        crate::execution_submit_adapter::NoSubmitExecutionAdapter,
    );

    let summary = state_machine.process_buy_candidate(&store, &signal, now)?;
    let order = store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .expect("canary order should exist");

    assert_eq!(summary.existing, 1);
    assert_eq!(summary.built, 0);
    assert_eq!(summary.simulated, 0);
    assert_eq!(summary.submit_disabled, 0);
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_BUILT
    );

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn execution_canary_state_machine_build_request_carries_quote_priority_metadata() -> Result<()>
{
    let db_path = unique_execution_state_machine_test_path("metadata");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = state_machine_signal("buy-metadata", "buy", now);
    store.insert_copy_signal(&signal)?;
    store.record_execution_quote_canary_event(
        &copybot_storage_core::ExecutionQuoteCanaryEventInsert {
            event_id: "quote:entry:metadata".to_string(),
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
            quote_price_sol: Some(0.081),
            shadow_price_sol: Some(0.08),
            slippage_bps: Some(125.0),
            price_impact_pct: Some(0.01),
            route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Metis\"}}]".to_string()),
            priority_fee_status: Some("ok".to_string()),
            priority_fee_lamports: Some(12_345),
            priority_fee_json: Some("{\"recommended\":12345}".to_string()),
            decision_status: Some("would_execute".to_string()),
            decision_reason: Some("within_slippage_limit".to_string()),
            error: None,
        },
    )?;
    let expected_client_order_id = format!("copybot:canary:{}", signal.signal_id);
    let state_machine = crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(
        canary_config(),
        MetadataAssertAdapter {
            expected_client_order_id,
        },
    );

    let summary = state_machine.process_buy_candidate(&store, &signal, now)?;

    assert_eq!(summary.reserved, 1);
    assert_eq!(summary.built, 1);
    assert_eq!(summary.simulated, 1);
    assert_eq!(summary.submit_disabled, 1);

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

fn state_machine_signal(
    signal_id: &str,
    side: &str,
    ts: chrono::DateTime<Utc>,
) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: format!("shadow:sig-state-machine:leader-wallet:{signal_id}:TokenMint"),
        wallet_id: "leader-wallet".to_string(),
        side: side.to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}

fn canary_config() -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_route = "metis-canary".to_string();
    config.canary_buy_size_sol = 0.01;
    config.quote_canary_buy_slippage_bps = 500;
    config
}

fn record_state_machine_entry_quote(
    store: &SqliteStore,
    signal: &copybot_core_types::CopySignalRow,
    now: chrono::DateTime<Utc>,
    decision_status: &str,
    priority_fee_status: &str,
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
            quote_price_sol: Some(0.081),
            shadow_price_sol: Some(0.08),
            slippage_bps: Some(125.0),
            price_impact_pct: Some(0.01),
            route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Metis\"}}]".to_string()),
            priority_fee_status: Some(priority_fee_status.to_string()),
            priority_fee_lamports: Some(12_345),
            priority_fee_json: Some("{\"recommended\":12345}".to_string()),
            decision_status: Some(decision_status.to_string()),
            decision_reason: Some("within_slippage_limit".to_string()),
            error: None,
        },
    )?;
    Ok(())
}

#[derive(Debug, Clone)]
struct BuildFailureAdapter;

impl crate::execution_submit_adapter::ExecutionSubmitAdapter for BuildFailureAdapter {
    fn build_transaction_plan(
        &self,
        _request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
    ) -> Result<crate::execution_submit_adapter::ExecutionTransactionPlan> {
        Err(anyhow::anyhow!("build failed"))
    }

    fn simulate_transaction_plan(
        &self,
        _plan: &crate::execution_submit_adapter::ExecutionTransactionPlan,
    ) -> Result<crate::execution_submit_adapter::ExecutionSimulationResult> {
        unreachable!("build failure should stop before simulation")
    }

    fn plan_submit(
        &self,
        _request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
    ) -> Result<crate::execution_submit_adapter::ExecutionSubmitPlan> {
        unreachable!("build failure should stop before submit planning")
    }
}

#[derive(Debug, Clone)]
struct SimulationFailureAdapter;

impl crate::execution_submit_adapter::ExecutionSubmitAdapter for SimulationFailureAdapter {
    fn build_transaction_plan(
        &self,
        request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
    ) -> Result<crate::execution_submit_adapter::ExecutionTransactionPlan> {
        crate::execution_submit_adapter::NoSubmitExecutionAdapter.build_transaction_plan(request)
    }

    fn simulate_transaction_plan(
        &self,
        _plan: &crate::execution_submit_adapter::ExecutionTransactionPlan,
    ) -> Result<crate::execution_submit_adapter::ExecutionSimulationResult> {
        Ok(crate::execution_submit_adapter::ExecutionSimulationResult {
            status: copybot_storage_core::EXECUTION_SIMULATION_STATUS_FAILED.to_string(),
            error: Some("simulation failed".to_string()),
        })
    }

    fn plan_submit(
        &self,
        _request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
    ) -> Result<crate::execution_submit_adapter::ExecutionSubmitPlan> {
        unreachable!("failed simulation should stop before submit planning")
    }
}

#[derive(Debug, Clone)]
struct MetadataAssertAdapter {
    expected_client_order_id: String,
}

impl crate::execution_submit_adapter::ExecutionSubmitAdapter for MetadataAssertAdapter {
    fn build_transaction_plan(
        &self,
        request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
    ) -> Result<crate::execution_submit_adapter::ExecutionTransactionPlan> {
        assert_eq!(request.client_order_id, self.expected_client_order_id);
        assert_eq!(
            request.metadata.quote_source.as_deref(),
            Some("execution_quote_canary_event")
        );
        assert_eq!(
            request.metadata.quote_event_id.as_deref(),
            Some("quote:entry:metadata")
        );
        assert_eq!(
            request.metadata.quote_in_amount_raw.as_deref(),
            Some("10000000")
        );
        assert_eq!(
            request.metadata.quote_out_amount_raw.as_deref(),
            Some("123456")
        );
        assert_eq!(request.metadata.priority_fee_lamports, Some(12_345));
        assert_eq!(
            request.metadata.priority_fee_source.as_deref(),
            Some("execution_quote_canary_event")
        );
        crate::execution_submit_adapter::NoSubmitExecutionAdapter.build_transaction_plan(request)
    }

    fn simulate_transaction_plan(
        &self,
        plan: &crate::execution_submit_adapter::ExecutionTransactionPlan,
    ) -> Result<crate::execution_submit_adapter::ExecutionSimulationResult> {
        assert_eq!(plan.client_order_id, self.expected_client_order_id);
        assert_eq!(
            plan.metadata.route_plan_json.as_deref(),
            Some("[{\"swapInfo\":{\"label\":\"Metis\"}}]")
        );
        crate::execution_submit_adapter::NoSubmitExecutionAdapter.simulate_transaction_plan(plan)
    }

    fn plan_submit(
        &self,
        request: &crate::execution_submit_adapter::ExecutionSubmitRequest,
    ) -> Result<crate::execution_submit_adapter::ExecutionSubmitPlan> {
        crate::execution_submit_adapter::NoSubmitExecutionAdapter.plan_submit(request)
    }
}

fn unique_execution_state_machine_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-execution-state-machine-{name}-{}-{nanos}.db",
        std::process::id()
    ))
}
