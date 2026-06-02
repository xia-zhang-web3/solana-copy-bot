use super::*;

#[tokio::test]
async fn execution_canary_state_machine_submit_timeout_waits_before_deadline() -> Result<()> {
    let db_path = unique_timeout_state_machine_test_path("wait");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let order_id = mark_submitted_unknown_order(&store, "buy-timeout-wait", now)?;
    let state_machine = timeout_state_machine();

    let summary = state_machine.process_submit_timeout(
        &store,
        &order_id,
        chrono::Duration::seconds(60),
        now + chrono::Duration::seconds(30),
    )?;
    let order = store
        .load_execution_canary_order(&order_id)?
        .expect("canary order should exist");

    assert_eq!(summary.submit_timeout_candidates, 1);
    assert_eq!(summary.submit_timeout_wait, 1);
    assert_eq!(summary.submit_timeout_retry, 0);
    assert_eq!(summary.submit_timeout_expire_unsafe, 0);
    assert_eq!(summary.last_elapsed_seconds, 28);
    assert_eq!(summary.last_timeout_seconds, 60);
    assert_eq!(
        summary.last_confirm_decision.as_deref(),
        Some(copybot_storage_core::EXECUTION_CANARY_CONFIRM_DECISION_WAIT)
    );
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMITTED
    );
    assert_eq!(order.attempt, 1);

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn execution_canary_state_machine_submit_timeout_retries_unknown_without_signature(
) -> Result<()> {
    let db_path = unique_timeout_state_machine_test_path("retry");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let order_id = mark_submitted_unknown_order(&store, "buy-timeout-retry", now)?;
    let state_machine = timeout_state_machine();

    let summary = state_machine.process_submit_timeout(
        &store,
        &order_id,
        chrono::Duration::seconds(60),
        now + chrono::Duration::seconds(65),
    )?;
    let order = store
        .load_execution_canary_order(&order_id)?
        .expect("canary order should exist");

    assert_eq!(summary.submit_timeout_retry, 1);
    assert_eq!(summary.simulated, 1);
    assert_eq!(summary.submit_timeout_expire_unsafe, 0);
    assert_eq!(
        summary.last_confirm_decision.as_deref(),
        Some(copybot_storage_core::EXECUTION_CANARY_CONFIRM_DECISION_RETRY)
    );
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_SIMULATED
    );
    assert_eq!(order.attempt, 2);
    assert!(order.tx_signature.is_none());
    assert_eq!(
        order.simulation_error.as_deref(),
        Some("retry_after_unknown_submit_timeout")
    );

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn execution_canary_state_machine_submit_timeout_expires_signature_without_retry(
) -> Result<()> {
    let db_path = unique_timeout_state_machine_test_path("expire-signature");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let order_id = mark_submitted_signed_order(&store, "buy-timeout-signature", now)?;
    let state_machine = timeout_state_machine();

    let summary = state_machine.process_submit_timeout(
        &store,
        &order_id,
        chrono::Duration::seconds(60),
        now + chrono::Duration::seconds(65),
    )?;
    let order = store
        .load_execution_canary_order(&order_id)?
        .expect("canary order should exist");

    assert_eq!(summary.submit_timeout_retry, 0);
    assert_eq!(summary.submit_timeout_expire_unsafe, 1);
    assert_eq!(summary.expired, 1);
    assert_eq!(
        summary.last_confirm_decision.as_deref(),
        Some(copybot_storage_core::EXECUTION_CANARY_CONFIRM_DECISION_EXPIRE_UNSAFE)
    );
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_EXPIRED
    );
    assert_eq!(
        order.err_code.as_deref(),
        Some(copybot_storage_core::EXECUTION_ERROR_EXPIRED)
    );
    assert_eq!(order.tx_signature.as_deref(), Some("tx-sig"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn execution_canary_dry_end_to_end_reports_owned_buy_sell_pnl_summary() -> Result<()> {
    let db_path = unique_timeout_state_machine_test_path("dry-e2e");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = timeout_signal("sell-e2e", "sell", now);
    store.insert_copy_signal(&signal)?;
    store.record_execution_canary_open_position(
        "exec-canary:e2e-buy",
        "TokenMint",
        10.0,
        Some(copybot_core_types::TokenQuantity::new(10_000, 3)),
        1.0,
        now,
    )?;
    let state_machine = timeout_state_machine();

    let summary = state_machine.process_sell_candidate(
        &store,
        &signal,
        crate::execution_canary_state_machine::ExecutionCanarySellCloseInput {
            target_qty: 10.0,
            target_qty_exact: Some(copybot_core_types::TokenQuantity::new(10_000, 3)),
            exit_price_sol: 0.12,
            slippage_bps: Some(25.0),
            dust_qty_epsilon: 0.001,
        },
        now + chrono::Duration::seconds(1),
    )?;
    let open_after = store.load_execution_canary_open_position("TokenMint")?;

    assert_eq!(summary.sell_candidates, 1);
    assert_eq!(summary.sell_execute, 1);
    assert_eq!(summary.sell_closed, 1);
    assert_eq!(summary.sell_partial, 0);
    assert_eq!(summary.sell_no_position, 0);
    assert_eq!(summary.last_closed_qty, 10.0);
    assert!((summary.last_pnl_sol - 0.2).abs() < 1e-9);
    assert_eq!(
        summary.last_sell_decision.as_deref(),
        Some(copybot_storage_core::EXECUTION_CANARY_SELL_DECISION_EXECUTE)
    );
    assert_eq!(
        summary.last_close_status.as_deref(),
        Some(copybot_storage_core::EXECUTION_CANARY_POSITION_CLOSE_CLOSED)
    );
    assert!(open_after.is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

fn timeout_state_machine() -> crate::execution_canary_state_machine::ExecutionCanaryStateMachine<
    crate::execution_submit_adapter::NoSubmitExecutionAdapter,
> {
    crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(
        timeout_canary_config(),
        crate::execution_submit_adapter::NoSubmitExecutionAdapter,
    )
}

fn timeout_canary_config() -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_route = "metis-canary".to_string();
    config.canary_buy_size_sol = 0.01;
    config.quote_canary_sell_slippage_bps = 100;
    config
}

fn mark_submitted_unknown_order(
    store: &SqliteStore,
    signal_id: &str,
    now: chrono::DateTime<Utc>,
) -> Result<String> {
    let order_id = mark_simulated_timeout_order(store, signal_id, now)?;
    store.mark_execution_canary_submitted_unknown(
        &order_id,
        now + chrono::Duration::seconds(2),
        "submit_returned_no_signature",
    )?;
    Ok(order_id)
}

fn mark_submitted_signed_order(
    store: &SqliteStore,
    signal_id: &str,
    now: chrono::DateTime<Utc>,
) -> Result<String> {
    let order_id = mark_simulated_timeout_order(store, signal_id, now)?;
    store.mark_execution_canary_submitted(
        &order_id,
        now + chrono::Duration::seconds(2),
        "tx-sig",
    )?;
    Ok(order_id)
}

fn mark_simulated_timeout_order(
    store: &SqliteStore,
    signal_id: &str,
    now: chrono::DateTime<Utc>,
) -> Result<String> {
    let signal = timeout_signal(signal_id, "buy", now);
    store.insert_copy_signal(&signal)?;
    let reserve = store.reserve_execution_canary_order(&signal.signal_id, "metis-canary", now)?;
    store
        .mark_execution_canary_built(&reserve.order.order_id, now + chrono::Duration::seconds(1))?;
    store.mark_execution_canary_simulated(
        &reserve.order.order_id,
        now + chrono::Duration::seconds(2),
        copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    Ok(reserve.order.order_id)
}

fn timeout_signal(
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

fn unique_timeout_state_machine_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-execution-timeout-state-machine-{name}-{}-{nanos}.db",
        std::process::id()
    ))
}
