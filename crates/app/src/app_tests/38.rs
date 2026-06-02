use super::*;

#[tokio::test]
async fn execution_canary_state_machine_sell_skips_without_owned_position() -> Result<()> {
    let db_path = unique_sell_state_machine_test_path("no-position");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = sell_signal("sell-no-position", now);
    store.insert_copy_signal(&signal)?;
    let state_machine = sell_state_machine();

    let summary = state_machine.process_sell_candidate(
        &store,
        &signal,
        sell_close_input(
            1.0,
            Some(copybot_core_types::TokenQuantity::new(1_000, 3)),
            0.1,
            0.0,
        ),
        now,
    )?;

    assert_eq!(summary.sell_candidates, 1);
    assert_eq!(summary.sell_no_position, 1);
    assert_eq!(summary.sell_closed, 0);
    assert_eq!(summary.skipped_reason, Some("no_owned_position"));
    assert_eq!(
        summary.last_sell_decision.as_deref(),
        Some(copybot_storage_core::EXECUTION_CANARY_SELL_DECISION_NO_POSITION)
    );
    assert!(summary.last_close_status.is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn execution_canary_state_machine_sell_closes_owned_position() -> Result<()> {
    let db_path = unique_sell_state_machine_test_path("full-close");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = sell_signal("sell-full", now);
    store.insert_copy_signal(&signal)?;
    record_open_canary_position(&store, now)?;
    let state_machine = sell_state_machine();

    let summary = state_machine.process_sell_candidate(
        &store,
        &signal,
        sell_close_input(
            10.0,
            Some(copybot_core_types::TokenQuantity::new(10_000, 3)),
            0.12,
            50.0,
        ),
        now,
    )?;
    let open_after = store.load_execution_canary_open_position("TokenMint")?;

    assert_eq!(summary.sell_execute, 1);
    assert_eq!(summary.sell_force_exit, 0);
    assert_eq!(summary.sell_closed, 1);
    assert_eq!(summary.sell_partial, 0);
    assert_eq!(summary.last_closed_qty, 10.0);
    assert!((summary.last_pnl_sol - 0.2).abs() < 1e-9);
    assert_eq!(
        summary.last_close_status.as_deref(),
        Some(copybot_storage_core::EXECUTION_CANARY_POSITION_CLOSE_CLOSED)
    );
    assert!(open_after.is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn execution_canary_state_machine_sell_force_exits_owned_position() -> Result<()> {
    let db_path = unique_sell_state_machine_test_path("force-exit");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = sell_signal("sell-force-exit", now);
    store.insert_copy_signal(&signal)?;
    record_open_canary_position(&store, now)?;
    let state_machine = sell_state_machine();

    let summary = state_machine.process_sell_candidate(
        &store,
        &signal,
        sell_close_input(
            10.0,
            Some(copybot_core_types::TokenQuantity::new(10_000, 3)),
            0.09,
            250.0,
        ),
        now,
    )?;

    assert_eq!(summary.sell_execute, 0);
    assert_eq!(summary.sell_force_exit, 1);
    assert_eq!(summary.sell_closed, 1);
    assert!((summary.last_pnl_sol + 0.1).abs() < 1e-9);
    assert_eq!(
        summary.last_sell_decision.as_deref(),
        Some(copybot_storage_core::EXECUTION_CANARY_SELL_DECISION_FORCE_EXIT)
    );

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn execution_canary_state_machine_sell_partial_close_keeps_remaining_position() -> Result<()>
{
    let db_path = unique_sell_state_machine_test_path("partial");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = sell_signal("sell-partial", now);
    store.insert_copy_signal(&signal)?;
    record_open_canary_position(&store, now)?;
    let state_machine = sell_state_machine();

    let summary = state_machine.process_sell_candidate(
        &store,
        &signal,
        sell_close_input(
            4.0,
            Some(copybot_core_types::TokenQuantity::new(4_000, 3)),
            0.125,
            0.0,
        ),
        now,
    )?;
    let remaining = store
        .load_execution_canary_open_position("TokenMint")?
        .expect("partial close should keep open position");

    assert_eq!(summary.sell_execute, 1);
    assert_eq!(summary.sell_closed, 1);
    assert_eq!(summary.sell_partial, 1);
    assert_eq!(
        summary.last_close_status.as_deref(),
        Some(copybot_storage_core::EXECUTION_CANARY_POSITION_CLOSE_PARTIAL)
    );
    assert_eq!(summary.last_closed_qty, 4.0);
    assert!((summary.last_pnl_sol - 0.1).abs() < 1e-9);
    assert!((remaining.qty - 6.0).abs() < 1e-9);
    assert_eq!(
        remaining.qty_exact,
        Some(copybot_core_types::TokenQuantity::new(6_000, 3))
    );

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

fn sell_state_machine() -> crate::execution_canary_state_machine::ExecutionCanaryStateMachine<
    crate::execution_submit_adapter::NoSubmitExecutionAdapter,
> {
    crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(
        sell_canary_config(),
        crate::execution_submit_adapter::NoSubmitExecutionAdapter,
    )
}

fn sell_canary_config() -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_route = "metis-canary".to_string();
    config.canary_buy_size_sol = 0.01;
    config.quote_canary_sell_slippage_bps = 100;
    config
}

fn sell_signal(signal_id: &str, ts: chrono::DateTime<Utc>) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: format!("shadow:sig-state-machine:leader-wallet:{signal_id}:TokenMint"),
        wallet_id: "leader-wallet".to_string(),
        side: "sell".to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}

fn sell_close_input(
    target_qty: f64,
    target_qty_exact: Option<copybot_core_types::TokenQuantity>,
    exit_price_sol: f64,
    slippage_bps: f64,
) -> crate::execution_canary_state_machine::ExecutionCanarySellCloseInput {
    crate::execution_canary_state_machine::ExecutionCanarySellCloseInput {
        target_qty,
        target_qty_exact,
        exit_price_sol,
        slippage_bps: Some(slippage_bps),
        dust_qty_epsilon: 0.001,
    }
}

fn record_open_canary_position(store: &SqliteStore, now: chrono::DateTime<Utc>) -> Result<()> {
    store.record_execution_canary_open_position(
        "exec-canary:buy-for-sell",
        "TokenMint",
        10.0,
        Some(copybot_core_types::TokenQuantity::new(10_000, 3)),
        1.0,
        now,
    )?;
    Ok(())
}

fn unique_sell_state_machine_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-execution-sell-state-machine-{name}-{}-{nanos}.db",
        std::process::id()
    ))
}
