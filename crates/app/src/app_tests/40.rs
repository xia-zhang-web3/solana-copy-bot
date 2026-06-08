use super::*;

#[tokio::test]
async fn execution_canary_state_machine_kill_switch_blocks_buy_before_reserve() -> Result<()> {
    let db_path = unique_safety_state_machine_test_path("kill-switch");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = safety_signal("buy-kill-switch", now);
    store.insert_copy_signal(&signal)?;
    let kill_switch_path = unique_safety_state_machine_test_path("kill-switch-file");
    std::fs::write(&kill_switch_path, b"stop")?;
    let state_machine = safety_state_machine_with_kill_switch(kill_switch_path.clone());

    let summary = state_machine
        .process_buy_candidate(&store, &signal, now)
        .await?;

    assert_eq!(summary.candidates, 1);
    assert_eq!(summary.safety_blocked, 1);
    assert_eq!(summary.skipped_reason, Some("kill_switch_active"));
    assert_eq!(summary.reserved, 0);
    assert!(store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .is_none());

    let _ = std::fs::remove_file(kill_switch_path);
    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn execution_canary_state_machine_max_open_positions_blocks_buy_before_reserve() -> Result<()>
{
    let db_path = unique_safety_state_machine_test_path("max-open");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = safety_signal("buy-max-open", now);
    store.insert_copy_signal(&signal)?;
    store.record_execution_canary_open_position(
        "exec-canary:safety-open",
        "OpenTokenMint",
        1.0,
        Some(copybot_core_types::TokenQuantity::new(1_000, 3)),
        0.01,
        now,
    )?;
    let state_machine = safety_state_machine();

    let summary = state_machine
        .process_buy_candidate(&store, &signal, now)
        .await?;

    assert_eq!(summary.safety_blocked, 1);
    assert_eq!(summary.open_positions, 1);
    assert_eq!(summary.skipped_reason, Some("max_open_positions"));
    assert_eq!(summary.reserved, 0);
    assert!(store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn execution_canary_state_machine_stale_open_position_does_not_block_buy() -> Result<()> {
    let db_path = unique_safety_state_machine_test_path("stale-open");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = safety_signal("buy-stale-open", now);
    store.insert_copy_signal(&signal)?;
    store.record_execution_canary_open_position(
        "exec-canary:safety-stale-open",
        "StaleOpenTokenMint",
        1.0,
        Some(copybot_core_types::TokenQuantity::new(1_000, 3)),
        0.01,
        now - chrono::Duration::minutes(16),
    )?;
    let state_machine = safety_state_machine();

    let summary = state_machine
        .process_buy_candidate(&store, &signal, now)
        .await?;

    assert_eq!(summary.safety_blocked, 0);
    assert_eq!(summary.entry_gate_blocked, 1);
    assert_eq!(summary.open_positions, 0);
    assert!((summary.daily_loss_sol - 0.01).abs() < 1e-9);
    assert_eq!(summary.skipped_reason, Some("missing_quote_metadata"));
    assert_eq!(store.execution_canary_open_position_count()?, 0);

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn execution_canary_state_machine_daily_loss_blocks_buy_before_reserve() -> Result<()> {
    let db_path = unique_safety_state_machine_test_path("daily-loss");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = safety_signal("buy-daily-loss", now);
    store.insert_copy_signal(&signal)?;
    record_closed_canary_loss(&store, now)?;
    let state_machine = safety_state_machine();

    let summary = state_machine
        .process_buy_candidate(&store, &signal, now)
        .await?;

    assert_eq!(summary.safety_blocked, 1);
    assert_eq!(summary.open_positions, 0);
    assert!(summary.daily_loss_sol >= 0.02);
    assert_eq!(summary.skipped_reason, Some("max_daily_loss"));
    assert_eq!(summary.reserved, 0);
    assert!(store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

fn safety_state_machine() -> crate::execution_canary_state_machine::ExecutionCanaryStateMachine<
    crate::execution_submit_adapter::NoSubmitExecutionAdapter,
> {
    safety_state_machine_with_kill_switch(unique_safety_state_machine_test_path("missing-stop"))
}

fn safety_state_machine_with_kill_switch(
    kill_switch_path: PathBuf,
) -> crate::execution_canary_state_machine::ExecutionCanaryStateMachine<
    crate::execution_submit_adapter::NoSubmitExecutionAdapter,
> {
    crate::execution_canary_state_machine::ExecutionCanaryStateMachine::new(
        safety_canary_config(kill_switch_path),
        crate::execution_submit_adapter::NoSubmitExecutionAdapter,
    )
}

fn safety_canary_config(kill_switch_path: PathBuf) -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_route = "metis-canary".to_string();
    config.canary_buy_size_sol = 0.01;
    config.canary_max_open_positions = 1;
    config.canary_max_daily_loss_sol = 0.02;
    config.canary_kill_switch_path = kill_switch_path.to_string_lossy().to_string();
    config
}

fn safety_signal(signal_id: &str, ts: chrono::DateTime<Utc>) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: format!("shadow:sig-state-machine:leader-wallet:{signal_id}:TokenMint"),
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

fn record_closed_canary_loss(store: &SqliteStore, now: chrono::DateTime<Utc>) -> Result<()> {
    store.record_execution_canary_open_position(
        "exec-canary:safety-loss",
        "LossTokenMint",
        10.0,
        Some(copybot_core_types::TokenQuantity::new(10_000, 3)),
        1.0,
        now,
    )?;
    store.close_execution_canary_open_position(
        "LossTokenMint",
        10.0,
        Some(copybot_core_types::TokenQuantity::new(10_000, 3)),
        0.097,
        0.001,
        now + chrono::Duration::seconds(1),
    )?;
    Ok(())
}

fn unique_safety_state_machine_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-execution-safety-state-machine-{name}-{}-{nanos}.db",
        std::process::id()
    ))
}
