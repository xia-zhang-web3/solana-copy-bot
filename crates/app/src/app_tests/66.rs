use super::*;
use copybot_core_types::{
    CopySignalRow, Lamports, TokenQuantity, COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE,
};

#[tokio::test]
async fn terminal_failed_sell_simulation_sweep_writes_off_open_position() -> Result<()> {
    let db_path = unique_terminal_write_off_test_path("terminal-failed-sell");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let token = "TokenMint";
    store.record_execution_canary_open_position(
        "existing-confirmed-buy",
        token,
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        0.01,
        now,
    )?;
    let signal = terminal_failed_sell_signal(token, now + chrono::Duration::minutes(1));
    store.insert_copy_signal(&signal)?;
    let order_id = mark_terminal_failed_sell_order(
        &store,
        &signal.signal_id,
        now + chrono::Duration::minutes(1),
        3,
    )?;
    let mut config = ExecutionConfig::default();
    config.canary_route =
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN.to_string();
    config.canary_tiny_submit_enabled = true;
    config.max_submit_attempts = 3;

    let summary = crate::execution_canary_route::process_failed_sell_simulation_sweep(
        &config,
        &store,
        now + chrono::Duration::minutes(2),
    )
    .await?
    .expect("terminal failed sell simulation should be swept");
    let order = store
        .load_execution_canary_order(&order_id)?
        .expect("failed sell order should remain recorded");

    assert_eq!(summary.existing, 1);
    assert_eq!(summary.sell_closed, 1);
    assert_eq!(summary.last_pnl_sol, -0.01);
    assert_eq!(
        summary.skipped_reason,
        Some("terminal_failed_sell_simulation_write_off")
    );
    assert_eq!(order.attempt, 3);
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED
    );
    assert!(store.load_execution_canary_open_position(token)?.is_none());
    assert_eq!(
        store.execution_canary_realized_loss_sol_since(now - chrono::Duration::hours(1))?,
        0.01
    );

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

fn mark_terminal_failed_sell_order(
    store: &SqliteStore,
    signal_id: &str,
    now: chrono::DateTime<Utc>,
    terminal_attempt: u32,
) -> Result<String> {
    let reserve = store.reserve_execution_canary_order(
        signal_id,
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN,
        now,
    )?;
    for attempt in 1..=terminal_attempt {
        store.mark_execution_canary_built(
            &reserve.order.order_id,
            now + chrono::Duration::seconds(i64::from(attempt * 10 + 1)),
        )?;
        store.mark_execution_canary_simulated(
            &reserve.order.order_id,
            now + chrono::Duration::seconds(i64::from(attempt * 10 + 2)),
            copybot_storage_core::EXECUTION_SIMULATION_STATUS_FAILED,
            Some("simulation failed"),
        )?;
        store.mark_execution_canary_failed(
            &reserve.order.order_id,
            now + chrono::Duration::seconds(i64::from(attempt * 10 + 3)),
            copybot_storage_core::EXECUTION_ERROR_SIMULATION_FAILED,
            "simulation failed",
        )?;
        if attempt < terminal_attempt {
            store.mark_execution_canary_failed_simulation_retry_candidate(
                &reserve.order.order_id,
                now + chrono::Duration::seconds(i64::from(attempt * 10 + 4)),
                "retry_failed_sell_with_owned_position_amount",
            )?;
        }
    }
    Ok(reserve.order.order_id)
}

fn terminal_failed_sell_signal(token: &str, ts: chrono::DateTime<Utc>) -> CopySignalRow {
    CopySignalRow {
        signal_id: format!("shadow:sig-terminal-failed-sell:leader:{token}"),
        wallet_id: "leader".to_string(),
        side: "sell".to_string(),
        token: token.to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}

fn unique_terminal_write_off_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-terminal-write-off-{name}-{}-{nanos}",
        std::process::id()
    ))
}
