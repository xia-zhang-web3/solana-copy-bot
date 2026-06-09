use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{
    CopySignalRow, Lamports, TokenQuantity, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS,
};
use copybot_storage_core::{
    ExecutionCanaryPositionRecordOutcome, SqliteStore, EXECUTION_CANARY_POSITION_CLOSE_CLOSED,
    EXECUTION_CANARY_POSITION_CLOSE_NO_POSITION, EXECUTION_SIMULATION_STATUS_PASSED,
};
use tempfile::tempdir;

fn ts(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .expect("timestamp")
        .with_timezone(&Utc)
}

fn signal(signal_id: &str, side: &str, token: &str, ts: DateTime<Utc>) -> CopySignalRow {
    CopySignalRow {
        signal_id: signal_id.to_string(),
        wallet_id: "leader-wallet".to_string(),
        side: side.to_string(),
        token: token.to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}

fn open_migrated_store(name: &str) -> Result<SqliteStore> {
    let dir = tempdir()?;
    let db_path = dir.keep().join(format!("{name}.db"));
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    Ok(store)
}

fn simulated_order(
    store: &SqliteStore,
    signal_id: &str,
    side: &str,
    now: DateTime<Utc>,
) -> Result<String> {
    store.insert_copy_signal(&signal(signal_id, side, "TokenMint", now))?;
    let reserve = store.reserve_execution_canary_order(signal_id, "metis-canary", now)?;
    store.mark_execution_canary_built(&reserve.order.order_id, now + Duration::seconds(1))?;
    store.mark_execution_canary_simulated(
        &reserve.order.order_id,
        now + Duration::seconds(2),
        EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    Ok(reserve.order.order_id)
}

fn confirmed_order(
    store: &SqliteStore,
    signal_id: &str,
    side: &str,
    now: DateTime<Utc>,
) -> Result<String> {
    let order_id = simulated_order(store, signal_id, side, now)?;
    store.mark_execution_canary_submitted(&order_id, now + Duration::seconds(3), "tx-sig")?;
    store.mark_execution_canary_confirmed(&order_id, now + Duration::seconds(4))?;
    Ok(order_id)
}

#[test]
fn confirmed_buy_fill_opens_position_after_confirmed_submit() -> Result<()> {
    let store = open_migrated_store("execution-canary-confirmed-buy-fill")?;
    let now = ts("2026-06-06T08:00:00Z");
    let order_id = confirmed_order(&store, "buy-confirmed", "buy", now)?;

    let result = store.record_execution_canary_confirmed_buy_fill(
        &order_id,
        "TokenMint",
        1.0,
        Some(TokenQuantity::new(1_000_000, 6)),
        0.2,
        now + Duration::seconds(5),
    )?;
    let position = store
        .load_execution_canary_open_position("TokenMint")?
        .expect("confirmed fill should open a canary-owned position");

    assert_eq!(
        result.outcome,
        ExecutionCanaryPositionRecordOutcome::Inserted
    );
    assert_eq!(
        position.position_id,
        "exec-canary-pos:exec-canary:buy-confirmed"
    );
    assert_eq!(position.cost_lamports, Some(Lamports::new(200_000_000)));
    Ok(())
}

#[test]
fn unconfirmed_buy_fill_is_rejected_without_opening_position() -> Result<()> {
    let store = open_migrated_store("execution-canary-unconfirmed-buy-fill")?;
    let now = ts("2026-06-06T08:00:00Z");
    let order_id = simulated_order(&store, "buy-simulated", "buy", now)?;

    let error = store
        .record_execution_canary_confirmed_buy_fill(
            &order_id,
            "TokenMint",
            1.0,
            Some(TokenQuantity::new(1_000_000, 6)),
            0.2,
            now + Duration::seconds(5),
        )
        .expect_err("unconfirmed buy fill must not open a position");

    assert!(format!("{error:#}").contains("requires confirmed status"));
    assert!(store
        .load_execution_canary_open_position("TokenMint")?
        .is_none());
    Ok(())
}

#[test]
fn confirmed_buy_fill_is_idempotent_per_order() -> Result<()> {
    let store = open_migrated_store("execution-canary-confirmed-buy-idempotent")?;
    let now = ts("2026-06-06T08:00:00Z");
    let order_id = confirmed_order(&store, "buy-idempotent", "buy", now)?;

    let first = store.record_execution_canary_confirmed_buy_fill(
        &order_id,
        "TokenMint",
        1.0,
        Some(TokenQuantity::new(1_000_000, 6)),
        0.2,
        now + Duration::seconds(5),
    )?;
    let second = store.record_execution_canary_confirmed_buy_fill(
        &order_id,
        "TokenMint",
        1.0,
        Some(TokenQuantity::new(1_000_000, 6)),
        0.2,
        now + Duration::seconds(5),
    )?;

    assert_eq!(
        first.outcome,
        ExecutionCanaryPositionRecordOutcome::Inserted
    );
    assert_eq!(
        second.outcome,
        ExecutionCanaryPositionRecordOutcome::Existing
    );
    assert_eq!(store.execution_canary_open_position_count()?, 1);
    Ok(())
}

#[test]
fn confirmed_buy_fill_merges_same_token_new_order() -> Result<()> {
    let store = open_migrated_store("execution-canary-confirmed-buy-merge")?;
    let now = ts("2026-06-06T08:00:00Z");
    let first_order_id = confirmed_order(&store, "buy-merge-a", "buy", now)?;
    let second_order_id =
        confirmed_order(&store, "buy-merge-b", "buy", now + Duration::seconds(5))?;

    let first = store.record_execution_canary_confirmed_buy_fill(
        &first_order_id,
        "TokenMint",
        1.0,
        Some(TokenQuantity::new(1_000_000, 6)),
        0.2,
        now + Duration::seconds(10),
    )?;
    let second = store.record_execution_canary_confirmed_buy_fill(
        &second_order_id,
        "TokenMint",
        1.5,
        Some(TokenQuantity::new(1_500_000, 6)),
        0.3,
        now + Duration::seconds(15),
    )?;
    let second_repeat = store.record_execution_canary_confirmed_buy_fill(
        &second_order_id,
        "TokenMint",
        1.5,
        Some(TokenQuantity::new(1_500_000, 6)),
        0.3,
        now + Duration::seconds(15),
    )?;
    let position = store
        .load_execution_canary_open_position("TokenMint")?
        .expect("merged confirmed fill should keep one open position");

    assert_eq!(
        first.outcome,
        ExecutionCanaryPositionRecordOutcome::Inserted
    );
    assert_eq!(second.outcome, ExecutionCanaryPositionRecordOutcome::Merged);
    assert_eq!(
        second_repeat.outcome,
        ExecutionCanaryPositionRecordOutcome::Existing
    );
    assert_eq!(
        position.position_id,
        "exec-canary-pos:exec-canary:buy-merge-a"
    );
    assert!((position.qty - 2.5).abs() < 1e-9);
    assert_eq!(position.qty_exact, Some(TokenQuantity::new(2_500_000, 6)));
    assert_eq!(position.cost_lamports, Some(Lamports::new(500_000_000)));
    Ok(())
}

#[test]
fn confirmed_sell_fill_closes_canary_owned_position() -> Result<()> {
    let store = open_migrated_store("execution-canary-confirmed-sell-fill")?;
    let now = ts("2026-06-06T08:00:00Z");
    let buy_order_id = confirmed_order(&store, "buy-owned", "buy", now)?;
    store.record_execution_canary_confirmed_buy_fill(
        &buy_order_id,
        "TokenMint",
        1.0,
        Some(TokenQuantity::new(1_000_000, 6)),
        0.2,
        now + Duration::seconds(5),
    )?;
    let sell_order_id = confirmed_order(&store, "sell-owned", "sell", now + Duration::minutes(1))?;

    let result = store.close_execution_canary_confirmed_sell_fill(
        &sell_order_id,
        "TokenMint",
        1.0,
        Some(TokenQuantity::new(1_000_000, 6)),
        0.3,
        0.001,
        now + Duration::minutes(1) + Duration::seconds(5),
    )?;

    assert_eq!(result.close_status, EXECUTION_CANARY_POSITION_CLOSE_CLOSED);
    assert!((result.pnl_sol - 0.1).abs() < 1e-9);
    assert!(store
        .load_execution_canary_open_position("TokenMint")?
        .is_none());
    Ok(())
}

#[test]
fn unconfirmed_sell_fill_is_rejected_without_closing_position() -> Result<()> {
    let store = open_migrated_store("execution-canary-unconfirmed-sell-fill")?;
    let now = ts("2026-06-06T08:00:00Z");
    let buy_order_id = confirmed_order(&store, "buy-kept", "buy", now)?;
    store.record_execution_canary_confirmed_buy_fill(
        &buy_order_id,
        "TokenMint",
        1.0,
        Some(TokenQuantity::new(1_000_000, 6)),
        0.2,
        now + Duration::seconds(5),
    )?;
    let sell_order_id =
        simulated_order(&store, "sell-simulated", "sell", now + Duration::minutes(1))?;

    let error = store
        .close_execution_canary_confirmed_sell_fill(
            &sell_order_id,
            "TokenMint",
            1.0,
            Some(TokenQuantity::new(1_000_000, 6)),
            0.3,
            0.001,
            now + Duration::minutes(1) + Duration::seconds(5),
        )
        .expect_err("unconfirmed sell fill must not close a position");

    assert!(format!("{error:#}").contains("requires confirmed status"));
    assert!(store
        .load_execution_canary_open_position("TokenMint")?
        .is_some());
    Ok(())
}

#[test]
fn confirmed_sell_fill_without_owned_position_reports_no_position() -> Result<()> {
    let store = open_migrated_store("execution-canary-sell-fill-no-position")?;
    let now = ts("2026-06-06T08:00:00Z");
    let sell_order_id = confirmed_order(&store, "sell-no-position", "sell", now)?;

    let result = store.close_execution_canary_confirmed_sell_fill(
        &sell_order_id,
        "TokenMint",
        1.0,
        Some(TokenQuantity::new(1_000_000, 6)),
        0.3,
        0.001,
        now + Duration::seconds(5),
    )?;

    assert_eq!(
        result.close_status,
        EXECUTION_CANARY_POSITION_CLOSE_NO_POSITION
    );
    assert_eq!(result.pnl_sol, 0.0);
    Ok(())
}
