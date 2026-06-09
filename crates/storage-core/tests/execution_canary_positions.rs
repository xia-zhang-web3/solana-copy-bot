use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_core_types::{Lamports, TokenQuantity};
use copybot_storage_core::{
    ExecutionCanaryPositionRecordOutcome, SqliteStore, EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
    EXECUTION_CANARY_SELL_DECISION_EXECUTE, EXECUTION_CANARY_SELL_DECISION_FORCE_EXIT,
    EXECUTION_CANARY_SELL_DECISION_NO_POSITION,
};
use tempfile::tempdir;

fn ts(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .expect("timestamp")
        .with_timezone(&Utc)
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

#[test]
fn execution_canary_open_position_recording_is_idempotent() -> Result<()> {
    let store = open_migrated_store("execution-canary-owned-position")?;
    let now = ts("2026-06-01T08:00:00Z");
    let qty_exact = TokenQuantity::new(1_234_567, 6);

    let first = store.record_execution_canary_open_position(
        "exec-canary:buy-1",
        "TokenMint",
        1.234567,
        Some(qty_exact),
        0.2,
        now,
    )?;
    let second = store.record_execution_canary_open_position(
        "exec-canary:buy-1",
        "TokenMint",
        1.234567,
        Some(qty_exact),
        0.2,
        now,
    )?;
    let loaded = store
        .load_execution_canary_open_position("TokenMint")?
        .expect("execution canary position should load");

    assert_eq!(
        first.outcome,
        ExecutionCanaryPositionRecordOutcome::Inserted
    );
    assert_eq!(
        second.outcome,
        ExecutionCanaryPositionRecordOutcome::Existing
    );
    assert_eq!(loaded.position_id, "exec-canary-pos:exec-canary:buy-1");
    assert_eq!(loaded.qty_exact, Some(qty_exact));
    assert_eq!(loaded.cost_lamports, Some(Lamports::new(200_000_000)));
    assert_eq!(
        first.position.cost_lamports,
        Some(Lamports::new(200_000_000))
    );
    assert_eq!(
        loaded.accounting_bucket,
        EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET
    );
    assert_eq!(loaded.state, "open");
    Ok(())
}

#[test]
fn execution_canary_open_position_recording_merges_same_token_new_order() -> Result<()> {
    let store = open_migrated_store("execution-canary-owned-position-merge")?;
    let now = ts("2026-06-01T08:00:00Z");

    let first = store.record_execution_canary_open_position(
        "exec-canary:buy-merge-1",
        "TokenMint",
        1.234567,
        Some(TokenQuantity::new(1_234_567, 6)),
        0.2,
        now,
    )?;
    let second = store.record_execution_canary_open_position(
        "exec-canary:buy-merge-2",
        "TokenMint",
        2.0,
        Some(TokenQuantity::new(2_000_000, 6)),
        0.3,
        now + chrono::Duration::seconds(10),
    )?;
    let loaded = store
        .load_execution_canary_open_position("TokenMint")?
        .expect("merged execution canary position should load");

    assert_eq!(
        first.outcome,
        ExecutionCanaryPositionRecordOutcome::Inserted
    );
    assert_eq!(second.outcome, ExecutionCanaryPositionRecordOutcome::Merged);
    assert_eq!(
        second.position.position_id,
        "exec-canary-pos:exec-canary:buy-merge-1"
    );
    assert!((loaded.qty - 3.234567).abs() < 1e-9);
    assert_eq!(loaded.qty_exact, Some(TokenQuantity::new(3_234_567, 6)));
    assert_eq!(loaded.cost_lamports, Some(Lamports::new(500_000_000)));
    assert_eq!(store.execution_canary_open_position_count()?, 1);
    Ok(())
}

#[test]
fn execution_canary_sell_decision_skips_when_no_owned_position() -> Result<()> {
    let store = open_migrated_store("execution-canary-sell-no-position")?;

    let decision = store.execution_canary_sell_decision("TokenMint", Some(12.0), 100.0)?;

    assert_eq!(
        decision.decision_status,
        EXECUTION_CANARY_SELL_DECISION_NO_POSITION
    );
    assert_eq!(decision.decision_reason, "no_owned_position");
    assert!(decision.position.is_none());
    Ok(())
}

#[test]
fn execution_canary_sell_decision_executes_inside_soft_slippage_limit() -> Result<()> {
    let store = open_migrated_store("execution-canary-sell-execute")?;
    record_open_position(&store)?;

    let decision = store.execution_canary_sell_decision("TokenMint", Some(99.0), 100.0)?;

    assert_eq!(
        decision.decision_status,
        EXECUTION_CANARY_SELL_DECISION_EXECUTE
    );
    assert_eq!(decision.decision_reason, "owned_position");
    assert!(decision.position.is_some());
    Ok(())
}

#[test]
fn execution_canary_sell_decision_executes_negative_slippage_improvement() -> Result<()> {
    let store = open_migrated_store("execution-canary-sell-negative-slippage")?;
    record_open_position(&store)?;

    let decision = store.execution_canary_sell_decision("TokenMint", Some(-3457.45), 100.0)?;

    assert_eq!(
        decision.decision_status,
        EXECUTION_CANARY_SELL_DECISION_EXECUTE
    );
    assert_eq!(decision.decision_reason, "owned_position");
    assert_eq!(decision.slippage_bps, Some(-3457.45));
    assert!(decision.position.is_some());
    Ok(())
}

#[test]
fn execution_canary_sell_decision_force_exits_owned_position_above_soft_limit() -> Result<()> {
    let store = open_migrated_store("execution-canary-sell-force-exit")?;
    record_open_position(&store)?;

    let decision = store.execution_canary_sell_decision("TokenMint", Some(109.57), 100.0)?;

    assert_eq!(
        decision.decision_status,
        EXECUTION_CANARY_SELL_DECISION_FORCE_EXIT
    );
    assert_eq!(decision.decision_reason, "sell_slippage_above_soft_limit");
    assert_eq!(decision.slippage_bps, Some(109.57));
    assert!(decision.position.is_some());
    Ok(())
}

#[test]
fn execution_canary_position_count_tracks_open_only() -> Result<()> {
    let store = open_migrated_store("execution-canary-open-count")?;
    let now = ts("2026-06-01T08:00:00Z");
    store.record_execution_canary_open_position(
        "exec-canary:buy-count-a",
        "TokenMintA",
        1.0,
        Some(TokenQuantity::new(1_000, 3)),
        0.2,
        now,
    )?;
    store.record_execution_canary_open_position(
        "exec-canary:buy-count-b",
        "TokenMintB",
        1.0,
        Some(TokenQuantity::new(1_000, 3)),
        0.2,
        now,
    )?;

    let before_close = store.execution_canary_open_position_count()?;
    store.close_execution_canary_open_position(
        "TokenMintA",
        1.0,
        Some(TokenQuantity::new(1_000, 3)),
        0.3,
        0.001,
        now + chrono::Duration::seconds(1),
    )?;
    let after_close = store.execution_canary_open_position_count()?;

    assert_eq!(before_close, 2);
    assert_eq!(after_close, 1);
    Ok(())
}

#[test]
fn execution_canary_realized_loss_sums_closed_losses_since_cutoff() -> Result<()> {
    let store = open_migrated_store("execution-canary-daily-loss")?;
    let now = ts("2026-06-01T08:00:00Z");
    record_open_position_for_token(&store, "exec-canary:loss", "LossMint", now)?;
    record_open_position_for_token(&store, "exec-canary:profit", "ProfitMint", now)?;
    record_open_position_for_token(&store, "exec-canary:old-loss", "OldLossMint", now)?;
    store.close_execution_canary_open_position(
        "LossMint",
        1.0,
        Some(TokenQuantity::new(1_000_000, 6)),
        0.15,
        0.001,
        now + chrono::Duration::seconds(1),
    )?;
    store.close_execution_canary_open_position(
        "ProfitMint",
        1.0,
        Some(TokenQuantity::new(1_000_000, 6)),
        0.25,
        0.001,
        now + chrono::Duration::seconds(2),
    )?;
    store.close_execution_canary_open_position(
        "OldLossMint",
        1.0,
        Some(TokenQuantity::new(1_000_000, 6)),
        0.1,
        0.001,
        ts("2026-05-31T23:59:00Z"),
    )?;

    let today_loss = store.execution_canary_realized_loss_sol_since(ts("2026-06-01T00:00:00Z"))?;
    let later_loss = store.execution_canary_realized_loss_sol_since(ts("2026-06-01T08:00:02Z"))?;

    assert!((today_loss - 0.05).abs() < 1e-9);
    assert_eq!(later_loss, 0.0);
    Ok(())
}

fn record_open_position(store: &SqliteStore) -> Result<()> {
    record_open_position_for_token(
        store,
        "exec-canary:buy-1",
        "TokenMint",
        ts("2026-06-01T08:00:00Z"),
    )
}

fn record_open_position_for_token(
    store: &SqliteStore,
    order_id: &str,
    token: &str,
    now: DateTime<Utc>,
) -> Result<()> {
    store.record_execution_canary_open_position(
        order_id,
        token,
        1.0,
        Some(TokenQuantity::new(1_000_000, 6)),
        0.2,
        now,
    )?;
    Ok(())
}
