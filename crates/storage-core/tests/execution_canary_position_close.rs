use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_core_types::{Lamports, TokenQuantity};
use copybot_storage_core::{
    SqliteStore, EXECUTION_CANARY_POSITION_CLOSE_CLOSED,
    EXECUTION_CANARY_POSITION_CLOSE_DUST_CLOSED, EXECUTION_CANARY_POSITION_CLOSE_NO_POSITION,
    EXECUTION_CANARY_POSITION_CLOSE_PARTIAL,
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
fn execution_canary_position_close_partial_preserves_exact_remaining() -> Result<()> {
    let store = open_migrated_store("execution-canary-close-partial")?;
    record_open_position(&store)?;

    let result = store.close_execution_canary_open_position(
        "TokenMint",
        4.0,
        Some(TokenQuantity::new(4_000, 3)),
        0.125,
        0.001,
        ts("2026-06-01T09:00:00Z"),
    )?;
    let remaining = result
        .remaining_position
        .as_ref()
        .expect("partial close should keep an open position");

    assert_eq!(result.close_status, EXECUTION_CANARY_POSITION_CLOSE_PARTIAL);
    assert_close(result.closed_qty, 4.0);
    assert_eq!(result.closed_qty_exact, Some(TokenQuantity::new(4_000, 3)));
    assert_close(result.remaining_qty, 6.0);
    assert_eq!(
        result.remaining_qty_exact,
        Some(TokenQuantity::new(6_000, 3))
    );
    assert_close(result.entry_cost_sol, 0.4);
    assert_close(result.exit_value_sol, 0.5);
    assert_close(result.pnl_sol, 0.1);
    assert_eq!(result.entry_cost_lamports, Some(Lamports::new(400_000_000)));
    assert_eq!(result.exit_value_lamports, Some(Lamports::new(500_000_000)));
    assert_eq!(
        result.pnl_lamports.map(|value| value.as_i128()),
        Some(100_000_000)
    );
    assert_close(remaining.qty, 6.0);
    assert_eq!(remaining.qty_exact, Some(TokenQuantity::new(6_000, 3)));
    assert_eq!(remaining.cost_lamports, Some(Lamports::new(600_000_000)));
    Ok(())
}

#[test]
fn execution_canary_position_close_full_closes_owned_position() -> Result<()> {
    let store = open_migrated_store("execution-canary-close-full")?;
    record_open_position(&store)?;

    let result = store.close_execution_canary_open_position(
        "TokenMint",
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        0.12,
        0.001,
        ts("2026-06-01T09:00:00Z"),
    )?;
    let open_after = store.load_execution_canary_open_position("TokenMint")?;

    assert_eq!(result.close_status, EXECUTION_CANARY_POSITION_CLOSE_CLOSED);
    assert_close(result.closed_qty, 10.0);
    assert_close(result.remaining_qty, 0.0);
    assert_close(result.entry_cost_sol, 1.0);
    assert_close(result.exit_value_sol, 1.2);
    assert_close(result.pnl_sol, 0.2);
    assert_eq!(
        result.pnl_lamports.map(|value| value.as_i128()),
        Some(200_000_000)
    );
    assert!(open_after.is_none());
    Ok(())
}

#[test]
fn execution_canary_position_close_dust_closes_remaining_tail() -> Result<()> {
    let store = open_migrated_store("execution-canary-close-dust")?;
    record_open_position(&store)?;

    let result = store.close_execution_canary_open_position(
        "TokenMint",
        9.995,
        Some(TokenQuantity::new(9_995, 3)),
        0.1,
        0.01,
        ts("2026-06-01T09:00:00Z"),
    )?;
    let open_after = store.load_execution_canary_open_position("TokenMint")?;

    assert_eq!(
        result.close_status,
        EXECUTION_CANARY_POSITION_CLOSE_DUST_CLOSED
    );
    assert_close(result.closed_qty, 10.0);
    assert_eq!(result.closed_qty_exact, Some(TokenQuantity::new(10_000, 3)));
    assert_close(result.remaining_qty, 0.0);
    assert_close(result.pnl_sol, 0.0);
    assert!(open_after.is_none());
    Ok(())
}

#[test]
fn execution_canary_position_close_raw_unit_tail_is_dust() -> Result<()> {
    let store = open_migrated_store("execution-canary-close-raw-dust")?;
    store.record_execution_canary_open_position(
        "exec-canary:buy-raw-dust",
        "RawDustMint",
        10.000001,
        Some(TokenQuantity::new(10_000_001, 6)),
        1.0,
        ts("2026-06-01T08:00:00Z"),
    )?;

    let result = store.close_execution_canary_open_position(
        "RawDustMint",
        10.0,
        Some(TokenQuantity::new(10_000_000, 6)),
        0.1,
        1e-12,
        ts("2026-06-01T09:00:00Z"),
    )?;
    let open_after = store.load_execution_canary_open_position("RawDustMint")?;

    assert_eq!(
        result.close_status,
        EXECUTION_CANARY_POSITION_CLOSE_DUST_CLOSED
    );
    assert_close(result.closed_qty, 10.000001);
    assert_eq!(
        result.closed_qty_exact,
        Some(TokenQuantity::new(10_000_001, 6))
    );
    assert_close(result.remaining_qty, 0.0);
    assert!(open_after.is_none());
    Ok(())
}

#[test]
fn execution_canary_position_close_reports_no_position() -> Result<()> {
    let store = open_migrated_store("execution-canary-close-none")?;

    let result = store.close_execution_canary_open_position(
        "TokenMint",
        1.0,
        Some(TokenQuantity::new(1_000, 3)),
        0.1,
        0.01,
        ts("2026-06-01T09:00:00Z"),
    )?;

    assert_eq!(
        result.close_status,
        EXECUTION_CANARY_POSITION_CLOSE_NO_POSITION
    );
    assert!(result.position_id.is_none());
    assert_close(result.closed_qty, 0.0);
    Ok(())
}

fn record_open_position(store: &SqliteStore) -> Result<()> {
    store.record_execution_canary_open_position(
        "exec-canary:buy-1",
        "TokenMint",
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        1.0,
        ts("2026-06-01T08:00:00Z"),
    )?;
    Ok(())
}

fn assert_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 1e-9,
        "actual={actual} expected={expected}"
    );
}
