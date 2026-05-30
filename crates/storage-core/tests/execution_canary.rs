use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{CopySignalRow, Lamports, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS};
use copybot_storage_core::{
    ExecutionDryRunRecordOutcome, SqliteStore, EXECUTION_SIMULATION_STATUS_DRY_RUN_SKIPPED,
    EXECUTION_STATUS_DRY_RUN_CONFIRMED,
};
use tempfile::tempdir;

fn ts(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .expect("timestamp")
        .with_timezone(&Utc)
}

fn signal(signal_id: &str, side: &str, ts: DateTime<Utc>) -> CopySignalRow {
    CopySignalRow {
        signal_id: signal_id.to_string(),
        wallet_id: "leader-wallet".to_string(),
        side: side.to_string(),
        token: "TokenMint".to_string(),
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

#[test]
fn execution_canary_candidates_skip_signals_that_already_have_orders() -> Result<()> {
    let store = open_migrated_store("execution-canary-candidates")?;
    let now = ts("2026-05-30T08:00:00Z");
    store.insert_copy_signal(&signal("buy-old", "buy", now - Duration::seconds(20)))?;
    store.insert_copy_signal(&signal("sell-new", "sell", now - Duration::seconds(10)))?;
    store.record_execution_dry_run_order("sell-new", "dry_run", now)?;

    let candidates = store.list_execution_canary_candidates(
        "shadow_recorded",
        now - Duration::minutes(1),
        10,
    )?;

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].signal_id, "buy-old");
    Ok(())
}

#[test]
fn execution_canary_candidates_ignore_stale_signals() -> Result<()> {
    let store = open_migrated_store("execution-canary-freshness")?;
    let now = ts("2026-05-30T08:00:00Z");
    store.insert_copy_signal(&signal("buy-old", "buy", now - Duration::minutes(10)))?;
    store.insert_copy_signal(&signal("buy-fresh", "buy", now - Duration::seconds(20)))?;

    let candidates = store.list_execution_canary_candidates(
        "shadow_recorded",
        now - Duration::minutes(1),
        10,
    )?;

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].signal_id, "buy-fresh");
    Ok(())
}

#[test]
fn execution_dry_run_order_recording_is_idempotent_per_signal() -> Result<()> {
    let store = open_migrated_store("execution-canary-idempotency")?;
    let now = ts("2026-05-30T08:00:00Z");
    store.insert_copy_signal(&signal("buy-1", "buy", now))?;

    let first = store.record_execution_dry_run_order("buy-1", "metis-dry-run", now)?;
    let second = store.record_execution_dry_run_order("buy-1", "metis-dry-run", now)?;
    let latest = store
        .latest_execution_dry_run_order()?
        .expect("dry-run order should exist");

    assert_eq!(first, ExecutionDryRunRecordOutcome::Inserted);
    assert_eq!(second, ExecutionDryRunRecordOutcome::Existing);
    assert_eq!(latest.signal_id, "buy-1");
    assert_eq!(latest.route, "metis-dry-run");
    assert_eq!(latest.status, EXECUTION_STATUS_DRY_RUN_CONFIRMED);
    assert_eq!(
        latest.simulation_status,
        EXECUTION_SIMULATION_STATUS_DRY_RUN_SKIPPED
    );
    assert_eq!(latest.attempt, 1);
    Ok(())
}
