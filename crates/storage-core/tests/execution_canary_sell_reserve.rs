use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{CopySignalRow, Lamports, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS};
use copybot_storage_core::{
    ExecutionCanaryRecordOutcome, SqliteStore, EXECUTION_ERROR_BUILD_FAILED,
};
use tempfile::tempdir;

const ROUTE: &str = "metis-swap-instructions-dry-run";

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
fn guarded_sell_reserve_blocks_second_in_flight_sell_for_same_token() -> Result<()> {
    let store = open_migrated_store("guarded-sell-reserve-blocks-in-flight")?;
    let now = ts("2026-06-09T10:00:00Z");
    store.insert_copy_signal(&signal("sell-first", "TokenMint", now))?;
    store.insert_copy_signal(&signal(
        "sell-second",
        "TokenMint",
        now + Duration::seconds(5),
    ))?;

    let first = store.reserve_execution_canary_sell_order_unless_token_in_flight(
        "sell-first",
        ROUTE,
        now,
    )?;
    let second = store.reserve_execution_canary_sell_order_unless_token_in_flight(
        "sell-second",
        ROUTE,
        now + Duration::seconds(6),
    )?;

    assert_eq!(first.outcome, ExecutionCanaryRecordOutcome::Inserted);
    assert!(!first.blocked_by_in_flight_sell);
    assert_eq!(second.outcome, ExecutionCanaryRecordOutcome::Existing);
    assert!(second.blocked_by_in_flight_sell);
    assert_eq!(second.order.order_id, first.order.order_id);
    assert!(store
        .load_execution_canary_order_by_signal("sell-second")?
        .is_none());
    Ok(())
}

#[test]
fn guarded_sell_reserve_allows_after_previous_sell_is_terminal() -> Result<()> {
    let store = open_migrated_store("guarded-sell-reserve-allows-terminal")?;
    let now = ts("2026-06-09T10:10:00Z");
    store.insert_copy_signal(&signal("sell-first", "TokenMint", now))?;
    store.insert_copy_signal(&signal(
        "sell-second",
        "TokenMint",
        now + Duration::seconds(5),
    ))?;
    let first = store.reserve_execution_canary_sell_order_unless_token_in_flight(
        "sell-first",
        ROUTE,
        now,
    )?;
    store.mark_execution_canary_failed(
        &first.order.order_id,
        now + Duration::seconds(1),
        EXECUTION_ERROR_BUILD_FAILED,
        "quote failed before submit",
    )?;

    let second = store.reserve_execution_canary_sell_order_unless_token_in_flight(
        "sell-second",
        ROUTE,
        now + Duration::seconds(6),
    )?;

    assert_eq!(second.outcome, ExecutionCanaryRecordOutcome::Inserted);
    assert!(!second.blocked_by_in_flight_sell);
    assert_ne!(second.order.order_id, first.order.order_id);
    Ok(())
}

#[test]
fn guarded_sell_reserve_ignores_stale_candidate_outside_guard_window() -> Result<()> {
    let store = open_migrated_store("guarded-sell-reserve-stale-candidate")?;
    let now = ts("2026-06-09T10:20:00Z");
    store.insert_copy_signal(&signal("sell-old", "TokenMint", now))?;
    store.insert_copy_signal(&signal(
        "sell-new",
        "TokenMint",
        now + Duration::minutes(10),
    ))?;
    let old =
        store.reserve_execution_canary_sell_order_unless_token_in_flight("sell-old", ROUTE, now)?;

    let new = store.reserve_execution_canary_sell_order_unless_token_in_flight(
        "sell-new",
        ROUTE,
        now + Duration::minutes(10),
    )?;

    assert_eq!(old.outcome, ExecutionCanaryRecordOutcome::Inserted);
    assert_eq!(new.outcome, ExecutionCanaryRecordOutcome::Inserted);
    assert!(!new.blocked_by_in_flight_sell);
    Ok(())
}

fn signal(signal_id: &str, token: &str, ts: DateTime<Utc>) -> CopySignalRow {
    CopySignalRow {
        signal_id: signal_id.to_string(),
        wallet_id: "leader-wallet".to_string(),
        side: "sell".to_string(),
        token: token.to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}
