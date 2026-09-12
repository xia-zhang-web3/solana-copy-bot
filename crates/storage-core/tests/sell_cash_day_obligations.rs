#[path = "common/sell_cash_day_fixture.rs"]
mod fixture;
use anyhow::Result;
use chrono::Duration;
use copybot_storage_core::*;
use fixture::*;

#[test]
fn mixed_cash_legacy_and_unsettled_are_distinct_from_historical_day_coverage() -> Result<()> {
    let db = Db::new(1, 3, 0, 1, 3)?;
    first(&db, as_of() - Duration::seconds(1))?;
    obligation(
        &db,
        "exec-canary:legacy",
        "sell",
        EXECUTION_STATUS_CANARY_CONFIRMED,
        Some("old"),
    )?;
    legacy_fill(&db, "exec-canary:legacy")?;
    obligation(
        &db,
        "exec-canary:submitted",
        "sell",
        EXECUTION_STATUS_CANARY_SUBMITTED,
        Some("pending"),
    )?;
    obligation(
        &db,
        "exec-canary:missing-signature",
        "sell",
        EXECUTION_STATUS_CANARY_SUBMITTED,
        None,
    )?;
    obligation(
        &db,
        "exec-canary:blank-signature",
        "sell",
        EXECUTION_STATUS_CANARY_CONFIRMED,
        Some(" "),
    )?;
    obligation(
        &db,
        "exec-canary:unreconciled",
        "SELL",
        EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED,
        Some("confirmed"),
    )?;
    let v = sums(&db, as_of(), 1, 0, 0)?;
    assert_eq!(v.known_events.zero_events, 1);
    let obligations = &v.undated_obligations;
    assert_eq!(obligations.legacy_fill_orders, 1);
    assert_eq!(obligations.submitted_without_fill, 2);
    assert_eq!(obligations.confirmed_unreconciled_without_fill, 1);
    assert_eq!(obligations.confirmed_without_fill, 1);
    assert_eq!(obligations.without_signature, 2);
    assert!(obligations.scope.contains("not_historical_as_of"));
    let older = sums(&db, time("2026-09-01T12:00:00Z"), 0, 0, 0)?;
    assert_eq!(
        &older.undated_obligations, obligations,
        "current unknowns are not backdated events"
    );
    assert!(older.coverage.contains("history_unproven"));
    Ok(())
}

#[test]
fn missing_signal_with_durable_sell_proof_remains_an_undated_obligation() -> Result<()> {
    let db = Db::new(1, 3, 0, 1, 0)?;
    db.conn()?.execute_batch(
        "PRAGMA foreign_keys=OFF; DELETE FROM copy_signals WHERE signal_id='sell-signal';",
    )?;
    let pending = sums(&db, as_of(), 0, 0, 0)?;
    assert_eq!(
        pending
            .undated_obligations
            .confirmed_unreconciled_without_fill,
        1
    );
    db.conn()?.execute(
        "UPDATE orders SET tx_signature=NULL,status=?1",
        [EXECUTION_STATUS_CANARY_SUBMITTED],
    )?;
    let missing = sums(&db, as_of(), 0, 0, 0)?;
    assert_eq!(missing.undated_obligations.submitted_without_fill, 1);
    assert_eq!(missing.undated_obligations.without_signature, 1);
    legacy_fill(&db, ORDER)?;
    let legacy = sums(&db, as_of(), 0, 0, 0)?;
    assert_eq!(legacy.undated_obligations.legacy_fill_orders, 1);
    assert_eq!(legacy.undated_obligations.submitted_without_fill, 0);
    Ok(())
}

#[test]
fn noncanary_buy_presubmit_and_terminal_failures_without_fills_are_excluded() -> Result<()> {
    let db = Db::new(1, 0, 0, 1, 0)?;
    first(&db, time("2026-09-05T12:00:00Z"))?;
    let empty = sums(&db, as_of(), 0, 0, 0)?;
    for (i, status) in [
        EXECUTION_STATUS_CANARY_CANDIDATE,
        EXECUTION_STATUS_CANARY_BUILT,
        EXECUTION_STATUS_CANARY_SIMULATED,
        EXECUTION_STATUS_CANARY_FAILED,
        EXECUTION_STATUS_CANARY_EXPIRED,
    ]
    .into_iter()
    .enumerate()
    {
        obligation(
            &db,
            &format!("exec-canary:pre-or-terminal-{i}"),
            "sell",
            status,
            Some("signature"),
        )?;
    }
    obligation(
        &db,
        "noncanary:sent",
        "sell",
        EXECUTION_STATUS_CANARY_SUBMITTED,
        None,
    )?;
    obligation(
        &db,
        "exec-canary:buy",
        "buy",
        EXECUTION_STATUS_CANARY_CONFIRMED,
        None,
    )?;
    legacy_fill(&db, "exec-canary:buy")?;
    // Unrelated cash-looking corruption is outside the canary scope.
    obligation(
        &db,
        "noncanary:cash",
        "sell",
        EXECUTION_STATUS_CANARY_CONFIRMED,
        None,
    )?;
    let conn = db.conn()?;
    conn.execute_batch(
        "PRAGMA ignore_check_constraints=ON;
        INSERT INTO fills(order_id,token,qty,accounting_basis,settlement_ts)
        VALUES('noncanary:cash','other',0,'receipt_native_cash','invalid-date');",
    )?;
    assert_eq!(view(&db, as_of())?, empty);
    Ok(())
}

#[test]
fn both_empty_database_and_only_undated_history_preserve_unknown_full_day_result() -> Result<()> {
    let db = Db::new(1, 0, 0, 1, 0)?;
    let all = snapshot(&db.conn()?)?;
    let pending = sums(&db, as_of(), 0, 0, 0)?;
    assert_eq!(
        pending
            .undated_obligations
            .confirmed_unreconciled_without_fill,
        1
    );
    assert_eq!(snapshot(&db.conn()?)?, all);
    let dir = tempfile::tempdir()?;
    let mut store = SqliteStore::open(dir.path().join("empty.db"))?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let empty = store.execution_canary_sell_cash_day(as_of())?;
    assert_eq!(empty.known_events.events, 0);
    assert_eq!(empty.known_events.signed_net_cash_result_lamports, "0");
    assert_eq!(empty.known_events.gross_negative_cash_result_lamports, "0");
    assert_eq!(
        empty
            .undated_obligations
            .confirmed_unreconciled_without_fill,
        0
    );
    assert!(empty.full_day_cash_result_lamports.is_none());
    assert!(empty.economic_pnl_lamports.is_none());
    Ok(())
}
