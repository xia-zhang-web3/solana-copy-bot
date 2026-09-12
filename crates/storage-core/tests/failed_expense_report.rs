#[path = "common/failed_expense_fixture.rs"]
mod fixture;
use anyhow::Result;
use chrono::Duration;
use fixture::*;

#[test]
fn failed_expense_full_window_exact_totals_ignore_display_limit_and_late_receipt_time() -> Result<()>
{
    let db = Db::new()?;
    for (id, sig) in [(ORDER, "failed-signature"), ("exec-canary:b", "sig-b")] {
        if id != ORDER {
            db.add(id, sig, "buy", db.now)?;
        }
        db.detect(id, "signature_status")?;
        db.store
            .apply_failed_expense(id, &db.facts(id, u64::MAX)?, db.now + Duration::days(10))?;
    }
    let full = db.report(10)?;
    let bounded = db.report(1)?;
    assert_eq!(
        bounded.known_wallet_fee_lamports,
        Some((u128::from(u64::MAX) * 2).to_string())
    );
    assert_eq!(
        bounded.known_wallet_fee_lamports,
        full.known_wallet_fee_lamports
    );
    assert_eq!(bounded.unknown_orders, 0);
    assert!(bounded.rows_truncated);
    assert_eq!(bounded.rows.len(), 1);
    let json = serde_json::to_value(&bounded)?;
    assert!(json["known_wallet_fee_lamports"].is_string());
    assert!(json["economic_pnl_lamports"].is_null());
    let late = db.store.execution_failed_expense_report(
        db.now + Duration::days(9),
        db.now + Duration::days(11),
        1,
    )?;
    assert_eq!(late.total_orders, 0);
    assert!(late.cohort_wallet_fee_lamports.is_none());
    let left =
        db.store
            .execution_failed_expense_report(db.now - Duration::seconds(1), db.now, 1)?;
    let right =
        db.store
            .execution_failed_expense_report(db.now, db.now + Duration::seconds(1), 1)?;
    assert_eq!((left.total_orders, right.total_orders), (0, 2));
    Ok(())
}
#[test]
fn failed_expense_mixed_legacy_and_empty_are_unknown_without_shadow_joins() -> Result<()> {
    let db = Db::new()?;
    assert!(db.report(1)?.known_wallet_fee_lamports.is_none());
    db.detect(ORDER, "receipt_meta")?;
    db.store
        .apply_failed_expense(ORDER, &db.facts(ORDER, 0)?, db.now)?;
    db.add("exec-canary:legacy", "old-sig", "buy", db.now)?;
    db.conn()?.execute(
        "UPDATE orders SET status='execution_canary_failed' WHERE order_id='exec-canary:legacy'",
        [],
    )?;
    let mixed = db.report(0)?;
    assert_eq!(
        (
            mixed.known_orders,
            mixed.unknown_orders,
            mixed.unresolved_orders,
            mixed.legacy_uncovered_orders
        ),
        (1, 1, 1, 1)
    );
    assert_eq!(mixed.known_wallet_fee_lamports.as_deref(), Some("0"));
    assert!(mixed.cohort_wallet_fee_lamports.is_none());
    assert_eq!(db.count("shadow_closed_trades")?, 0);
    assert_eq!(
        mixed.history_coverage,
        "unverified_prior_history_no_backfill"
    );
    // Pre-submit rejection has no signature and cannot create network expense evidence.
    db.add("exec-canary:preflight", "", "buy", db.now)?;
    db.conn()?.execute(
        "UPDATE orders SET status='execution_canary_failed' WHERE order_id='exec-canary:preflight'",
        [],
    )?;
    assert!(db
        .detect("exec-canary:preflight", "signature_status")
        .is_err());
    assert_eq!(db.report(0)?.total_orders, 2);
    Ok(())
}
#[test]
fn failed_expense_absent_migration_remains_uncovered_not_zero() -> Result<()> {
    let db = Db::new()?;
    db.conn()?.execute_batch("DROP TABLE execution_failed_expense_ledger; DROP TABLE execution_failed_expense_facts; DROP TABLE execution_failed_expense_tasks;")?;
    db.conn()?
        .execute("UPDATE orders SET status='execution_canary_failed'", [])?;
    let report = db.report(0)?;
    assert_eq!(report.coverage, "schema_unavailable");
    assert_eq!(report.legacy_uncovered_orders, 1);
    assert!(report.known_wallet_fee_lamports.is_none());
    Ok(())
}
