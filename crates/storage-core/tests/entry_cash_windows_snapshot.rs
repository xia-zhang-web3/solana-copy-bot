#[path = "common/entry_cash_fixture.rs"]
mod cash;
#[path = "common/entry_cost_fixture.rs"]
mod fixture;
use anyhow::Result;
use chrono::Duration;
use fixture::*;

#[test]
fn entry_cash_day_boundaries_late_accounting_and_replay_keep_original_closed_floor_policy(
) -> Result<()> {
    let mut db = fixed()?;
    let since = time("2026-09-06T00:00:00Z");
    cash::inventory(
        &db.conn()?,
        "lot",
        "cash-mint",
        3,
        0,
        since - Duration::days(1),
    )?;
    cash::settle(
        &db.store,
        &db.conn()?,
        "exec-canary:old",
        "old",
        WALLET,
        "cash-mint",
        1,
        -20,
        since - Duration::nanoseconds(1),
    )?;
    let fresh = cash::claim(
        &db.store,
        &db.conn()?,
        "exec-canary:late",
        "late",
        WALLET,
        "cash-mint",
        1,
        -5,
        since - Duration::days(1),
    )?;
    db.store
        .apply_execution_canary_sell_settlement(&fresh, since)?;
    assert_eq!(
        db.store.execution_canary_entry_cost(since)?.known_total()?,
        0
    );
    assert_eq!(
        db.store
            .execution_canary_entry_cost(since + Duration::nanoseconds(1))?
            .known_total()?,
        5
    );
    let day = db.store.execution_canary_sell_cash_day(db.now)?;
    assert_eq!(day.known_events.gross_negative_cash_result_lamports, "5");
    let before = snapshot(&db)?;
    assert!(
        db.store
            .apply_execution_canary_sell_settlement(&fresh, db.now + Duration::days(1))?
            .already_accounted
    );
    assert_eq!(snapshot(&db)?, before);
    cash::settle(
        &db.store,
        &db.conn()?,
        "exec-canary:future-close",
        "future",
        WALLET,
        "cash-mint",
        1,
        0,
        db.now + Duration::days(1),
    )?;
    // Existing CLOSED floor has no upper cutoff: the future-dated close deliberately
    // retains lifetime 25 while the separate public day API remains a clean day view.
    for _ in 0..2 {
        db.reopen()?;
        let v = cost(&db)?;
        assert_eq!(v.closed_loss.loss_lamports, "25");
        assert_eq!(
            v.cash_loss.day_gross_negative_lamports.as_deref(),
            Some("5")
        );
        assert_eq!(v.cash_loss.additional_loss_lamports.as_deref(), Some("0"));
        assert_eq!(v.known_total()?, 25);
        assert_eq!(db.store.execution_canary_sell_cash_day(db.now)?, day);
    }
    Ok(())
}

#[test]
fn entry_cash_closed_cash_and_failed_components_share_one_read_snapshot_under_writes() -> Result<()>
{
    use std::sync::{Arc, Barrier};
    let db = fixed()?;
    closed(&db, "floor", Some(-1), None, "closed", db.now)?;
    complete(&db, ORDER, 2)?;
    cash::inventory(&db.conn()?, "cash-lot", "cash-mint", 2, 0, db.now)?;
    cash::settle(
        &db.store,
        &db.conn()?,
        "exec-canary:cash",
        "cash",
        WALLET,
        "cash-mint",
        1,
        -4,
        db.now,
    )?;
    let facts2 = serde_json::to_string(&db.facts(ORDER, 2)?)?;
    let facts20 = serde_json::to_string(&db.facts(ORDER, 20)?)?;
    let path = db.path.clone();
    let barrier = Arc::new(Barrier::new(2));
    let b = barrier.clone();
    let writer = std::thread::spawn(move || -> Result<()> {
        let mut conn = rusqlite::Connection::open(path)?;
        conn.busy_timeout(std::time::Duration::from_secs(2))?;
        b.wait();
        // Coherent synthetic commits vary all three components atomically. This is
        // a stress test; the barrier does not force an intermediate read observation.
        for n in 0..96 {
            let (floor, fee, native, facts) = if n % 2 == 0 {
                (10, "20", 40, &facts20)
            } else {
                (1, "2", 4, &facts2)
            };
            let tx = conn.transaction()?;
            tx.execute(
                "UPDATE positions SET pnl_lamports=?1 WHERE token='floor'",
                [-floor],
            )?;
            tx.execute(
                "UPDATE execution_failed_expense_facts SET facts_json=?1",
                [facts],
            )?;
            tx.execute("UPDATE execution_failed_expense_ledger SET wallet_fee_lamports=?1,transaction_fee_lamports=?1",[fee])?;
            tx.execute("UPDATE execution_canary_receipt_facts SET wallet_native_pre=?1,wallet_native_post='0',wallet_native_delta=?2",rusqlite::params![native.to_string(),(-native).to_string()])?;
            tx.execute("UPDATE fills SET wallet_native_delta_lamports=?1,cash_result_delta_lamports=?1,accumulated_cash_result_lamports=?1",[-native])?;
            tx.execute(
                "UPDATE positions SET pnl_lamports=?1 WHERE position_id='cash-lot'",
                [-native],
            )?;
            tx.commit()?;
            std::thread::yield_now();
        }
        Ok(())
    });
    let reader = copybot_storage_core::SqliteStore::open_read_only(&db.path)?;
    barrier.wait();
    let read = (|| -> Result<()> {
        for _ in 0..192 {
            let v = reader.execution_canary_entry_cost(db.now + Duration::seconds(1))?;
            let tuple = (
                v.closed_loss.loss_lamports.as_str(),
                v.cash_loss.additional_loss_lamports.as_deref(),
                v.failed_expenses.known_wallet_fee_lamports.as_deref(),
                v.known_total()?,
            );
            assert!(
                matches!(
                    tuple,
                    ("1", Some("4"), Some("2"), 7) | ("10", Some("40"), Some("20"), 70)
                ),
                "mixed components: {v:?}"
            );
        }
        Ok(())
    })();
    writer.join().expect("snapshot writer panic")?;
    read?;
    Ok(())
}
