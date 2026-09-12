#[path = "common/sell_settlement_fixture.rs"]
mod fixture;
use anyhow::Result;
use copybot_storage_core::*;
use fixture::*;

#[test]
fn readonly_repeat_reopen_preserve_every_table_and_pending_risk() -> Result<()> {
    for cash in [-9, 0, 30] {
        let db = Db::new(7, 10, 0, 7, cash)?;
        let before = snapshot(&db.conn()?)?;
        let risk = db
            .store
            .execution_canary_submit_risk_summary(db.now, "retry", 3)?;
        assert_eq!(risk.active_orders, 1);
        let expected = db.ready()?;
        for _ in 0..3 {
            let ro = SqliteStore::open_read_only(&db.path)?;
            assert!(ro.is_read_only());
            assert_eq!(ready(&ro)?, expected);
            assert_eq!(ready(&ro)?, expected);
            assert!(ro.execution_canary_accounting_pending()?);
            assert!(ro.execution_canary_token_accounting_pending("mint")?);
            assert!(!ro.execution_canary_fill_exists(ORDER)?);
            assert_eq!(
                ro.execution_canary_receipt_submit_block_reason("next", "mint", "sell")?,
                Some("sell_token_accounting_pending")
            );
            assert_eq!(
                ro.execution_canary_receipt_submit_block_reason("next", "other", "buy")?,
                Some(EXECUTION_ACCOUNTING_PENDING_REASON)
            );
            assert_eq!(
                ro.execution_canary_submit_risk_summary(db.now, "retry", 3)?,
                risk
            );
            let pending =
                ro.list_reconcilable_execution_canary_orders_for_route("tiny", "retry", 10)?;
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].order_id, ORDER);
            assert_eq!(snapshot(&db.conn()?)?, before);
        }
    }
    Ok(())
}

#[test]
fn changed_expected_inventory_produces_a_new_plan_and_never_applies_old_one() -> Result<()> {
    let db = Db::new(7, 10, 0, 3, 0)?;
    let original = db.ready()?;
    // Explicit fixture mutation to demonstrate staleness. No production apply exists.
    db.conn()?.execute(
        "UPDATE positions SET qty_raw='5',cost_lamports=8,pnl_lamports=-2",
        [],
    )?;
    let changed = snapshot(&db.conn()?)?;
    let next = db.ready()?;
    assert_ne!(next.expected_position, original.expected_position);
    assert_eq!(next.expected_position.quantity.raw(), 5);
    assert_eq!(next.expected_position.entry_basis.as_u64(), 8);
    assert_eq!(next.expected_position.accumulated_cash_result.as_i128(), -2);
    assert_eq!(next.remaining_quantity.raw(), 2);
    assert_eq!(next.allocated_entry_basis.as_u64(), 5);
    assert_eq!(next.accumulated_cash_result.as_i128(), -7);
    assert_eq!(snapshot(&db.conn()?)?, changed);
    assert!(db.store.execution_canary_accounting_pending()?);
    Ok(())
}

#[test]
fn concurrent_atomic_fixture_changes_never_mix_read_snapshots() -> Result<()> {
    use rusqlite::Connection;
    use std::sync::{Arc, Barrier};
    let db = Db::new(7, 10, 0, 3, 30)?;
    let path = db.path.clone();
    let barrier = Arc::new(Barrier::new(2));
    let writer_barrier = barrier.clone();
    // Two synthetic coherent worlds with different identity, token quantity and basis.
    // The writer changes them atomically; this is NOT settlement application.
    let writer = std::thread::spawn(move || -> Result<()> {
        let mut conn = Connection::open(path)?;
        writer_barrier.wait();
        for i in 0..160 {
            let tx = conn.transaction()?;
            let (raw, cost, sold, cash, sig) = if i % 2 == 0 {
                (11, 23, "-5", "50", "second")
            } else {
                (7, 10, "-3", "30", "signature")
            };
            tx.execute("UPDATE orders SET tx_signature=?1", [sig])?;
            tx.execute(
                "UPDATE execution_canary_receipt_proofs SET tx_signature=?1",
                [sig],
            )?;
            tx.execute(
                "UPDATE execution_canary_receipt_facts SET tx_signature=?1,token_delta_raw=?2,
                wallet_native_post=?3,wallet_native_delta=?3",
                rusqlite::params![sig, sold, cash],
            )?;
            tx.execute(
                "UPDATE positions SET qty_raw=?1,cost_lamports=?2",
                rusqlite::params![raw.to_string(), cost],
            )?;
            tx.commit()?;
            std::thread::yield_now();
        }
        Ok(())
    });
    let ro = SqliteStore::open_read_only(&db.path)?;
    barrier.wait();
    let reads = (|| -> Result<()> {
        for _ in 0..320 {
            let p = ready(&ro)?;
            let observed = (
                p.receipt.tx_signature.as_str(),
                p.sold_quantity.raw(),
                p.expected_position.quantity.raw(),
                p.allocated_entry_basis.as_u64(),
                p.remaining_entry_basis.as_u64(),
                p.cash_result_delta.as_i128(),
            );
            assert!(
                matches!(
                    observed,
                    ("signature", 3, 7, 5, 5, 25) | ("second", 5, 11, 11, 12, 39)
                ),
                "{observed:?}"
            );
        }
        Ok(())
    })();
    writer.join().expect("fixture writer panic")?;
    reads?;
    assert!(db.store.execution_canary_accounting_pending()?);
    assert!(!db.store.execution_canary_fill_exists(ORDER)?);
    Ok(())
}
