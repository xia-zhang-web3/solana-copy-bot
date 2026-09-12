#[path = "common/sell_cash_day_fixture.rs"]
mod fixture;
use anyhow::{ensure, Result};
use chrono::Duration;
use copybot_storage_core::*;
use fixture::*;
use std::sync::{Arc, Barrier};

#[test]
fn atomic_settlements_and_undated_counts_share_one_read_snapshot() -> Result<()> {
    const COUNT: u64 = 16;
    let db = Db::new(COUNT, COUNT as i64, 0, 1, 0)?;
    let mut facts = vec![db
        .store
        .load_execution_canary_receipt_facts(ORDER)?
        .unwrap()];
    for i in 1..COUNT {
        facts.push(prepare(&db, &format!("exec-canary:concurrent-{i}"), 1, 0)?);
    }
    let barrier = Arc::new(Barrier::new(2));
    let (path, b) = (db.path.clone(), barrier.clone());
    let writer = std::thread::spawn(move || -> Result<()> {
        let store = SqliteStore::open(path)?;
        b.wait();
        for f in facts {
            store.apply_execution_canary_sell_settlement(&f, as_of() - Duration::seconds(1))?;
            std::thread::yield_now();
        }
        Ok(())
    });
    let ro = SqliteStore::open_read_only(&db.path)?;
    barrier.wait();
    let reads = (|| -> Result<()> {
        for _ in 0..96 {
            let v = ro.execution_canary_sell_cash_day(as_of())?;
            let known = &v.known_events;
            ensure!(
                known.events + v.undated_obligations.confirmed_unreconciled_without_fill == COUNT,
                "mixed read snapshots: {v:?}"
            );
            ensure!(
                known.signed_net_cash_result_lamports == (-i128::from(known.events)).to_string(),
                "mixed cash totals"
            );
            ensure!(
                known.gross_negative_cash_result_lamports == known.events.to_string(),
                "mixed gross totals"
            );
            ensure!(
                known.partial_events == known.events.min(COUNT - 1),
                "mixed event quantity classification"
            );
            ensure!(
                known.full_events == u64::from(known.events == COUNT),
                "mixed final event classification"
            );
        }
        Ok(())
    })();
    writer.join().expect("synthetic settlement writer panic")?;
    reads?;
    sums(&db, as_of(), COUNT, -i128::from(COUNT), u128::from(COUNT))?;
    Ok(())
}

#[test]
fn readonly_reopen_retains_data_pending_risk_and_existing_cap_outputs() -> Result<()> {
    let db = Db::new(3, 9, 0, 1, -4)?;
    first(&db, as_of() - Duration::seconds(1))?;
    prepare(&db, "exec-canary:pending", 1, 0)?;
    let before = snapshot(&db.conn()?)?;
    let cap = db.store.execution_canary_entry_cost(as_of())?;
    let risk = db
        .store
        .execution_canary_submit_risk_summary(as_of(), "retry", 3)?;
    let block = db
        .store
        .execution_canary_receipt_submit_block_reason("new-buy", "mint", "buy")?;
    assert!(block.is_some());
    let expected = view(&db, as_of())?;
    for _ in 0..3 {
        let ro = SqliteStore::open_read_only(&db.path)?;
        assert_eq!(ro.execution_canary_sell_cash_day(as_of())?, expected);
        assert_eq!(ro.execution_canary_entry_cost(as_of())?, cap);
        assert_eq!(
            ro.execution_canary_submit_risk_summary(as_of(), "retry", 3)?,
            risk
        );
        assert_eq!(
            ro.execution_canary_receipt_submit_block_reason("new-buy", "mint", "buy")?,
            block
        );
        assert_eq!(snapshot(&db.conn()?)?, before);
    }
    Ok(())
}
