#[path = "common/b96_r1_barrier.rs"]
mod barrier;
#[path = "common/b96_r1.rs"]
mod f;
use anyhow::Result;
use copybot_storage_core::{
    association_inbox::AssociationInbox, ExecutionSellIntentOutcome as Legacy,
    ExecutionSellIntentReject as Reject, SqliteStore,
};
use f::*;
use std::time::Duration;
fn busy(error: anyhow::Error) {
    assert!(error.chain().any(|e| matches!(e.downcast_ref::<rusqlite::Error>(), Some(rusqlite::Error::SqliteFailure(code, _)) if code.code == rusqlite::ErrorCode::DatabaseBusy)), "{error:#}");
}
#[test]
fn r1_deterministic_actual_record_and_strict_race_each_winner_holds_reservation() -> Result<()> {
    for strict_wins in [true, false] {
        let (mut f, swap) = ready()?;
        f.db.conn()?.pragma_update(None, "journal_mode", "WAL")?;
        let mut l = limits();
        l.busy_ms = 1;
        let mut strict = if strict_wins {
            barrier::connection(|| AssociationInbox::open(&f.db.path, l))?
        } else {
            AssociationInbox::open(&f.db.path, l)?
        };
        let legacy = if strict_wins {
            SqliteStore::open(&f.db.path)?
        } else {
            barrier::connection(|| SqliteStore::open(&f.db.path))?
        };
        legacy.set_busy_timeout(Duration::ZERO)?;
        let before = f.db.snapshot()?;
        let gate = barrier::Gate::arm();
        if strict_wins {
            let writer = std::thread::spawn(move || {
                strict.stage_ordered_source_sell_intent("sell", PROVIDER_ORDER_STRICT_V1)
            });
            gate.reached()?;
            busy(legacy.record_execution_sell_intent(&swap).unwrap_err());
            assert_eq!(f.db.snapshot()?, before);
            no_intent(&f)?; // No uncommitted claim or runnable signal leaks to this connection.
            gate.release();
            assert!(matches!(
                writer.join().unwrap()?,
                OrderedSellStage::Inserted(_)
            ));
            assert!(matches!(
                legacy.record_execution_sell_intent(&swap)?,
                Legacy::Rejected(Reject::SourceSignatureClaimed)
            ));
            assert_eq!(f.db.snapshot()?, before);
        } else {
            let event = swap.clone();
            let writer = std::thread::spawn(move || legacy.record_execution_sell_intent(&event));
            gate.reached()?;
            busy(
                strict
                    .stage_ordered_source_sell_intent("sell", PROVIDER_ORDER_STRICT_V1)
                    .unwrap_err(),
            );
            assert_eq!(f.db.snapshot()?, before);
            no_intent(&f)?;
            gate.release();
            assert!(matches!(writer.join().unwrap()?, Legacy::Inserted(_)));
            assert_eq!(
                strict.stage_ordered_source_sell_intent("sell", PROVIDER_ORDER_STRICT_V1)?,
                OrderedSellStage::Blocked(OrderedSellReason::SourceSignatureClaimed)
            );
        }
        gate.completed();
        assert_eq!(count(&f, "source_sell_signature_claims")?, 1);
        assert_eq!(count(&f, "execution_source_sell_intents")?, 0);
        reopen(&mut f)?;
        if strict_wins {
            assert_eq!(fresh(&f)?, OrderedSellDecision::ValidatedNow);
            assert_eq!(
                owner(&f, "sell")?.as_deref(),
                Some(PROVIDER_ORDER_STRICT_V1)
            );
        } else {
            blocked(&mut f)?;
            assert_eq!(owner(&f, "sell")?.as_deref(), Some("legacy"));
        }
    }
    Ok(())
}

#[test]
#[ignore = "requires hash-bound actual accepted95 0071 database"]
fn r1_retention_busy_keeps_old_signal_until_claim_and_delete_can_commit() -> Result<()> {
    let mut f = pre0072()?;
    f.db.store.insert_copy_signal(&signal(&f, SIGNAL, "sell"))?;
    upgrade(&mut f)?;
    f.db.store.set_busy_timeout(Duration::ZERO)?;
    let before = f.db.snapshot()?;
    let lock = f.db.conn()?;
    lock.execute_batch("BEGIN IMMEDIATE")?;
    busy(retain(&f).unwrap_err());
    assert_eq!(f.db.snapshot()?, before);
    assert_eq!(owner(&f, "sell")?, None);
    lock.execute_batch("ROLLBACK")?;
    assert_eq!(retain(&f)?.copy_signals_deleted, 1);
    reopen(&mut f)?;
    blocked(&mut f)?;
    assert_eq!(owner(&f, "sell")?.as_deref(), Some("legacy"));
    Ok(())
}
