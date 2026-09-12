#[path = "common/b96_ordered.rs"]
mod f;
use anyhow::Result;
use copybot_storage_core::{ExecutionSourceSellOutcome as Legacy, ExecutionSourceSellReject};
use f::*;
#[test]
fn b96_legacy_then_ordered_and_promoted_retained_never_reassign() -> Result<()> {
    for promoted in [false, true] {
        let mut f = within()?;
        let s = observed(&f)?;
        let Legacy::Inserted(i) = legacy(&f, &s)? else {
            panic!("legacy insert")
        };
        if promoted {
            f.db.store
                .promote_execution_source_sell_intent(&i.intent_id)?;
        }
        let before = f.db.snapshot()?;
        assert_eq!(
            refused(&mut f)?,
            OrderedSellStage::Blocked(OrderedSellReason::SourceSignatureClaimed)
        );
        f.db.conn()?
            .execute("DELETE FROM execution_source_sell_intents", [])?;
        reopen(&mut f)?;
        assert_eq!(
            refused(&mut f)?,
            OrderedSellStage::Blocked(OrderedSellReason::SourceSignatureClaimed)
        );
        assert_eq!(f.db.snapshot()?, before);
        assert_eq!(count(&f, "ordered_source_sell_intents")?, 0);
    }
    Ok(())
}
#[test]
fn b96_ordered_then_legacy_and_replace_delete_are_blocked() -> Result<()> {
    let mut f = within()?;
    let s = observed(&f)?;
    let i = inserted(&mut f)?;
    assert!(matches!(
        legacy(&f, &s)?,
        Legacy::Rejected(ExecutionSourceSellReject::StagedEventConflict)
    ));
    let c = f.db.conn()?;
    for sql in [
        "DELETE FROM ordered_source_sell_intents",
        "UPDATE ordered_source_sell_intents SET policy='other'",
        "INSERT OR REPLACE INTO ordered_source_sell_intents SELECT * FROM ordered_source_sell_intents",
        "DELETE FROM source_sell_signature_claims",
        "UPDATE source_sell_signature_claims SET owner='legacy'",
        "INSERT OR REPLACE INTO source_sell_signature_claims SELECT * FROM source_sell_signature_claims",
    ] { assert!(c.execute_batch(sql).is_err(), "{sql}"); }
    reopen(&mut f)?;
    assert_eq!(
        f.inbox
            .load_ordered_source_sell_intent_history(&i.intent_id)?,
        Some(i)
    );
    assert_eq!(count(&f, "execution_source_sell_intents")?, 0);
    Ok(())
}
#[test]
fn b96_orphan_canonical_signals_promotions_orders_reserve_signature() -> Result<()> {
    for case in ["signal", "promotion", "order"] {
        let mut f = within()?;
        let id = "shadow:sell:another-wallet:sell:mint";
        if case == "promotion" {
            f.db.conn()?.execute("INSERT INTO execution_source_sell_promotions VALUES(?1,'source-sell:sell','2026-09-10T00:00:00Z')", [id])?;
        } else {
            let order = f.db.seed(id, "another-wallet", "sell")?;
            // Intent ownership must see an order even when its canonical signal was lost.
            if case == "order" {
                let c = f.db.conn()?;
                c.pragma_update(None, "foreign_keys", false)?;
                c.execute("DELETE FROM copy_signals WHERE signal_id=?1", [id])?;
                // Orphan is pending in accepted financial reader; it still cannot stage.
                assert!(!order.is_empty());
            }
        }
        refused(&mut f)?;
        assert_eq!(count(&f, "ordered_source_sell_intents")?, 0);
    }
    Ok(())
}
#[test]
fn b96_concurrent_legacy_ordered_and_ordered_ordered_have_one_owner() -> Result<()> {
    use std::sync::{Arc, Barrier};
    for legacy_race in [true, false] {
        let mut f = within()?;
        let s = observed(&f)?;
        let mut l = limits();
        l.busy_ms = 5000;
        let mut a = copybot_storage_core::association_inbox::AssociationInbox::open(&f.db.path, l)?;
        let mut b = copybot_storage_core::association_inbox::AssociationInbox::open(&f.db.path, l)?;
        let store = copybot_storage_core::SqliteStore::open(&f.db.path)?;
        let position = store
            .load_execution_canary_open_position("mint")?
            .unwrap()
            .position_id;
        let barrier = Arc::new(Barrier::new(3));
        let start = barrier.clone();
        let first = std::thread::spawn(move || {
            start.wait();
            a.stage_ordered_source_sell_intent("sell", PROVIDER_ORDER_STRICT_V1)
        });
        let start = barrier.clone();
        let second = std::thread::spawn(move || -> Result<bool> {
            start.wait();
            Ok(if legacy_race {
                matches!(
                    store.stage_execution_source_sell_intent(&s, &position)?,
                    Legacy::Inserted(_)
                )
            } else {
                matches!(
                    b.stage_ordered_source_sell_intent("sell", PROVIDER_ORDER_STRICT_V1)?,
                    OrderedSellStage::Inserted(_)
                )
            })
        });
        barrier.wait();
        let first = first.join().unwrap()?;
        let second_inserted = second.join().unwrap()?;
        assert_eq!(
            usize::from(matches!(first, OrderedSellStage::Inserted(_)))
                + usize::from(second_inserted),
            1
        );
        assert_eq!(count(&f, "source_sell_signature_claims")?, 1);
        assert_eq!(
            count(&f, "ordered_source_sell_intents")? + count(&f, "execution_source_sell_intents")?,
            1
        );
        reopen(&mut f)?;
        if count(&f, "ordered_source_sell_intents")? == 1 {
            assert!(matches!(stage(&mut f)?, OrderedSellStage::Existing(_)));
        } else {
            refused(&mut f)?;
        }
    }
    Ok(())
}
