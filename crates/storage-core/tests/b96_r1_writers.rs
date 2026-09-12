#[path = "common/b96_r1.rs"]
mod f;
use anyhow::Result;
use copybot_storage_core::{
    ExecutionSellIntentOutcome as Legacy, ExecutionSellIntentReject as Reject,
};
use f::*;

#[test]
fn r1_actual_record_both_directions_replay_and_reopen() -> Result<()> {
    for strict_first in [true, false] {
        let (mut f, swap) = ready()?;
        let before = f.db.snapshot()?;
        if strict_first {
            let i = inserted(&mut f)?;
            for _ in 0..2 {
                assert!(matches!(
                    f.db.store.record_execution_sell_intent(&swap)?,
                    Legacy::Rejected(Reject::SourceSignatureClaimed)
                ));
                assert_eq!(fresh(&f)?, OrderedSellDecision::ValidatedNow);
                assert_eq!(f.db.snapshot()?, before);
                reopen(&mut f)?;
            }
            assert_eq!(
                f.inbox
                    .load_ordered_source_sell_intent_history(&i.intent_id)?,
                Some(i)
            );
            assert_eq!(
                owner(&f, "sell")?.as_deref(),
                Some(PROVIDER_ORDER_STRICT_V1)
            );
        } else {
            let Legacy::Inserted(s) = f.db.store.record_execution_sell_intent(&swap)? else {
                panic!("legacy control");
            };
            assert_eq!(s.signal_id, SIGNAL);
            assert_eq!(
                f.db.store.execution_sell_intent_position_block_reason(&s)?,
                None
            );
            let saved = f.db.snapshot()?;
            for _ in 0..2 {
                blocked(&mut f)?;
                assert!(matches!(
                    f.db.store.record_execution_sell_intent(&swap)?,
                    Legacy::Rejected(Reject::SignalAlreadyExists)
                ));
                assert_eq!(f.db.snapshot()?, saved);
                reopen(&mut f)?;
            }
            assert_eq!(owner(&f, "sell")?.as_deref(), Some("legacy"));
        }
        assert_eq!(count(&f, "source_sell_signature_claims")?, 1);
        assert_eq!(count(&f, "execution_source_sell_intents")?, 0);
    }
    Ok(())
}
#[test]
fn r1_noncanonical_buy_and_other_signature_do_not_block_strict() -> Result<()> {
    for (id, side) in [
        ("shadow:sell:leader:buy:mint", "buy"),
        ("manual:sell:leader:sell:mint", "sell"),
        ("shadow:other:leader:sell:mint", "sell"),
        ("shadow:sell:leader:sell:mint:extra", "sell"),
        ("shadow:sell::sell:mint", "sell"),
    ] {
        let mut f = within()?;
        assert!(f.db.store.insert_copy_signal(&signal(&f, id, side))?);
        assert_eq!(owner(&f, "sell")?, None);
        inserted(&mut f)?;
        assert_eq!(retain(&f)?.copy_signals_deleted, 1);
        reopen(&mut f)?;
        assert_eq!(fresh(&f)?, OrderedSellDecision::ValidatedNow, "{id}");
    }
    Ok(())
}
#[test]
fn r1_sql_signal_order_and_promotion_cannot_cross_strict_claim() -> Result<()> {
    let mut f = within()?;
    inserted(&mut f)?;
    let before = f.db.snapshot()?;
    assert!(f
        .db
        .store
        .insert_copy_signal(&signal(&f, SIGNAL, "sell"))
        .is_err());
    let c = f.db.conn()?;
    c.pragma_update(None, "foreign_keys", false)?;
    assert!(c.execute("INSERT INTO orders(order_id,signal_id,client_order_id,route,submit_ts,status) VALUES('orphan',?1,'orphan','tiny','2026-09-07T12:00:00Z','execution_failed')",[SIGNAL]).is_err());
    assert!(c.execute("INSERT INTO execution_source_sell_promotions VALUES(?1,'source-sell:sell','2026-09-07T12:00:00Z')",[SIGNAL]).is_err());
    assert_eq!(f.db.snapshot()?, before);
    assert_eq!(count(&f, "execution_source_sell_promotions")?, 0);
    assert_eq!(count(&f, "source_sell_signature_claims")?, 1);
    assert_eq!(fresh(&f)?, OrderedSellDecision::ValidatedNow);
    Ok(())
}
#[test]
fn r1_record_claim_ignore_abort_and_signal_faults_do_not_commit_half_state() -> Result<()> {
    for (table, timing, action) in [
        ("source_sell_signature_claims", "BEFORE", "IGNORE"),
        (
            "source_sell_signature_claims",
            "BEFORE",
            "ABORT,'claim fault'",
        ),
        ("copy_signals", "BEFORE", "IGNORE"),
        ("copy_signals", "AFTER", "ABORT,'signal fault'"),
    ] {
        let (mut f, swap) = ready()?;
        let before = f.db.snapshot()?;
        f.db.conn()?.execute_batch(&format!(
            "CREATE TRIGGER fault {timing} INSERT ON {table} BEGIN SELECT RAISE({action}); END;"
        ))?;
        let actual = f.db.store.record_execution_sell_intent(&swap);
        if table == "copy_signals" && action == "IGNORE" {
            assert!(matches!(
                actual?,
                Legacy::Rejected(Reject::SignalAlreadyExists)
            ));
        } else {
            assert!(actual.is_err(), "{table} {action}: {actual:?}");
        }
        no_intent(&f)?;
        assert_eq!(f.db.snapshot()?, before);
        reopen(&mut f)?;
        no_intent(&f)?;
        f.db.conn()?.execute_batch("DROP TRIGGER fault")?;
        inserted(&mut f)?;
    }
    Ok(())
}

#[test]
fn r1_actual_other_legacy_sell_remains_eligible_with_strict_owner() -> Result<()> {
    let (mut f, mut swap) = ready()?;
    inserted(&mut f)?;
    swap.signature = "other".into();
    assert!(f.db.store.insert_observed_swap(&swap)?);
    assert!(matches!(
        f.db.store.record_execution_sell_intent(&swap)?,
        Legacy::Inserted(_)
    ));
    assert_eq!(owner(&f, "other")?.as_deref(), Some("legacy"));
    assert_eq!(
        owner(&f, "sell")?.as_deref(),
        Some(PROVIDER_ORDER_STRICT_V1)
    );
    assert_eq!(fresh(&f)?, OrderedSellDecision::ValidatedNow);
    Ok(())
}
