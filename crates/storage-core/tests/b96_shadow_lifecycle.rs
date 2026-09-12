#[path = "common/b96_ordered.rs"]
mod f;
use anyhow::Result;
use copybot_storage_core::{
    association_sell_preparation::{Check, Reason},
    association_sell_shadow_types::*,
};
use f::*;
#[test]
fn b96_shadow_before_equal_after_unanchored_and_other_pair_actual_policy() -> Result<()> {
    for case in ["before", "equal", "after", "unanchored", "other"] {
        let mut f = within()?;
        match case {
            "before" => {
                lot(&mut f, "lot", 42, "block", 2)?;
            }
            "equal" => {
                lot(&mut f, "lot", 42, "block", 3)?;
            }
            "after" => {
                lot(&mut f, "lot", 42, "block", 4)?;
            }
            "other" => {
                f.db.store
                    .insert_shadow_lot("other", "mint", 1.0, 1.0, f.db.now)?;
            }
            _ => {
                f.db.store
                    .insert_shadow_lot("leader", "mint", 1.0, 1.0, f.db.now)?;
            }
        }
        assert_eq!(
            f.read()?.current.selected_chain,
            Check::ProviderOrderedWithinBlock
        );
        if matches!(case, "after" | "other") {
            inserted(&mut f)?;
        } else {
            let actual = refused(&mut f)?;
            assert!(matches!(
                actual,
                OrderedSellStage::Blocked(OrderedSellReason::ShadowLot { .. })
                    | OrderedSellStage::Unknown(OrderedSellReason::ShadowLot { .. })
            ));
            no_intent(&f)?;
        }
    }
    Ok(())
}
#[test]
fn b96_partial_shadow_scan_is_unknown_not_empty_success() -> Result<()> {
    let mut f = within()?;
    f.db.store
        .insert_shadow_lot("leader", "mint", 1.0, 1.0, f.db.now)?;
    f.db.conn()?.execute(
        "UPDATE shadow_lots SET risk_context=?1",
        ["x".repeat(8 << 20)],
    )?;
    assert_eq!(
        f.read()?.current.shadow.unwrap().scan,
        ShadowScan::Unknown(Reason::LookupBound)
    );
    assert_eq!(
        refused(&mut f)?,
        OrderedSellStage::Unknown(OrderedSellReason::ShadowScan(Reason::LookupBound))
    );
    no_intent(&f)
}
#[test]
fn b96_late_lot_pending_conflict_generation_and_fingerprint_refuse_fresh_keep_history() -> Result<()>
{
    for case in [
        "lot",
        "pending",
        "conflict",
        "generation",
        "fingerprint",
        "zero",
    ] {
        let mut f = within()?;
        let i = inserted(&mut f)?;
        match case {
            "lot" => {
                f.db.store
                    .insert_shadow_lot("leader", "mint", 1.0, 1.0, f.db.now)?;
            }
            "pending" => {
                f.db.seed("shadow:late:other:buy:mint", "other", "buy")?;
            }
            "conflict" => {
                f.conflict("sell")?;
            }
            "generation" => {
                f.db.close()?;
                let order = f.db.seed("shadow:next:leader:buy:mint", "leader", "buy")?;
                f.db.buy(&order)?;
            }
            "fingerprint" => {
                corrupt_first(&f, "UPDATE association_sell_preparations SET first_binding=json_set(first_binding,'$.contributors_fingerprint','pre95 fingerprint')")?;
            }
            _ => {
                f.db.conn()?
                    .execute("UPDATE positions SET qty=0,qty_raw='0'", [])?;
            }
        }
        reopen(&mut f)?;
        let actual = fresh(&f)?;
        record(case, &actual)?;
        assert!(
            matches!(
                actual,
                OrderedSellDecision::Unknown(_) | OrderedSellDecision::Blocked(_)
            ),
            "{case}: {actual:?}"
        );
        refused(&mut f)?;
        assert_eq!(
            f.inbox
                .load_ordered_source_sell_intent_history(&i.intent_id)?,
            Some(i)
        );
        assert_eq!(count(&f, "ordered_source_sell_intents")?, 1);
    }
    Ok(())
}
#[test]
fn b96_future_message_clock_does_not_change_actual_policy() -> Result<()> {
    use copybot_core_types::association_delivery::MessageTime;
    for clock in [
        MessageTime::Missing,
        MessageTime::CreatedAt {
            seconds: 9_000_000_000,
            nanos: 1,
        },
    ] {
        let mut f = F::new()?;
        f.anchors()?;
        let mut a = facts("sell", "leader", false);
        a.message_time = clock.clone();
        f.admit(a.clone())?;
        f.terminal(&a, 3, 42, "block")?;
        let i = inserted(&mut f)?;
        assert_eq!(i.first.sell.admission.message_time, clock);
        assert_eq!(fresh(&f)?, OrderedSellDecision::ValidatedNow);
    }
    Ok(())
}
