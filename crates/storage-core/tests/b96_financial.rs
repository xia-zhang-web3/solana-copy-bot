#[path = "common/b96_ordered.rs"]
mod f;
use anyhow::Result;
use copybot_storage_core::association_sell_preparation::{Check, Reason};
use f::*;
#[test]
fn b96_positive_within_across_restart_and_history_are_not_legacy_signals() -> Result<()> {
    for across in [false, true] {
        let mut f = F::new()?;
        if across {
            ready(&mut f)?;
        } else {
            f.anchors()?;
            f.sell()?;
            f.drain()?;
        }
        let money = f.db.snapshot()?;
        let i = inserted(&mut f)?;
        assert_eq!(i.intent_id, "source-sell:sell");
        assert_eq!(i.staged_evaluation.trade_authority, "trade_authority_none");
        assert_eq!(
            i.first.sell.admission.message_time,
            copybot_core_types::association_delivery::MessageTime::Missing
        );
        assert_eq!(
            stage(&mut f)?,
            OrderedSellStage::Existing(Box::new(i.clone()))
        );
        reopen(&mut f)?;
        assert_eq!(
            stage(&mut f)?,
            OrderedSellStage::Existing(Box::new(i.clone()))
        );
        assert_eq!(fresh(&f)?, OrderedSellDecision::ValidatedNow);
        assert_eq!(
            f.inbox
                .load_ordered_source_sell_intent_history(&i.intent_id)?,
            Some(i)
        );
        assert_eq!(f.db.snapshot()?, money);
        for table in [
            "observed_swaps",
            "execution_source_sell_intents",
            "execution_source_sell_promotions",
            "execution_quote_canary_events",
            "execution_quote_canary_provider_samples",
        ] {
            assert_eq!(count(&f, table)?, 0);
        }
        assert!(f
            .db
            .store
            .load_execution_source_sell_intent("source-sell:sell")?
            .is_none());
        assert!(matches!(
            f.db.store
                .promote_execution_source_sell_intent("source-sell:sell")?,
            copybot_storage_core::ExecutionSourceSellPromotionOutcome::Rejected(
                copybot_storage_core::ExecutionSourceSellPromotionReject::StagedMissing
            )
        ));
        assert_eq!(
            f.db.store.advance_execution_source_sell_staging()?,
            copybot_storage_core::ExecutionSourceSellStagingVisit::Wrapped
        );
    }
    Ok(())
}
#[test]
fn b96_other_receipt_after_or_missing_anchor_blocks_selected_positive() -> Result<()> {
    for anchored in [true, false] {
        let mut f = F::new()?;
        let order =
            f.db.seed("shadow:otherbuy:other:buy:mint", "other", "buy")?;
        f.db.buy(&order)?;
        f.anchors()?;
        if anchored {
            let a = facts(&format!("sig:{order}"), "execution-wallet", true);
            f.admit(a.clone())?;
            f.terminal(&a, 4, 42, "block")?;
        }
        f.sell()?;
        assert_eq!(
            f.read()?.current.selected_chain,
            Check::ProviderOrderedWithinBlock
        );
        let actual = refused(&mut f)?;
        assert_eq!(
            actual,
            if anchored {
                OrderedSellStage::Blocked(OrderedSellReason::ContributorOrder {
                    order_id: order,
                    check: Check::Blocked(Reason::NonIncreasingIndex),
                })
            } else {
                OrderedSellStage::Unknown(OrderedSellReason::ContributorOrder {
                    order_id: order,
                    check: Check::Unknown(Reason::MissingAnchor),
                })
            }
        );
        no_intent(&f)?;
    }
    Ok(())
}
#[test]
fn b96_pending_unproven_and_financial_bound_never_vacuously_stage() -> Result<()> {
    for case in ["pending", "unproven", "bound"] {
        let mut f = F::new()?;
        let order =
            f.db.seed("shadow:otherbuy:other:buy:mint", "other", "buy")?;
        if case == "unproven" {
            f.db.buy(&order)?;
            f.db.conn()?.execute(
                "UPDATE fills SET position_id=NULL WHERE order_id=?1",
                [&order],
            )?;
        }
        if case == "bound" {
            f.db.conn()?
                .execute("UPDATE copy_signals SET status=?1", ["x".repeat(8 << 20)])?;
        }
        f.anchors()?;
        f.sell()?;
        let actual = refused(&mut f)?;
        match case {
            "pending" => assert_eq!(
                actual,
                OrderedSellStage::Blocked(OrderedSellReason::PendingBuys)
            ),
            "unproven" => assert_eq!(
                actual,
                OrderedSellStage::Blocked(OrderedSellReason::UnprovenLinks)
            ),
            _ => assert!(matches!(actual, OrderedSellStage::Unknown(_))),
        }
        no_intent(&f)?;
    }
    Ok(())
}
#[test]
fn b96_legacy_signal_time_control_accepts_receipt_after_sell_strict_refuses() -> Result<()> {
    let mut f = F::new()?;
    let a = facts("leaderbuy", "leader", true);
    f.admit(a.clone())?;
    f.terminal(&a, 1, 42, "block")?;
    let a = facts(&f.our, "execution-wallet", true);
    f.admit(a.clone())?;
    f.terminal(&a, 4, 42, "block")?;
    f.sell()?;
    assert_eq!(
        refused(&mut f)?,
        OrderedSellStage::Blocked(OrderedSellReason::SelectedChain(Check::Blocked(
            Reason::NonIncreasingIndex
        )))
    );
    no_intent(&f)?;
    // Separate known-time legacy control, not a timestamp injected into the new intent.
    let s = observed(&f)?;
    let actual = legacy(&f, &s)?;
    assert!(
        matches!(
            actual,
            copybot_storage_core::ExecutionSourceSellOutcome::Inserted(_)
        ),
        "{actual:?}"
    );
    Ok(())
}
#[test]
fn b96_omitted_first_contributors_and_unknown_first_cannot_rebind() -> Result<()> {
    let mut f = within()?;
    corrupt_first(&f, "UPDATE association_sell_preparations SET first_binding=json_set(first_binding,'$.contributors',json('[]'))")?;
    assert_eq!(
        refused(&mut f)?,
        OrderedSellStage::Unknown(OrderedSellReason::EmptyContributors)
    );
    no_intent(&f)?;
    let mut f = F::new()?;
    f.db.close()?;
    f.anchors()?;
    f.sell()?;
    let first = f.read()?.first;
    let order = f.db.seed("shadow:new:leader:buy:mint", "leader", "buy")?;
    f.db.buy(&order)?;
    reopen(&mut f)?;
    assert_eq!(
        refused(&mut f)?,
        OrderedSellStage::Unknown(OrderedSellReason::FirstGenerationUnknown)
    );
    assert_eq!(f.read()?.first, first);
    Ok(())
}

#[test]
fn b96_stale_first_financial_fingerprint_before_stage_is_never_rebound() -> Result<()> {
    let mut f = within()?;
    corrupt_first(&f, "UPDATE association_sell_preparations SET first_binding=json_set(first_binding,'$.contributors_fingerprint','old pending literal fingerprint')")?;
    let first = f.read()?.first;
    reopen(&mut f)?;
    assert_eq!(
        refused(&mut f)?,
        OrderedSellStage::Blocked(OrderedSellReason::SelectedChain(Check::Blocked(
            Reason::FinancialSetChanged
        )))
    );
    assert_eq!(f.read()?.first, first);
    no_intent(&f)
}
