#[path = "common/association_sell_fixture.rs"]
mod f;
use anyhow::Result;
use copybot_core_types::association_delivery::*;
use copybot_storage_core::association_sell_preparation::*;
use f::*;
#[test]
fn b90_three_exact_anchors_and_native_fee_difference_no_financial_mutation() -> Result<()> {
    let mut f = F::new()?;
    let before = f.db.snapshot()?;
    f.anchors()?;
    f.sell()?;
    let p = f.read()?;
    assert_eq!(p.current.selected_chain, Check::ProviderOrderedWithinBlock);
    assert_eq!(p.current.contributor_orders.len(), 1);
    assert_eq!(p.current.trade_authority, "trade_authority_none");
    assert!(p
        .current
        .limitations
        .contains(&"history_is_at_most_proven_subset".into()));
    let FirstWitness::Selected(w) = p.first.witness else {
        panic!("witness missing")
    };
    assert_eq!(w.source_signature, "leaderbuy");
    assert_eq!(w.receipt.contributor.tx_signature, f.our);
    assert_eq!(f.db.snapshot()?, before);
    Ok(())
}
#[test]
fn b90_message_clock_missing_invalid_future_one_ns_does_not_supply_order() -> Result<()> {
    for (clock, expected) in [
        (MessageTime::Missing, MessageClockCheck::Missing),
        (MessageTime::InvalidNanos(-1), MessageClockCheck::Invalid),
        (
            MessageTime::CreatedAt {
                seconds: 10,
                nanos: 1,
            },
            MessageClockCheck::FutureVsAppDequeue,
        ),
    ] {
        let mut f = F::new()?;
        f.anchors()?;
        let mut sell = facts("sell", "leader", false);
        sell.message_time = clock.clone();
        f.admit(sell.clone())?;
        f.terminal(&sell, 3, 42, "block")?;
        let p = f.read()?;
        assert_eq!(p.first.sell.admission.message_time, clock);
        assert_eq!(p.first.message_clock_check, expected);
        assert_eq!(p.current.selected_chain, Check::ProviderOrderedWithinBlock);
    }
    Ok(())
}
#[test]
fn b90_reverse_equal_cross_slot_hash_and_missing_terminal() -> Result<()> {
    for (index, slot, hash, expected) in [
        (2, 42, "block", Check::Blocked(Reason::NonIncreasingIndex)),
        (1, 42, "block", Check::Blocked(Reason::NonIncreasingIndex)),
        (
            3,
            43,
            "block",
            Check::Blocked(Reason::ParentEndpointMalformed),
        ),
        (3, 42, "other", Check::Unknown(Reason::DifferentBlockhash)),
        (3, 42, "", Check::Unknown(Reason::EmptyBlockhash)),
    ] {
        let mut f = F::new()?;
        f.anchors()?;
        let mut sell = facts("sell", "leader", false);
        sell.facts.slot = slot;
        f.admit(sell.clone())?;
        assert_eq!(
            f.read()?.current.selected_chain,
            Check::Unknown(Reason::MissingTerminal)
        );
        f.terminal(&sell, index, slot, hash)?;
        assert_eq!(f.read()?.current.selected_chain, expected);
    }
    Ok(())
}
#[test]
fn b90_wrong_receipt_anchor_wallet_token_side_raw_decimals_block() -> Result<()> {
    for variant in 0..6 {
        let mut f = F::new()?;
        let source = facts("leaderbuy", "leader", true);
        f.admit(source.clone())?;
        f.terminal(&source, 1, 42, "block")?;
        let mut our = facts(&f.our, "execution-wallet", true);
        match variant {
            0 => our.facts.wallet = "leader".into(),
            1 => our.facts.token_out = "other".into(),
            2 => {
                our.facts.token_in = "mint".into();
                our.facts.token_out = SOL.into();
            }
            3 => our.facts.exact_amounts.as_mut().unwrap().amount_out_raw = "7001".into(),
            4 => {
                our.facts
                    .exact_amounts
                    .as_mut()
                    .unwrap()
                    .amount_out_decimals = 4
            }
            _ => our.facts.exact_amounts.as_mut().unwrap().amount_out_raw = "07000".into(),
        }
        f.admit(our.clone())?;
        f.terminal(&our, 2, 42, "block")?;
        f.sell()?;
        assert!(matches!(
            f.read()?.current.selected_chain,
            Check::Blocked(Reason::IdentityConflict | Reason::AmountConflict)
        ));
    }
    Ok(())
}
#[test]
fn b90_our_and_leader_signature_substitution_never_proves_chain() -> Result<()> {
    let mut f = F::new()?;
    // Only the leader BUY is present, even though its token matches our receipt.
    let source = facts("leaderbuy", "leader", true);
    f.admit(source.clone())?;
    f.terminal(&source, 1, 42, "block")?;
    f.sell()?;
    assert_eq!(
        f.read()?.current.selected_chain,
        Check::Unknown(Reason::MissingAnchor)
    );
    Ok(())
}
