#[path = "common/association_sell_fixture.rs"]
mod f;
use anyhow::Result;
use copybot_core_types::association_delivery::*;
use copybot_storage_core::{association_inbox::AssociationInbox, association_sell_preparation::*};
use f::*;
#[test]
fn b90_sell_first_late_selected_anchors_finish_without_rebinding() -> Result<()> {
    let mut f = F::new()?;
    f.sell()?;
    let initial = f.read()?.first;
    assert!(matches!(initial.witness, FirstWitness::Selected(_)));
    assert_eq!(
        f.read()?.current.selected_chain,
        Check::Unknown(Reason::MissingAnchor)
    );
    f.anchors()?;
    f.drain()?;
    assert_eq!(f.read()?.first, initial);
    assert_eq!(
        f.read()?.current.selected_chain,
        Check::ProviderOrderedWithinBlock
    );
    assert_eq!(
        f.read()?.historical_latest.selected_chain,
        Check::ProviderOrderedWithinBlock
    );
    f.inbox = AssociationInbox::open(&f.db.path, limits())?;
    f.admit(facts("sell", "leader", false))?;
    assert_eq!(f.read()?.first, initial);
    assert_eq!(
        f.read()?.current.selected_chain,
        Check::ProviderOrderedWithinBlock
    );
    Ok(())
}
#[test]
fn b90_late_conflict_each_anchor_invalidates_current_even_without_queue_refresh() -> Result<()> {
    for target in ["sell", "leaderbuy", "our"] {
        let mut f = F::new()?;
        f.anchors()?;
        f.sell()?;
        let original = f.read()?.first;
        let sig = if target == "our" {
            f.our.clone()
        } else {
            target.into()
        };
        f.conflict(&sig)?;
        assert_eq!(
            f.read()?.current.selected_chain,
            Check::Blocked(Reason::AnchorConflict)
        );
        assert_eq!(f.read()?.first, original);
        f.inbox = AssociationInbox::open(&f.db.path, limits())?;
        assert_eq!(
            f.read()?.current.selected_chain,
            Check::Blocked(Reason::AnchorConflict)
        );
    }
    Ok(())
}
#[test]
fn b90_generation_a_to_b_before_commit_and_after_preparation() -> Result<()> {
    for before in [true, false] {
        let mut f = F::new()?;
        f.anchors()?;
        let sell = facts("sell", "leader", false);
        let a = f.db.store.association_candidate(&sell.facts);
        if !before {
            f.admit(sell.clone())?;
        }
        f.db.close()?;
        let order =
            f.db.seed("shadow:newbuy:leader:buy:mint", "leader", "buy")?;
        f.db.buy(&order)?;
        if before {
            f.event(DeliveryEvent::Admission(sell.clone()), a.clone())?;
        }
        f.terminal(&sell, 3, 42, "block")?;
        let p = f.read()?;
        assert_eq!(p.first.candidate, a);
        assert_eq!(
            p.current.selected_chain,
            if before {
                Check::Unknown(Reason::GenerationChanged)
            } else {
                Check::Blocked(Reason::GenerationChanged)
            }
        );
    }
    Ok(())
}
#[test]
fn b90_unknown_initial_candidate_or_witness_cannot_adopt_later_buy() -> Result<()> {
    for unknown_candidate in [true, false] {
        let mut f = F::new()?;
        f.anchors()?;
        if unknown_candidate {
            f.db.close()?;
        } else {
            f.db.conn()?
                .execute("DELETE FROM execution_canary_receipt_facts", [])?;
        }
        f.sell()?;
        let first = f.read()?.first;
        assert!(matches!(first.witness, FirstWitness::Unknown(_)));
        let id =
            f.db.seed("shadow:newbuy:leader:buy:mint", "leader", "buy")?;
        f.db.buy(&id)?;
        f.admit(facts("sell", "leader", false))?;
        let p = f.read()?;
        assert_eq!(p.first, first);
        assert_ne!(p.current.selected_chain, Check::ProviderOrderedWithinBlock);
    }
    Ok(())
}
#[test]
fn b90_new_buy_and_pending_buy_invalidate_stored_positive() -> Result<()> {
    for applied in [true, false] {
        let mut f = F::new()?;
        f.anchors()?;
        f.sell()?;
        let first = f.read()?.first;
        let id =
            f.db.seed("shadow:newbuy:leader:buy:mint", "leader", "buy")?;
        if applied {
            f.db.buy(&id)?;
        }
        let p = f.read()?;
        assert_eq!(p.first, first);
        assert_eq!(
            p.historical_latest.selected_chain,
            Check::ProviderOrderedWithinBlock
        );
        assert_eq!(
            p.current.selected_chain,
            Check::Blocked(Reason::FinancialSetChanged)
        );
        if !applied {
            assert!(!p.current.pending_buys.is_empty());
        }
    }
    Ok(())
}
#[test]
fn b90_unknown_and_newer_contributors_are_visible_without_claiming_all_buys_ordered() -> Result<()>
{
    for index in [None, Some(4)] {
        let mut f = F::new()?;
        f.anchors()?;
        let id =
            f.db.seed("shadow:otherbuy:other:buy:mint", "other", "buy")?;
        f.db.buy(&id)?;
        if let Some(index) = index {
            let a = facts(&format!("sig:{id}"), "execution-wallet", true);
            f.admit(a.clone())?;
            f.terminal(&a, index, 42, "block")?;
        }
        f.sell()?;
        let p = f.read()?;
        assert_eq!(p.current.selected_chain, Check::ProviderOrderedWithinBlock);
        assert_eq!(p.current.contributor_orders.len(), 2);
        assert_eq!(
            p.current.contributor_orders[1].relative_to_sell,
            if index.is_some() {
                Check::Blocked(Reason::NonIncreasingIndex)
            } else {
                Check::Unknown(Reason::MissingAnchor)
            }
        );
        assert!(p
            .current
            .limitations
            .contains(&"sell_after_all_buys_not_proven".into()));
    }
    Ok(())
}
#[test]
fn b90_recovery_missing_terminal_and_unresolved_never_positive() -> Result<()> {
    for recovery in [true, false] {
        let mut f = F::new()?;
        f.anchors()?;
        let sell = facts("sell", "leader", false);
        f.admit(sell.clone())?;
        if recovery {
            f.inbox = AssociationInbox::open(&f.db.path, limits())?;
            f.terminal(&sell, 3, 42, "block")?;
        } else {
            f.event(
                DeliveryEvent::Terminal {
                    signature: "sell".into(),
                    expected: sell,
                    result: Terminal::Unresolved(Unresolved::EndOfStream),
                },
                CandidateGeneration::Unknown,
            )?;
        }
        assert_eq!(
            f.read()?.current.selected_chain,
            Check::Unknown(if recovery {
                Reason::Recovery
            } else {
                Reason::UnresolvedTerminal
            })
        );
    }
    Ok(())
}
