#[path = "common/b95_shadow.rs"]
mod f;
use anyhow::Result;
use copybot_storage_core::{association_sell_preparation::*, association_sell_shadow_types::*};
use f::*;
#[test]
fn b95_actual_within_block_before_after_and_equal_do_not_change_selected_chain() -> Result<()> {
    for (index, expected) in [
        (0, ShadowRelation::BeforeSell),
        (4, ShadowRelation::AfterSell),
        (3, ShadowRelation::Conflict(Reason::NonIncreasingIndex)),
    ] {
        let mut f = within()?;
        let selected = f.read()?.current.selected_chain;
        lot(&mut f, "origin", 42, "block", index)?;
        let e = evidence(&f)?;
        assert_eq!(e.scan, ShadowScan::Complete);
        assert_eq!(e.lots.len(), 1);
        assert_eq!(e.lots[0].relation, expected);
        assert_eq!(f.read()?.current.selected_chain, selected);
        if index == 4 {
            assert_eq!(e.lots[0].sell_to_origin, Check::ProviderOrderedWithinBlock);
        }
        if index == 0 {
            assert_eq!(e.lots[0].origin_to_sell, Check::ProviderOrderedWithinBlock);
        }
    }
    Ok(())
}
#[test]
fn b95_actual_parent_chain_proves_both_directions() -> Result<()> {
    for before in [true, false] {
        let mut f = F::new()?;
        ready(&mut f)?;
        let selected = f.read()?.current.selected_chain;
        if before {
            lot(&mut f, "origin", 40, &hash(2), 99)?;
        } else {
            put(&mut f, edge(key(80, 8), key(60, 6)))?;
            lot(&mut f, "origin", 80, &hash(8), 0)?;
        }
        let e = evidence(&f)?;
        assert_eq!(
            e.lots[0].relation,
            if before {
                ShadowRelation::BeforeSell
            } else {
                ShadowRelation::AfterSell
            }
        );
        let check = if before {
            &e.lots[0].origin_to_sell
        } else {
            &e.lots[0].sell_to_origin
        };
        assert_eq!(*check, Check::ProviderOrderedAcrossBlocks);
        let p = f.read()?;
        assert_eq!(p.current.selected_chain, selected);
        assert!(p
            .current
            .parent_paths
            .iter()
            .any(|p| p.earlier_signature == "origin" || p.later_signature == "origin"));
        reopen(&mut f)?;
        assert_eq!(evidence(&f)?.lots[0].relation, e.lots[0].relation);
    }
    Ok(())
}
#[test]
fn b95_missing_origin_anchor_exact_and_recovery_remain_explicit_unknown() -> Result<()> {
    for case in 0..5 {
        let mut f = within()?;
        let mut a = facts("origin", "leader", true);
        match case {
            0 => {
                f.db.store
                    .insert_shadow_lot("leader", "mint", 1.0, 1.0, f.db.now)?;
            }
            1 => {
                insert(&f, &a)?;
            }
            2 => {
                f.admit(a.clone())?;
                insert(&f, &a)?;
                reopen(&mut f)?;
            }
            3 => {
                a.facts.exact_amounts = None;
                f.admit(a.clone())?;
                f.terminal(&a, 4, 42, "block")?;
                insert(&f, &a)?;
            }
            _ => {
                f.admit(a.clone())?;
                insert(&f, &a)?;
            }
        }
        assert_eq!(
            evidence(&f)?.lots[0].relation,
            ShadowRelation::Unknown(match case {
                0 => Reason::MissingShadowOrigin,
                1 => Reason::MissingAnchor,
                2 => Reason::Recovery,
                3 => Reason::MissingExactAmounts,
                _ => Reason::MissingTerminal,
            })
        );
    }
    Ok(())
}
#[test]
fn b95_swapped_source_facts_and_late_sticky_conflicts_never_clear() -> Result<()> {
    for case in 0..4 {
        let mut f = within()?;
        let a = facts("origin", "leader", true);
        insert(&f, &a)?;
        let mut admitted = a.clone();
        if case == 0 {
            admitted.facts.amount_out_bits = 8.0f64.to_bits();
        }
        if case == 1 {
            admitted
                .facts
                .exact_amounts
                .as_mut()
                .unwrap()
                .amount_out_raw = "8000".into();
        }
        if case == 2 {
            admitted.facts.wallet = "different".into();
        }
        f.admit(admitted.clone())?;
        f.terminal(&admitted, 4, 42, "block")?;
        if case == 3 {
            f.conflict("origin")?;
        }
        let expected = ShadowRelation::Conflict(if case == 3 {
            Reason::AnchorConflict
        } else {
            Reason::SourceFactsConflict
        });
        assert_eq!(evidence(&f)?.lots[0].relation, expected);
        reopen(&mut f)?;
        assert_eq!(evidence(&f)?.lots[0].relation, expected);
    }
    Ok(())
}
#[test]
fn b95_parent_gap_and_conflict_keep_selected_chain_separate() -> Result<()> {
    let mut f = F::new()?;
    ready(&mut f)?;
    lot(&mut f, "origin", 80, &hash(8), 0)?;
    assert_eq!(
        evidence(&f)?.lots[0].relation,
        ShadowRelation::Unknown(Reason::ParentGap)
    );
    put(&mut f, edge(key(80, 8), key(60, 6)))?;
    assert_eq!(evidence(&f)?.lots[0].relation, ShadowRelation::AfterSell);
    put(&mut f, edge(key(80, 8), key(50, 5)))?;
    assert_eq!(
        evidence(&f)?.lots[0].relation,
        ShadowRelation::Conflict(Reason::ParentConflict)
    );
    assert_eq!(
        f.read()?.current.selected_chain,
        Check::ProviderOrderedAcrossBlocks
    );
    reopen(&mut f)?;
    assert_eq!(
        evidence(&f)?.lots[0].relation,
        ShadowRelation::Conflict(Reason::ParentConflict)
    );
    Ok(())
}
