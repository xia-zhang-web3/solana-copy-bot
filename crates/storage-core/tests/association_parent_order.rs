#[path = "common/association_parent_fixture.rs"]
mod f;
use anyhow::Result;
use copybot_core_types::{association_delivery::*, association_parent::*};
use copybot_storage_core::{association_inbox::AssociationInbox, association_sell_preparation::*};
use f::*;
#[test]
fn b91_direct_multihop_skipped_slots_and_late_frontier_preserve_first_witness() -> Result<()> {
    for (direct, sell_first) in [(true, false), (false, false), (false, true)] {
        let mut f = F::new()?;
        let before = f.db.snapshot()?;
        anchors(&mut f, sell_first)?;
        let first = f.read()?.first;
        assert_eq!(
            f.read()?.current.selected_chain,
            Check::Unknown(Reason::ParentGap)
        );
        let edges = if direct {
            vec![edge(key(42, 3), key(30, 1)), edge(key(60, 6), key(42, 3))]
        } else {
            graph()
        };
        for e in edges.iter().skip(1) {
            put(&mut f, e.clone())?;
        }
        f.drain()?;
        assert_eq!(
            f.read()?.current.selected_chain,
            Check::Unknown(Reason::ParentGap)
        );
        put(&mut f, edges[0].clone())?;
        f.drain()?;
        let p = f.read()?;
        assert_eq!(p.current.selected_chain, Check::ProviderOrderedAcrossBlocks);
        assert_eq!(
            p.current.contributor_orders[0].relative_to_sell,
            Check::ProviderOrderedAcrossBlocks
        );
        assert!(!p.current.parent_paths.is_empty());
        for path in &p.current.parent_paths {
            assert_eq!(path.edges.first().unwrap().child, path.later);
            assert_eq!(path.edges.last().unwrap().parent, path.earlier);
            assert!(path.edges.windows(2).all(|w| w[0].parent == w[1].child));
            assert!(path.edges.iter().all(|e| e.parent.slot < e.child.slot));
        }
        assert_eq!(p.first, first);
        assert_eq!(f.db.snapshot()?, before);
    }
    Ok(())
}
#[test]
fn b91_parent_only_conflict_of_each_used_edge_is_sticky_across_restart_and_correct_replay(
) -> Result<()> {
    for original in graph() {
        let mut f = F::new()?;
        ready(&mut f)?;
        let before = f.db.snapshot()?;
        let positive = f.read()?;
        assert_eq!(
            positive.current.selected_chain,
            Check::ProviderOrderedAcrossBlocks
        );
        f.inbox = AssociationInbox::open(&f.db.path, limits())?;
        let bad = edge(original.child.clone(), key(original.child.slot - 1, 57));
        put(&mut f, bad)?;
        f.drain()?;
        let stale = serde_json::to_string(&positive.historical_latest)?;
        f.db.conn()?.execute(
            "UPDATE association_sell_preparations SET latest_evaluation=?1 WHERE signature='sell'",
            [stale],
        )?;
        assert_eq!(
            f.read()?.historical_latest.selected_chain,
            Check::ProviderOrderedAcrossBlocks
        );
        assert_eq!(
            f.read()?.current.selected_chain,
            Check::Blocked(Reason::ParentConflict)
        );
        put(&mut f, original.clone())?;
        f.event(
            DeliveryEvent::Session(SessionGap::Reset),
            CandidateGeneration::Unknown,
        )?;
        f.inbox = AssociationInbox::open(&f.db.path, limits())?;
        f.drain()?;
        assert_eq!(
            f.read()?.current.selected_chain,
            Check::Blocked(Reason::ParentConflict)
        );
        assert_eq!(f.read()?.first, positive.first);
        let (wire,conflict):(String,Option<String>)=f.db.conn()?.query_row("SELECT first_observation,contradiction FROM association_parent_blocks WHERE block_key=?1",[serde_json::to_string(&original.child)?],|r|Ok((r.get(0)?,r.get(1)?)))?;
        assert_eq!(serde_json::from_str::<ParentObservation>(&wire)?, original);
        assert!(conflict.is_some());
        assert_eq!(f.db.snapshot()?, before);
    }
    Ok(())
}
#[test]
fn b91_common_ancestor_fork_and_hash_slot_alias_cannot_prove_order() -> Result<()> {
    let mut f = F::new()?;
    anchors(&mut f, false)?;
    for e in [
        edge(key(42, 3), key(30, 1)),
        edge(key(60, 6), key(42, 7)),
        edge(key(42, 7), key(30, 1)),
    ] {
        put(&mut f, e)?;
    }
    assert_eq!(
        f.read()?.current.selected_chain,
        Check::Blocked(Reason::ParentBranchMismatch)
    );
    let mut f = F::new()?;
    ready(&mut f)?;
    put(&mut f, edge(key(43, 3), key(30, 1)))?;
    assert_eq!(
        f.read()?.current.selected_chain,
        Check::Blocked(Reason::ParentHashSlotConflict)
    );
    f.inbox = AssociationInbox::open(&f.db.path, limits())?;
    f.drain()?;
    put(&mut f, edge(key(42, 3), key(40, 2)))?;
    assert_eq!(
        f.read()?.current.selected_chain,
        Check::Blocked(Reason::ParentHashSlotConflict)
    );
    Ok(())
}
#[test]
fn b91_missing_malformed_nondecreasing_and_missing_to_known_are_explicit() -> Result<()> {
    for variant in 0..4 {
        let mut f = F::new()?;
        anchors(&mut f, false)?;
        put(&mut f, edge(key(60, 6), key(42, 3)))?;
        let parent = match variant {
            0 => BlockKey {
                slot: 0,
                hash: String::new(),
            },
            1 => BlockKey {
                slot: 30,
                hash: "0".repeat(32),
            },
            2 => key(42, 2),
            _ => key(43, 2),
        };
        put(&mut f, edge(key(42, 3), parent))?;
        f.drain()?;
        let expected = if variant == 0 {
            Check::Unknown(Reason::ParentMissingData)
        } else {
            Check::Blocked(Reason::ParentMalformed)
        };
        assert_eq!(f.read()?.current.selected_chain, expected);
        put(&mut f, edge(key(42, 3), key(30, 1)))?;
        f.drain()?;
        assert_eq!(
            f.read()?.current.selected_chain,
            Check::Blocked(Reason::ParentConflict)
        );
    }
    Ok(())
}
#[test]
fn b91_aggregate_path_budget_n_and_n_plus_one_revalidate_historical_positive() -> Result<()> {
    let mut f = F::new()?;
    anchors(&mut f, false)?;
    let mut edges = vec![edge(key(42, 3), key(40, 2)), edge(key(40, 2), key(30, 1))];
    let nodes = [
        key(60, 6),
        key(58, 7),
        key(56, 8),
        key(54, 9),
        key(52, 10),
        key(50, 5),
        key(42, 3),
    ];
    edges.extend(nodes.windows(2).map(|w| edge(w[0].clone(), w[1].clone())));
    for e in edges {
        put(&mut f, e)?;
    }
    f.drain()?;
    let first = f.read()?.first;
    assert!(f.inbox.usage()?.0 < 64);
    for (count, expected) in [
        (66, Check::ProviderOrderedAcrossBlocks),
        (65, Check::Unknown(Reason::ParentTraversalBound)),
    ] {
        f.inbox = AssociationInbox::open(
            &f.db.path,
            copybot_storage_core::association_inbox::InboxLimits { count, ..limits() },
        )?;
        assert_eq!(f.read()?.current.selected_chain, expected);
        assert_eq!(f.read()?.first, first);
    }
    Ok(())
}
