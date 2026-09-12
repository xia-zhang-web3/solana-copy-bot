#[path = "common/association_sell_fixture.rs"]
mod f;
use anyhow::Result;
use copybot_core_types::association_delivery::*;
use copybot_storage_core::{
    association_inbox::AssociationInbox, association_sell_preparation::*, SqliteStore,
};
use f::*;
#[test]
fn b90_committed_preparation_crash_child() -> Result<()> {
    let Ok(path) = std::env::var("B90_CRASH_DB") else {
        return Ok(());
    };
    let store = SqliteStore::open(&path)?;
    let mut inbox = AssociationInbox::open(&path, limits())?;
    let a = facts("sell", "leader", false);
    let candidate = store.association_candidate(&a.facts);
    inbox.persist_at(
        &Delivery {
            session: "crash".into(),
            sequence: 0,
            arrival_offset_ns: 0,
            event: DeliveryEvent::Admission(a),
        },
        &candidate,
        chrono::DateTime::from_timestamp(10, 0).unwrap(),
    )?;
    // Durable write/readback completed, but no outer consumer ACK is released.
    std::process::exit(17);
}
#[test]
fn b90_crash_reopen_and_exact_replay_preserve_first_selected_witness() -> Result<()> {
    let mut f = F::new()?;
    f.anchors()?;
    f.drain()?;
    let status = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "b90_committed_preparation_crash_child",
            "--nocapture",
        ])
        .env("B90_CRASH_DB", &f.db.path)
        .status()?;
    assert_eq!(status.code(), Some(17));
    let before = f.read()?.first;
    assert!(matches!(before.witness, FirstWitness::Selected(_)));
    f.inbox = AssociationInbox::open(&f.db.path, limits())?;
    let a = facts("sell", "leader", false);
    f.inbox.persist_at(
        &Delivery {
            session: "crash".into(),
            sequence: 0,
            arrival_offset_ns: 0,
            event: DeliveryEvent::Admission(a),
        },
        &CandidateGeneration::Unknown,
        chrono::DateTime::from_timestamp(20, 0).unwrap(),
    )?;
    assert_eq!(f.read()?.first, before);
    assert_eq!(
        f.read()?.current.selected_chain,
        Check::Unknown(Reason::Recovery)
    );
    Ok(())
}
#[test]
fn b90_first_bindings_cannot_be_changed_or_deleted() -> Result<()> {
    let mut f = F::new()?;
    f.anchors()?;
    f.sell()?;
    let before = f.read()?.first;
    for sql in [
        "UPDATE association_sell_preparations SET first_binding='{}'",
        "UPDATE association_sell_preparations SET initial_evaluation='{}'",
        "DELETE FROM association_sell_preparations",
        "DELETE FROM association_sell_dependencies",
        "UPDATE association_sell_dependencies SET first_identity='{}'",
    ] {
        assert!(f.db.conn()?.execute_batch(sql).is_err(), "{sql}");
        assert_eq!(f.read()?.first, before);
    }
    Ok(())
}
#[test]
fn b90_late_financial_witness_corruption_invalidates_without_refresh() -> Result<()> {
    let mut f = F::new()?;
    f.anchors()?;
    f.sell()?;
    f.db.conn()?.execute(
        "UPDATE execution_canary_receipt_facts SET token_delta_raw='7001'",
        [],
    )?;
    let p = f.read()?;
    assert_eq!(
        p.historical_latest.selected_chain,
        Check::ProviderOrderedWithinBlock
    );
    assert_eq!(
        p.current.selected_chain,
        Check::Blocked(Reason::FinancialSetChanged)
    );
    assert!(!p.current.unproven_links.is_empty());
    Ok(())
}
#[test]
fn b90_conflict_other_contributor_and_new_contributor_remain_explicit() -> Result<()> {
    let mut f = F::new()?;
    f.anchors()?;
    let id = f.db.seed("shadow:second:other:buy:mint", "other", "buy")?;
    f.db.buy(&id)?;
    let sig = format!("sig:{id}");
    let a = facts(&sig, "execution-wallet", true);
    f.admit(a.clone())?;
    f.terminal(&a, 2, 42, "block")?;
    f.sell()?;
    f.conflict(&sig)?;
    assert_eq!(
        f.read()?.current.selected_chain,
        Check::Blocked(Reason::AnchorConflict)
    );
    let id = f.db.seed("shadow:third:other:buy:mint", "other", "buy")?;
    f.db.buy(&id)?;
    let p = f.read()?;
    assert_eq!(p.first.contributors.len(), 2);
    assert_eq!(p.current.current_contributors.len(), 3);
    assert_eq!(p.current.contributor_orders.len(), 3);
    assert_eq!(
        p.current.contributor_orders[2].relative_to_sell,
        Check::Unknown(Reason::MissingAnchor)
    );
    Ok(())
}
