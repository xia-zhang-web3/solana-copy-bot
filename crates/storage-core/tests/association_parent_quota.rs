#[path = "common/association_parent_fixture.rs"]
mod f;
use anyhow::Result;
use copybot_core_types::association_delivery::*;
use copybot_storage_core::{association_inbox::*, association_sell_preparation::*};
use f::*;
fn session(f: &mut F) -> Result<()> {
    f.event(
        DeliveryEvent::Session(SessionGap::Reset),
        CandidateGeneration::Unknown,
    )
}
fn overflow(inbox: &mut AssociationInbox) -> Result<()> {
    inbox.persist(
        &Delivery {
            session: "overflow".into(),
            sequence: 0,
            arrival_offset_ns: 0,
            event: DeliveryEvent::Session(SessionGap::Reset),
        },
        &CandidateGeneration::Unknown,
    )
}
#[test]
fn b91_full_graph_count_and_bytes_reopen_with_reserved_jobs_and_cursors() -> Result<()> {
    let mut f = F::new()?;
    let l = InboxLimits {
        count: 64,
        ..limits()
    };
    f.inbox = AssociationInbox::open(&f.db.path, l)?;
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("base.sqlite");
    f.db.conn()?
        .execute("VACUUM INTO ?1", [path.to_str().unwrap()])?;
    ready(&mut f)?;
    while f.inbox.usage()?.0 < 64 {
        session(&mut f)?;
    }
    f.drain()?;
    let first = f.read()?.first;
    let full = f.inbox.usage()?;
    assert_eq!(full.0, 64);
    assert!(overflow(&mut f.inbox).is_err());
    f.inbox = AssociationInbox::open(&f.db.path, l)?;
    assert_eq!(f.inbox.usage()?, full);
    f.inbox.recover_sell_preparation()?;
    f.inbox = AssociationInbox::open(&f.db.path, l)?;
    f.drain()?;
    assert_eq!(f.inbox.usage()?, full);
    assert_eq!(f.read()?.first, first);
    assert_eq!(
        f.read()?.current.selected_chain,
        Check::ProviderOrderedAcrossBlocks
    );
    let mut exact = AssociationInbox::open(&path, InboxLimits { bytes: full.1, ..l })?;
    let c = f.db.conn()?;
    let mut q = c.prepare("SELECT delivery FROM association_inbox_events ORDER BY sequence")?;
    for wire in q.query_map([], |r| r.get::<_, String>(0))? {
        let d: Delivery = serde_json::from_str(&wire?)?;
        let candidate = match &d.event {
            DeliveryEvent::Admission(a) => f.db.store.association_candidate(&a.facts),
            _ => CandidateGeneration::Unknown,
        };
        exact.persist_at(
            &d,
            &candidate,
            chrono::DateTime::from_timestamp(10, 0).unwrap(),
        )?;
        while exact.has_sell_preparation_work()? {
            exact.recover_sell_preparation()?;
        }
    }
    assert_eq!(exact.usage()?, full);
    assert!(overflow(&mut exact).is_err());
    exact = AssociationInbox::open(&path, InboxLimits { bytes: full.1, ..l })?;
    exact.recover_sell_preparation()?;
    exact = AssociationInbox::open(&path, InboxLimits { bytes: full.1, ..l })?;
    while exact.has_sell_preparation_work()? {
        exact.recover_sell_preparation()?;
    }
    assert_eq!(exact.usage()?, full);
    assert_eq!(exact.sell_preparation("sell")?.unwrap().first, first);
    assert!(overflow(&mut exact).is_err());
    println!(
        "B91_GRAPH_FULL count={} bytes={} reopen=exact drain=exact Nplus1=rejected",
        full.0, full.1
    );
    Ok(())
}
#[test]
fn b91_late_frontier_continuation_survives_restart_with_multiple_dependents() -> Result<()> {
    let mut f = F::new()?;
    anchors(&mut f, false)?;
    for n in 0..6 {
        let mut a = facts(&format!("sell-{n}"), "leader", false);
        a.facts.slot = 60;
        f.admit(a.clone())?;
        f.terminal(&a, 3, 60, &hash(6))?;
    }
    for e in graph().into_iter().skip(1) {
        put(&mut f, e)?;
    }
    f.drain()?;
    let first = f.read()?.first;
    put(&mut f, graph()[0].clone())?;
    assert!(f.inbox.has_sell_preparation_work()?);
    f.inbox.recover_sell_preparation()?;
    f.inbox = AssociationInbox::open(&f.db.path, limits())?;
    f.drain()?;
    for name in std::iter::once("sell".into()).chain((0..6).map(|n| format!("sell-{n}"))) {
        assert_eq!(
            f.inbox
                .sell_preparation(&name)?
                .unwrap()
                .current
                .selected_chain,
            Check::ProviderOrderedAcrossBlocks
        );
    }
    assert_eq!(f.read()?.first, first);
    let pending: i64 = f.db.conn()?.query_row(
        "SELECT count(*) FROM association_parent_work WHERE pending!=0",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(pending, 0);
    Ok(())
}

#[test]
fn b91_empty_graph_full_inbox_recovery_allocates_no_parent_reservations() -> Result<()> {
    let mut f = F::new()?;
    anchors(&mut f, false)?;
    f.drain()?;
    assert_eq!(
        f.read()?.current.selected_chain,
        Check::Unknown(Reason::ParentGap)
    );
    assert_eq!(
        f.db.conn()?.query_row(
            "SELECT count(*) FROM association_parent_dependencies",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        0
    );
    while f.inbox.usage()?.0 < 64 {
        session(&mut f)?;
    }
    let first = f.read()?.first;
    let full = f.inbox.usage()?;
    let l = InboxLimits {
        count: full.0,
        bytes: full.1,
        ..limits()
    };
    f.inbox = AssociationInbox::open(&f.db.path, l)?;
    f.drain()?;
    assert_eq!(f.inbox.usage()?, full);
    assert_eq!(f.read()?.first, first);
    assert!(overflow(&mut f.inbox).is_err());
    Ok(())
}
