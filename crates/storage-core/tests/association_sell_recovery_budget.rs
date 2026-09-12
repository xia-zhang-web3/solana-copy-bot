#[path = "common/association_sell_fixture.rs"]
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
fn next_input(inbox: &mut AssociationInbox) -> Result<()> {
    inbox.persist(
        &Delivery {
            session: "over-limit".into(),
            sequence: 0,
            arrival_offset_ns: 0,
            event: DeliveryEvent::Session(SessionGap::Reset),
        },
        &CandidateGeneration::Unknown,
    )
}
fn unresolved(f: &mut F) -> Result<Vec<FirstBinding>> {
    f.admit(facts("leaderbuy", "leader", true))?;
    f.admit(facts(&f.our, "execution-wallet", true))?;
    let mut first = vec![];
    for n in 0..7 {
        let name = format!("sell-{n}");
        f.admit(facts(&name, "leader", false))?;
        first.push(f.inbox.sell_preparation(&name)?.unwrap().first);
    }
    f.drain()?;
    Ok(first)
}
fn check_first(inbox: &AssociationInbox, first: &[FirstBinding]) -> Result<()> {
    for b in first {
        let p = inbox
            .sell_preparation(&b.sell.admission.facts.signature)?
            .unwrap();
        assert_eq!(&p.first, b);
        assert_eq!(p.current.selected_chain, Check::Unknown(Reason::Recovery));
    }
    Ok(())
}
#[test]
fn b90r1_full_count_reopen_and_mid_recovery_restart_keep_first_and_reject_n_plus_one() -> Result<()>
{
    let mut f = F::new()?;
    let l = InboxLimits {
        count: 64,
        ..limits()
    };
    f.inbox = AssociationInbox::open(&f.db.path, l)?;
    let first = unresolved(&mut f)?;
    while f.inbox.usage()?.0 < l.count {
        session(&mut f)?;
    }
    let full = f.inbox.usage()?;
    assert!(next_input(&mut f.inbox).is_err());
    assert_eq!(f.inbox.usage()?, full);
    f.inbox = AssociationInbox::open(&f.db.path, l)?;
    assert_eq!(f.inbox.usage()?, full);
    assert!(f.inbox.has_sell_preparation_work()?);
    f.inbox.recover_sell_preparation()?;
    f.inbox.recover_sell_preparation()?;
    f.inbox = AssociationInbox::open(&f.db.path, l)?;
    assert!(f.drain()? >= 9);
    check_first(&f.inbox, &first)?;
    assert_eq!(f.inbox.usage()?.0, 64);
    assert!(next_input(&mut f.inbox).is_err());
    assert_eq!(
        f.db.conn()?
            .query_row("SELECT count(*) FROM association_sell_work", [], |r| r
                .get::<_, i64>(0))?,
        0
    );
    println!(
        "count boundary before={full:?} after={:?}",
        f.inbox.usage()?
    );
    Ok(())
}
#[test]
fn b90r1_exact_byte_budget_from_first_admission_reopens_and_drains_without_extra_charge(
) -> Result<()> {
    let mut f = F::new()?;
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("same-budget.sqlite");
    f.db.conn()?
        .execute("VACUUM INTO ?1", [path.to_str().unwrap()])?;
    let first = unresolved(&mut f)?;
    // Trailing session is part of the admitted input, not recovery headroom.
    session(&mut f)?;
    let (_, b) = f.inbox.usage()?;
    let l = InboxLimits {
        bytes: b,
        ..limits()
    };
    let mut inbox = AssociationInbox::open(&path, l)?;
    let c = f.db.conn()?;
    let mut q = c.prepare("SELECT delivery FROM association_inbox_events ORDER BY sequence")?;
    for wire in q.query_map([], |r| r.get::<_, String>(0))? {
        let d: Delivery = serde_json::from_str(&wire?)?;
        let candidate = match &d.event {
            DeliveryEvent::Admission(a) => f.db.store.association_candidate(&a.facts),
            _ => CandidateGeneration::Unknown,
        };
        inbox.persist_at(
            &d,
            &candidate,
            chrono::DateTime::from_timestamp(10, 0).unwrap(),
        )?;
        while inbox.has_sell_preparation_work()? {
            inbox.recover_sell_preparation()?;
        }
    }
    while inbox.has_sell_preparation_work()? {
        inbox.recover_sell_preparation()?;
    }
    assert_eq!(inbox.usage()?.1, b);
    assert!(next_input(&mut inbox).is_err());
    let full = inbox.usage()?;
    inbox = AssociationInbox::open(&path, l)?;
    assert_eq!(inbox.usage()?, full);
    for _ in 0..4 {
        inbox.recover_sell_preparation()?;
    }
    inbox = AssociationInbox::open(&path, l)?;
    let mut steps = 0;
    while inbox.has_sell_preparation_work()? {
        inbox.recover_sell_preparation()?;
        steps += 1;
        assert!(steps < 32);
    }
    check_first(&inbox, &first)?;
    assert!(inbox.usage()?.1 <= b);
    assert!(next_input(&mut inbox).is_err());
    println!(
        "byte boundary before={full:?} after={:?} steps={steps}",
        inbox.usage()?
    );
    Ok(())
}
#[test]
fn b90r1_recovery_cursor_ignore_rolls_back_before_open_succeeds() -> Result<()> {
    let mut f = F::new()?;
    unresolved(&mut f)?;
    f.db.conn()?.execute_batch("CREATE TRIGGER ignored BEFORE UPDATE ON association_sell_bootstrap BEGIN SELECT RAISE(IGNORE); END;")?;
    assert!(AssociationInbox::open(&f.db.path, limits()).is_err());
    assert!(!f.inbox.identity("leaderbuy")?.unwrap().recovery);
    assert!(!f.inbox.has_sell_preparation_work()?);
    Ok(())
}
