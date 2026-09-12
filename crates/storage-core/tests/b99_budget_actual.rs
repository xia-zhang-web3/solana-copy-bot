#[path = "common/b97_automatic.rs"]
mod f;
#[path = "common/b99_slow_oracle.rs"]
mod oracle;
use anyhow::Result;
use copybot_storage_core::association_inbox::{AssociationInbox, InboxLimits};
use f::*;
fn check(f: &F, strict: bool) -> Result<()> {
    assert_eq!(f.inbox.usage()?, oracle::usage(&f.db.conn()?, strict)?);
    Ok(())
}
#[test]
fn b99_actual_automatic_staging_fresh_dependency_restart_equals_source_oracle() -> Result<()> {
    let mut f = new()?;
    check(&f, true)?;
    insert(&f, &facts("origin", "leader", true))?;
    f.anchors()?;
    check(&f, true)?;
    f.sell()?;
    check(&f, true)?;
    while f.inbox.has_sell_preparation_work()? {
        f.inbox.recover_sell_preparation()?;
        check(&f, true)?;
    }
    pair(&f, 0)?;
    let origin = facts("origin", "leader", true);
    f.admit(origin.clone())?;
    check(&f, true)?;
    f.terminal(&origin, 4, 42, "block")?;
    check(&f, true)?;
    while f.inbox.has_sell_preparation_work()? {
        f.inbox.recover_sell_preparation()?;
        check(&f, true)?;
    }
    pair(&f, 1)?;
    let money = money(&f)?;
    let before = f.inbox.usage()?;
    assert!(matches!(stage(&mut f)?, OrderedSellStage::Existing(_)));
    assert_eq!(f.inbox.usage()?, before);
    automatic(&mut f)?;
    check(&f, true)?;
    while f.inbox.has_sell_preparation_work()? {
        f.inbox.recover_sell_preparation()?;
        check(&f, true)?;
    }
    assert_eq!(f::money(&f)?, money);
    Ok(())
}
#[test]
fn b99_manual_stage_has_no_new_cap_but_next_automatic_check_sees_both_records() -> Result<()> {
    let mut f = within()?; // Observation API prepares only; no strict records yet.
    check(&f, false)?;
    let prior = oracle::usage(&f.db.conn()?, true)?;
    let l = InboxLimits {
        count: prior.0,
        bytes: prior.1,
        ..limits()
    };
    let mut automatic = AssociationInbox::open_ordered_sell_consumer(&f.db.path, l)?;
    // This public manual operation retains its existing no-policy-cap contract.
    assert!(matches!(stage(&mut f)?, OrderedSellStage::Inserted(_)));
    assert_eq!(f.inbox.usage()?, prior);
    let after = oracle::usage(&f.db.conn()?, true)?;
    assert_eq!(after.0, prior.0 + 2);
    assert_eq!(automatic.usage()?, after);
    let saved = protocol(&f)?;
    assert!(automatic
        .persist(&pulse(1), &CandidateGeneration::Unknown)
        .is_err());
    assert_eq!(protocol(&f)?, saved);
    assert!(AssociationInbox::open_ordered_sell_consumer(&f.db.path, l).is_err());
    assert_eq!(protocol(&f)?, saved);
    let fresh = AssociationInbox::open_ordered_sell_consumer(&f.db.path, limits())?;
    assert_eq!(fresh.usage()?, oracle::usage(&f.db.conn()?, true)?);
    Ok(())
}
