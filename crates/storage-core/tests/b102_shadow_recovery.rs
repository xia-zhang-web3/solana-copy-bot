#[path = "common/b102_shadow.rs"]
mod f;
use anyhow::Result;
use f::*;
#[test]
fn b102_close_commit_survives_lost_wake_restart_duplicate_and_noop() -> Result<()> {
    let mut f = parked()?;
    let first = f.read()?.first;
    assert_eq!(
        close(&f, "close", "leader", "mint", 1.0, limits())?.closed_qty,
        1.0
    );
    assert_eq!(count(&f, WORK)?, 1);
    let saved = state(&f)?;
    assert_eq!(
        close(&f, "close", "leader", "mint", 1.0, limits())?.closed_qty,
        0.0
    );
    assert_eq!(state(&f)?, saved);
    // Both ordinary SQLite handles reopen; no wake, bootstrap reset or status edit.
    f.db.reopen()?;
    automatic(&mut f)?;
    f.drain()?;
    pair(&f, 1)?;
    assert_eq!(intent(&f, "sell")?.unwrap().first, first);
    assert_eq!(fresh(&f)?, OrderedSellDecision::ValidatedNow);
    assert_eq!(count(&f, WORK)?, 0);
    let saved = state(&f)?;
    close(&f, "new-no-lot", "leader", "mint", 1.0, limits())?;
    close(&f, "close", "leader", "mint", 1.0, limits())?;
    for _ in 0..3 {
        f.inbox.recover_sell_preparation()?;
    }
    assert_eq!(state(&f)?, saved);
    Ok(())
}
#[test]
fn b102_partial_unknown_and_unrelated_pairs_never_gain_authority() -> Result<()> {
    let mut f = parked()?;
    unrelated(&mut f)?;
    f.db.conn()?.execute_batch("CREATE TRIGGER untouched_other BEFORE UPDATE ON association_sell_preparations WHEN OLD.signature LIKE 'other-%' BEGIN SELECT RAISE(ABORT,'unrelated preparation visited'); END;")?;
    let first = f.read()?.first;
    close(&f, "partial", "leader", "mint", 0.5, limits())?;
    f.drain()?;
    pair(&f, 0)?;
    assert_eq!(f.read()?.first, first);
    assert!(!f.inbox.has_sell_preparation_work()?);
    assert_eq!(count(&f, WORK)?, 0);
    close(&f, "remaining", "leader", "mint", 0.5, limits())?;
    f.drain()?;
    pair(&f, 1)?;
    assert_eq!(intent(&f, "sell")?.unwrap().first, first);
    assert!(intent(&f, "other-wallet")?.is_none());
    assert!(intent(&f, "other-mint")?.is_none());
    Ok(())
}
#[test]
fn b102_bounded_pair_continuation_unknown_a_advances_b_and_new_close_resets_only_pair() -> Result<()>
{
    let mut f = new()?;
    missing_lot(&f, "leader", "mint")?;
    missing_lot(&f, "leader", "mint")?;
    f.anchors()?;
    for n in 0..13 {
        let a = facts(&format!("S{n:02}"), "leader", false);
        let candidate = if n == 0 {
            CandidateGeneration::Unknown
        } else {
            f.db.store.association_candidate(&a.facts)
        };
        f.event(DeliveryEvent::Admission(a.clone()), candidate)?;
        f.terminal(&a, 3, 42, "block")?;
    }
    f.drain()?;
    close(&f, "first-close", "leader", "mint", 1.0, limits())?;
    f.inbox.recover_sell_preparation()?;
    assert_eq!(cursor(&f)?, "S00");
    // Another real risk change before prior work is drained must revisit S00.
    close(&f, "second-close", "leader", "mint", 1.0, limits())?;
    assert_eq!(cursor(&f)?, "");
    let mut turns = 0;
    let mut prior = 0;
    while f.inbox.has_sell_preparation_work()? {
        f.inbox.recover_sell_preparation()?;
        let n = count(&f, "ordered_source_sell_intents")?;
        assert!((0..=1).contains(&(n - prior)));
        prior = n;
        turns += 1;
        assert!(turns <= 14);
    }
    assert_eq!(turns, 14);
    pair(&f, 12)?;
    assert!(intent(&f, "S00")?.is_none());
    assert!(intent(&f, "S12")?.is_some());
    assert_eq!(count(&f, WORK)?, 0);
    Ok(())
}
