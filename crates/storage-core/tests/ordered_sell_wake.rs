#[path = "common/b97_automatic.rs"]
mod f;
use anyhow::Result;
use f::*;

#[test]
fn new_terminal_intent_wakes_once_without_replaying_duplicates_or_existing_history() -> Result<()> {
    let mut f = new()?;
    assert!(!f.inbox.take_ordered_sell_wake());
    f.anchors()?;
    let sell = facts("sell", "leader", false);
    f.admit(sell.clone())?;
    pair(&f, 0)?;
    assert!(!f.inbox.take_ordered_sell_wake());

    f.terminal(&sell, 3, 42, "block")?;
    pair(&f, 1)?;
    assert!(f.inbox.take_ordered_sell_wake());
    assert!(!f.inbox.take_ordered_sell_wake());
    f.terminal(&sell, 3, 42, "block")?;
    f.drain()?;
    pair(&f, 1)?;
    assert!(!f.inbox.take_ordered_sell_wake());
    automatic(&mut f)?;
    f.drain()?;
    pair(&f, 1)?;
    assert!(!f.inbox.take_ordered_sell_wake());
    Ok(())
}

#[test]
fn failed_stage_or_later_precommit_proof_never_publish_a_wake() -> Result<()> {
    for fault in [
        "CREATE TRIGGER fault BEFORE INSERT ON ordered_source_sell_intents BEGIN SELECT RAISE(IGNORE); END;",
        "CREATE TRIGGER fault BEFORE INSERT ON ordered_source_sell_intents BEGIN SELECT RAISE(ABORT,'wake refusal'); END;",
        "CREATE TRIGGER fault AFTER INSERT ON ordered_source_sell_intents BEGIN UPDATE association_sell_preparations SET latest_evaluation='{}' WHERE signature=NEW.signature; END;",
    ] {
        let mut f = new()?;
        f.anchors()?;
        let sell = facts("sell", "leader", false);
        f.admit(sell.clone())?;
        assert!(!f.inbox.take_ordered_sell_wake());
        let before = protocol(&f)?;
        f.db.conn()?.execute_batch(fault)?;
        assert!(f.terminal(&sell, 3, 42, "block").is_err());
        assert_eq!(protocol(&f)?, before);
        pair(&f, 0)?;
        assert!(!f.inbox.take_ordered_sell_wake());

        f.db.conn()?.execute_batch("DROP TRIGGER fault")?;
        f.terminal(&sell, 3, 42, "block")?;
        pair(&f, 1)?;
        assert!(f.inbox.take_ordered_sell_wake());
        assert!(!f.inbox.take_ordered_sell_wake());
    }
    Ok(())
}

#[test]
fn recovery_wakes_only_for_its_newly_committed_intent() -> Result<()> {
    let mut f = within()?;
    pair(&f, 0)?;
    assert!(!f.inbox.take_ordered_sell_wake());
    automatic(&mut f)?;
    let mut prior = count(&f, "ordered_source_sell_intents")?;
    let initial = f.inbox.take_ordered_sell_wake();
    assert_eq!(initial, prior > 0);
    let mut wakes = usize::from(initial);
    let mut turns = 0;
    while f.inbox.has_sell_preparation_work()? {
        f.inbox.recover_sell_preparation()?;
        let next = count(&f, "ordered_source_sell_intents")?;
        let wake = f.inbox.take_ordered_sell_wake();
        assert_eq!(wake, next > prior);
        assert!(!f.inbox.take_ordered_sell_wake());
        wakes += usize::from(wake);
        prior = next;
        turns += 1;
        assert!(turns < 100);
    }
    pair(&f, 1)?;
    assert_eq!(wakes, 1);
    f.inbox.recover_sell_preparation()?;
    assert!(!f.inbox.take_ordered_sell_wake());
    automatic(&mut f)?;
    f.drain()?;
    assert!(!f.inbox.take_ordered_sell_wake());
    Ok(())
}

#[test]
fn multiple_new_intents_coalesce_until_taken() -> Result<()> {
    let mut f = new()?;
    f.anchors()?;
    for signature in ["sell-A", "sell-B"] {
        let sell = facts(signature, "leader", false);
        f.admit(sell.clone())?;
        f.terminal(&sell, 3, 42, "block")?;
    }
    pair(&f, 2)?;
    assert!(f.inbox.take_ordered_sell_wake());
    assert!(!f.inbox.take_ordered_sell_wake());
    f.drain()?;
    assert!(!f.inbox.take_ordered_sell_wake());
    Ok(())
}
