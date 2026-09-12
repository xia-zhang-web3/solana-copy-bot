#[path = "common/b102_shadow.rs"]
mod f;
use anyhow::Result;
use f::*;
#[test]
fn b102_enqueue_ignored_changed_or_fifo_fault_rolls_back_entire_close() -> Result<()> {
    for trigger in [
        "CREATE TRIGGER fault BEFORE INSERT ON association_shadow_sell_work BEGIN SELECT RAISE(IGNORE); END;",
        "CREATE TRIGGER fault AFTER INSERT ON association_shadow_sell_work BEGIN UPDATE association_shadow_sell_work SET after_signature='skip'; END;",
        "CREATE TRIGGER fault AFTER INSERT ON shadow_closed_trades BEGIN SELECT RAISE(ABORT,'close fault'); END;",
    ] {
        let f=parked()?; let before=state(&f)?;
        f.db.conn()?.execute_batch(trigger)?;
        assert!(close(&f,"close","leader","mint",1.0,limits()).is_err());
        assert_eq!(state(&f)?,before);
        f.db.conn()?.execute_batch("DROP TRIGGER fault")?;
        close(&f,"close","leader","mint",1.0,limits())?;
        assert_eq!(count(&f,WORK)?,1);
    }
    Ok(())
}
#[test]
fn b102_recovery_rollback_retains_cursor_then_recovers_without_status_repair() -> Result<()> {
    let mut f = parked()?;
    close(&f, "close", "leader", "mint", 1.0, limits())?;
    let before = state(&f)?;
    f.db.conn()?.execute_batch("CREATE TRIGGER fault BEFORE UPDATE ON association_shadow_sell_work BEGIN SELECT RAISE(IGNORE); END;")?;
    assert!(f.inbox.recover_sell_preparation().is_err());
    assert_eq!(state(&f)?, before);
    f.db.conn()?.execute_batch("DROP TRIGGER fault")?;
    f.drain()?;
    pair(&f, 1)?;
    assert_eq!(count(&f, WORK)?, 0);
    Ok(())
}
#[test]
fn b102_exact_count_and_byte_boundaries_refuse_without_losing_risk_or_work() -> Result<()> {
    let f = parked()?;
    let (n, b) = f.inbox.usage()?;
    // One pair row; cursor reserves all four UTF-8 bytes of signature "sell".
    let charge = 512 + "leader".len() + "mint".len() + "sell".len();
    let exact = InboxLimits {
        count: n + 1,
        bytes: b + charge,
        ..limits()
    };
    for l in [
        InboxLimits { count: n, ..exact },
        InboxLimits {
            bytes: exact.bytes - 1,
            ..exact
        },
    ] {
        let before = state(&f)?;
        assert!(close(&f, "close", "leader", "mint", 1.0, l).is_err());
        assert_eq!(state(&f)?, before);
    }
    close(&f, "close", "leader", "mint", 1.0, exact)?;
    // Close only mutates financial tables plus precisely the newly charged row.
    assert_eq!(f.inbox.usage()?, (n + 1, b + charge));
    Ok(())
}
#[test]
fn b102_quota_capacity_returns_by_normal_cleanup_same_limits_retry() -> Result<()> {
    let mut f = parked()?;
    unrelated(&mut f)?;
    missing_lot(&f, "other", "mint")?;
    let (n, b) = f.inbox.usage()?;
    let l = InboxLimits {
        count: n + 1,
        bytes: b + 1024,
        ..limits()
    };
    close(&f, "other-close", "other", "mint", 1.0, l)?;
    let before = state(&f)?;
    assert!(close(&f, "target-close", "leader", "mint", 1.0, l).is_err());
    assert_eq!(state(&f)?, before);
    f.inbox = AssociationInbox::open_ordered_sell_consumer(&f.db.path, l)?;
    f.drain()?; // unrelated Unknown is consumed and its cursor deleted
    assert_eq!(count(&f, WORK)?, 0);
    close(&f, "target-close", "leader", "mint", 1.0, l)?;
    assert_eq!(count(&f, WORK)?, 1);
    // Same cap cannot accommodate intent/claim: rollback leaves continuation.
    let before = state(&f)?;
    assert!(f.inbox.recover_sell_preparation().is_err());
    assert_eq!(state(&f)?, before);
    Ok(())
}
