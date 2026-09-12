#[path = "common/b97_automatic.rs"]
mod f;
use anyhow::Result;
use f::*;

#[test]
fn b97_ignore_abort_in_stage_roll_back_event_preparation_cursor_claim_and_intent() -> Result<()> {
    for table in [
        "source_sell_signature_claims",
        "ordered_source_sell_intents",
    ] {
        for action in ["IGNORE", "ABORT,'b97 refusal'"] {
            let mut f = new()?;
            f.anchors()?;
            let sell = facts("sell", "leader", false);
            f.admit(sell.clone())?;
            let before = protocol(&f)?;
            let money_before = money(&f)?;
            f.db.conn()?.execute_batch(&format!(
                "CREATE TRIGGER fault BEFORE INSERT ON {table} BEGIN SELECT RAISE({action}); END;"
            ))?;
            assert!(f.terminal(&sell, 3, 42, "block").is_err());
            assert_eq!(protocol(&f)?, before);
            assert_eq!(money(&f)?, money_before);
            pair(&f, 0)?;
            f.db.conn()?.execute_batch("DROP TRIGGER fault")?;
            // The failed terminal was not durable; restart marks that identity Recovery.
            automatic(&mut f)?;
            f.drain()?;
            pair(&f, 0)?;
            assert!(f.inbox.identity("sell")?.unwrap().terminal.is_none());
        }
    }
    Ok(())
}

#[test]
fn b97_continuation_cursor_ignore_rolls_back_new_claim_and_intent() -> Result<()> {
    let mut f = within()?;
    automatic(&mut f)?;
    while f.db.conn()?.query_row(
        "SELECT after_signature FROM association_sell_bootstrap",
        [],
        |r| r.get::<_, String>(0),
    )? != "leaderbuy"
    {
        f.inbox.recover_sell_preparation()?;
    }
    // 'sell' is next (receipt starts 'sig:'). The stage succeeds, but cursor may not.
    f.db.conn()?.execute_batch("CREATE TRIGGER fault BEFORE UPDATE ON association_sell_bootstrap BEGIN SELECT RAISE(IGNORE); END;")?;
    let before = protocol(&f)?;
    assert!(f.inbox.recover_sell_preparation().is_err());
    assert_eq!(protocol(&f)?, before);
    pair(&f, 0)?;
    Ok(())
}

#[test]
fn b97_busy_never_writes_stage_or_cursor() -> Result<()> {
    let mut f = within()?;
    automatic(&mut f)?;
    let before = protocol(&f)?;
    let c = f.db.conn()?;
    c.execute_batch("BEGIN IMMEDIATE")?;
    let error = f.inbox.recover_sell_preparation().unwrap_err();
    assert!(error.chain().any(|e| e
        .downcast_ref::<rusqlite::Error>()
        .is_some_and(|e| e.sqlite_error_code() == Some(rusqlite::ErrorCode::DatabaseBusy))));
    assert_eq!(protocol(&f)?, before);
    c.execute_batch("ROLLBACK")?;
    f.drain()?;
    pair(&f, 1)?;
    Ok(())
}

#[test]
fn b97_later_trigger_changed_preparation_is_caught_before_commit() -> Result<()> {
    let mut f = new()?;
    f.anchors()?;
    let sell = facts("sell", "leader", false);
    f.admit(sell.clone())?;
    let before = protocol(&f)?;
    // The initial preparation readback succeeds. A later stage statement changes
    // that already-read key, so final precommit verification must reject the turn.
    f.db.conn()?.execute_batch("CREATE TRIGGER fault AFTER INSERT ON ordered_source_sell_intents BEGIN UPDATE association_sell_preparations SET latest_evaluation='{}' WHERE signature=NEW.signature; END;")?;
    assert!(f.terminal(&sell, 3, 42, "block").is_err());
    assert_eq!(protocol(&f)?, before);
    pair(&f, 0)?;
    Ok(())
}
