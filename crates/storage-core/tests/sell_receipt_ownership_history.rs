#[path = "common/sell_cash_day_fixture.rs"]
mod fixture;
#[path = "common/sell_receipt_history.rs"]
mod history;
use anyhow::Result;
use copybot_storage_core::SqliteStore;
use fixture::*;

#[test]
fn historical_duplicate_settlements_replay_after_close_without_repair() -> Result<()> {
    let db = Db::new(2, 6, 0, 1, 0)?;
    let first = first(&db, as_of())?;
    let mut second = db.facts(1, 0);
    second.order_id = "exec-canary:historical-second".into();
    let expected = history::settle(&db, second.clone(), "tiny", as_of())?.settlement;
    assert_eq!(expected.remaining_quantity.raw(), 0);
    let before = snapshot(&db.conn()?)?;
    let reopened = SqliteStore::open(&db.path)?;
    for (facts, expected) in [(db.facts(1, 0), first), (second, expected)] {
        let replay = reopened.apply_execution_canary_sell_settlement(&facts, as_of())?;
        assert!(replay.already_accounted);
        assert_eq!(replay.settlement, expected);
    }
    assert!(reopened.execution_canary_sell_cash_day(as_of()).is_err());
    assert_eq!(snapshot(&db.conn()?)?, before);
    Ok(())
}
