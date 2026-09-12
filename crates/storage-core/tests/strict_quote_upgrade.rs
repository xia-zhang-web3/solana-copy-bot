#[path = "common/strict_quote_fixture.rs"]
mod f;
use anyhow::Result;
use chrono::Utc;
use f::*;
#[test]
fn strict_quote_additive_accepted99_upgrade_preserves_first_legacy_and_migration_history(
) -> Result<()> {
    let mut f = fixture()?;
    let sql = f.db.conn()?;
    sql.execute("INSERT INTO execution_quote_canary_events(event_id,signal_id,wallet_id,token,side,quote_status,request_ts,quote_in_amount_raw,quote_out_amount_raw) VALUES('legacy-quote','legacy-signal','leader','mint','sell','ok','2026-09-09T00:00:00Z','123','456')",[])?;
    let before = f
        .inbox
        .load_ordered_source_sell_intent_history(ID)?
        .unwrap();
    sql.execute_batch("DROP TABLE ordered_sell_quote_results;DROP TABLE ordered_sell_quote_cursor;DELETE FROM schema_migrations WHERE version='0074_ordered_sell_quote_only.sql';")?;
    let history: Vec<(String, String)> = sql
        .prepare("SELECT version,applied_at FROM schema_migrations ORDER BY version")?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    assert!(f
        .db
        .store
        .claim_strict_sell_quote(limits(), ENDPOINT, || Utc::now())
        .is_err());
    let migrations = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../migrations"));
    assert_eq!(f.db.store.run_migrations(migrations)?, 1);
    assert_eq!(f.db.store.run_migrations(migrations)?, 0);
    let after:Vec<(String,String)>=sql.prepare("SELECT version,applied_at FROM schema_migrations WHERE version!='0074_ordered_sell_quote_only.sql' ORDER BY version")?.query_map([],|r|Ok((r.get(0)?,r.get(1)?)))?.collect::<rusqlite::Result<_>>()?;
    assert_eq!(after, history);
    assert_eq!(
        f.inbox
            .load_ordered_source_sell_intent_history(ID)?
            .unwrap(),
        before
    );
    let row:(String,String)=sql.query_row("SELECT quote_in_amount_raw,quote_out_amount_raw FROM execution_quote_canary_events WHERE event_id='legacy-quote'",[],|r|Ok((r.get(0)?,r.get(1)?)))?;
    assert_eq!(row, ("123".into(), "456".into()));
    let now = Utc::now();
    let c = claim(&f, now)?;
    f.db.store
        .complete_strict_sell_quote(&c, limits(), observation(&c, now), || now)?;
    assert_eq!(count(&f, "execution_quote_canary_events")?, 1);
    Ok(())
}
