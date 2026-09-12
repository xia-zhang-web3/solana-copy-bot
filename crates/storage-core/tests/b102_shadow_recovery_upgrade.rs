#[path = "common/b102_shadow.rs"]
mod f;
use anyhow::Result;
use f::*;
const DROP: &str = "DROP TABLE association_shadow_sell_work; DROP INDEX b102_shadow_sell_pair; DROP INDEX b102_shadow_sell_pair_bytes; DELETE FROM schema_migrations WHERE version='0075_shadow_sell_recovery.sql';";
#[test]
fn b102_upgrade0074_parked_missing_origin_is_indexed_without_rebinding_or_backfill() -> Result<()> {
    let mut f = parked()?;
    let first = f.read()?.first;
    let initial = f.read()?.historical_initial;
    let sql = f.db.conn()?;
    sql.execute_batch(DROP)?;
    let before = state_without_work(&f)?;
    let prior_history = history(&sql)?;
    assert!(AssociationInbox::open_ordered_sell_consumer(&f.db.path, limits()).is_err());
    assert!(close(&f, "close", "leader", "mint", 1.0, limits()).is_err());
    assert_eq!(state_without_work(&f)?, before);
    assert_eq!(
        f.db.store.run_migrations(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))?,
        1
    );
    assert_eq!(history(&sql)?, prior_history);
    // No consumer bootstrap is needed to discover this old preparation's pair.
    close(&f, "close", "leader", "mint", 1.0, limits())?;
    assert_eq!(count(&f, WORK)?, 1);
    automatic(&mut f)?;
    f.drain()?;
    pair(&f, 1)?;
    assert_eq!(f.read()?.first, first);
    assert_eq!(f.read()?.historical_initial, initial);
    Ok(())
}
fn state_without_work(f: &F) -> Result<Vec<String>> {
    let mut s = protocol(f)?;
    s.extend(money(f)?);
    Ok(s)
}
fn history(c: &rusqlite::Connection) -> Result<Vec<(String, String)>> {
    Ok(c.prepare(
        "SELECT version,applied_at FROM schema_migrations WHERE version<'0075' ORDER BY version",
    )?
    .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
    .collect::<rusqlite::Result<_>>()?)
}
#[test]
fn b102_missing_or_changed_required_schema_refuses_but_legacy_close_stays_compatible() -> Result<()>
{
    for ddl in [DROP,
        "DROP INDEX b102_shadow_sell_pair",
        "DROP INDEX b102_shadow_sell_pair; CREATE INDEX b102_shadow_sell_pair ON association_sell_preparations(signature);",
        "DROP TABLE association_shadow_sell_work; CREATE TABLE association_shadow_sell_work(wallet TEXT,token TEXT,after_signature TEXT);",
    ] {
        let f=parked()?;
        f.db.conn()?.execute_batch(ddl)?;
        assert!(AssociationInbox::open_ordered_sell_consumer(&f.db.path,limits()).is_err());
        assert!(close(&f,"close","leader","mint",1.0,limits()).is_err());
        assert_eq!(count(&f,"shadow_closed_trades")?,0);
        assert_eq!(f.db.store.close_shadow_lots_fifo_atomic_exact_with_context("legacy","leader","mint",1.0,
            Some(TokenQuantity::new(1000,3)),0.000001,copybot_storage_core::SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE,f.db.now)?.closed_qty,1.0);
    }
    Ok(())
}

#[test]
#[ignore = "requires retained baseline0074 DB produced by accepted100/R1 executable"]
fn b102_actual_accepted0074_missing_origin_db_upgrade_and_close() -> Result<()> {
    let input = std::path::PathBuf::from(std::env::var("B102_0074_DB")?);
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("upgraded.sqlite");
    std::fs::copy(input, &path)?;
    let mut store = copybot_storage_core::SqliteStore::open(&path)?;
    let sql = rusqlite::Connection::open(&path)?;
    let (signature,binding,initial):(String,String,String)=sql.query_row(
        "SELECT signature,first_binding,initial_evaluation FROM association_sell_preparations LIMIT 1",[],
        |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
    assert!(sql
        .query_row(
            "SELECT latest_evaluation FROM association_sell_preparations LIMIT 1",
            [],
            |r| r.get::<_, String>(0)
        )?
        .contains("MissingShadowOrigin"));
    let prior = history(&sql)?;
    assert_eq!(
        sql.query_row("SELECT max(version) FROM schema_migrations", [], |r| r
            .get::<_, String>(
            0
        ))?,
        "0074_ordered_sell_quote_only.sql"
    );
    assert!(AssociationInbox::open_ordered_sell_consumer(&path, limits()).is_err());
    assert_eq!(
        store.run_migrations(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))?,
        1
    );
    assert_eq!(history(&sql)?, prior);
    let value: serde_json::Value = serde_json::from_str(&binding)?;
    let facts = &value["sell"]["admission"]["facts"];
    let wallet = facts["wallet"].as_str().unwrap();
    let token = facts["token_in"].as_str().unwrap();
    let lots = store.list_shadow_lots(wallet, token)?;
    assert_eq!(lots.len(), 1);
    let lot = &lots[0];
    store.close_shadow_lots_fifo_atomic_exact_with_recovery(
        "upgrade-close",
        wallet,
        token,
        lot.qty,
        lot.qty_exact,
        0.000001,
        copybot_storage_core::SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE,
        chrono::Utc::now(),
        Some(limits()),
    )?;
    assert_eq!(
        sql.query_row(
            "SELECT count(*) FROM association_shadow_sell_work",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        1
    );
    let mut consumer = AssociationInbox::open_ordered_sell_consumer(&path, limits())?;
    exhaust(&mut consumer)?;
    let prepared = consumer.sell_preparation(&signature)?.unwrap();
    assert_eq!(serde_json::to_string(&prepared.first)?, binding);
    assert_eq!(
        serde_json::to_string(&prepared.historical_initial)?,
        initial
    );
    assert!(consumer
        .load_ordered_source_sell_intent_history(&format!("source-sell:{signature}"))?
        .is_some());
    assert_eq!(
        sql.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))?,
        "ok"
    );
    Ok(())
}
