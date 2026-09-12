#[path = "common/source_sell_fixture.rs"]
mod fixture;
use anyhow::Result;
use copybot_storage_core::{ExecutionSourceSellStagingVisit as Visit, SqliteStore};
use fixture::Db;
use rusqlite::Connection;

fn staged(d: &Db, id: &str) -> Result<()> {
    let event = d.observed(id, "source-a")?;
    assert!(matches!(
        d.store
            .stage_execution_source_sell_intent(&event, &d.position()?)?,
        copybot_storage_core::ExecutionSourceSellOutcome::Inserted(_)
    ));
    Ok(())
}

#[test]
fn checkpoint_before_decode_survives_crash_deleted_row_and_new_arrivals() -> Result<()> {
    let mut d = Db::new()?;
    d.proven("buy-a", "source-a")?;
    staged(&d, "b")?;
    staged(&d, "a")?;
    d.conn()?.execute(
        "UPDATE execution_source_sell_intents SET staged_at='bad' WHERE event_signature='a'",
        [],
    )?;
    let visit = d.store.advance_execution_source_sell_staging()?;
    assert!(
        matches!(visit, Visit::Row { rowid: 2, intent_id: Some(ref id) } if id == "source-sell:a")
    );
    // No decoder/promotion call: simulate a crash immediately after checkpoint commit.
    d.reopen()?;
    d.conn()?.execute(
        "DELETE FROM execution_source_sell_intents WHERE rowid=2",
        [],
    )?;
    for n in 0..40 {
        staged(&d, &format!("new-{n}"))?;
    }
    assert!(
        matches!(d.store.advance_execution_source_sell_staging()?, Visit::Row { rowid: 1, intent_id: Some(ref id) } if id == "source-sell:b")
    );
    assert_eq!(
        d.store.advance_execution_source_sell_staging()?,
        Visit::Wrapped
    );
    d.reopen()?;
    assert!(
        matches!(d.store.advance_execution_source_sell_staging()?, Visit::Row { intent_id: Some(ref id), .. } if id == "source-sell:new-39")
    );
    Ok(())
}

#[test]
fn raw_malformed_keys_also_checkpoint_and_missing_schema_is_an_error() -> Result<()> {
    let mut d = Db::new()?;
    d.proven("buy-a", "source-a")?;
    staged(&d, "b")?;
    staged(&d, "bad-key")?;
    d.conn()?.execute(
        "UPDATE execution_source_sell_intents SET intent_id=x'ff' WHERE rowid=2",
        [],
    )?;
    assert_eq!(
        d.store.advance_execution_source_sell_staging()?,
        Visit::Row {
            rowid: 2,
            intent_id: None
        }
    );
    d.reopen()?;
    assert!(matches!(
        d.store.advance_execution_source_sell_staging()?,
        Visit::Row { rowid: 1, .. }
    ));
    d.conn()?
        .execute_batch("DROP TABLE execution_source_sell_intents")?;
    assert!(format!(
        "{:#}",
        d.store.advance_execution_source_sell_staging().unwrap_err()
    )
    .contains("no such table"));
    Ok(())
}

#[test]
fn upgrade_0062_preserves_source_and_money_and_reopens_idempotently() -> Result<()> {
    let mut d = Db::new()?;
    d.proven("buy-a", "source-a")?;
    staged(&d, "a")?;
    d.conn()?.execute_batch("DROP TABLE execution_source_sell_staging_cursor;
        DELETE FROM schema_migrations WHERE version='0062_execution_source_sell_staging_cursor.sql';")?;
    let before = fixture::snapshot(&d.conn()?, &["schema_migrations"])?;
    assert_eq!(
        d.store
            .run_migrations(std::path::Path::new(fixture::MIGRATIONS))?,
        1
    );
    assert_eq!(
        fixture::snapshot(
            &d.conn()?,
            &["schema_migrations", "execution_source_sell_staging_cursor"]
        )?,
        before
    );
    d.reopen()?;
    assert_eq!(
        d.store
            .run_migrations(std::path::Path::new(fixture::MIGRATIONS))?,
        0
    );
    assert!(
        matches!(d.store.advance_execution_source_sell_staging()?, Visit::Row { intent_id: Some(ref id), .. } if id == "source-sell:a")
    );
    Ok(())
}

#[test]
fn production_seek_does_not_scan_a_large_retained_prefix() -> Result<()> {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    let d = Db::new()?;
    d.proven("buy-a", "source-a")?;
    staged(&d, "a")?;
    let mut c = d.conn()?;
    // Synthetic retained history, including malformed payloads. Cursor must seek raw rowids.
    let tx = c.transaction()?;
    for n in 0..20_000 {
        tx.execute("INSERT INTO execution_source_sell_intents SELECT
            'source-sell:'||?1,?1,source_wallet,dex,token,token_out,amount_in,amount_out,slot,'malformed',
            amount_in_raw,amount_in_decimals,amount_out_raw,amount_out_decimals,
            position_id,buy_fill_id,buy_order_id,buy_signal_id,buy_tx_signature,buy_execution_wallet,staged_at
            FROM execution_source_sell_intents WHERE rowid=1", [format!("raw-{n}")])?;
    }
    tx.commit()?;
    let source = include_str!("../src/execution_source_sell_staging_cursor.rs");
    let sql = source
        .split("const NEXT: &str =")
        .nth(1)
        .unwrap()
        .trim_start()
        .strip_prefix('"')
        .unwrap()
        .split("\";")
        .next()
        .unwrap();
    let plan = c
        .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?
        .query_map([10], |r| r.get::<_, String>(3))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    assert!(plan
        .iter()
        .any(|s| s.contains("SEARCH execution_source_sell_intents USING INTEGER PRIMARY KEY")));
    assert!(plan
        .iter()
        .all(|s| !s.contains("SCAN") && !s.contains("TEMP B-TREE")));
    let steps = Arc::new(AtomicUsize::new(0));
    let count = steps.clone();
    c.progress_handler(
        1,
        Some(move || {
            count.fetch_add(1, Ordering::Relaxed);
            false
        }),
    );
    assert_eq!(c.query_row(sql, [10], |r| r.get::<_, i64>(0))?, 9);
    c.progress_handler(0, None::<fn() -> bool>);
    assert!(steps.load(Ordering::Relaxed) < 80);
    eprintln!(
        "B46_CURSOR_PLAN {plan:?}; VM steps={}",
        steps.load(Ordering::Relaxed)
    );
    c.execute(
        "INSERT INTO execution_source_sell_staging_cursor VALUES(1,10)",
        [],
    )?;
    assert!(matches!(
        d.store.advance_execution_source_sell_staging()?,
        Visit::Row { rowid: 9, .. }
    ));
    drop(c);
    let reopened = SqliteStore::open(&d.path)?;
    assert!(matches!(
        reopened.advance_execution_source_sell_staging()?,
        Visit::Row { rowid: 8, .. }
    ));
    assert_eq!(
        Connection::open(&d.path)?.query_row(
            "SELECT count(*) FROM execution_source_sell_intents",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        20_001
    );
    Ok(())
}
