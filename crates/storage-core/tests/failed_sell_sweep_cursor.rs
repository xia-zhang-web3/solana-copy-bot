#[path = "common/source_write_off_db.rs"]
mod fixture;
#[path = "common/historical_migration_fixture.rs"]
mod historical;
use anyhow::Result;
use copybot_core_types::TokenQuantity;
use copybot_storage_core::{
    ExecutionFailedSellSweepVisit as Visit, SqliteStore, EXECUTION_STATUS_CANARY_FAILED,
};
use fixture::*;
use rusqlite::{params, Connection};

const TABLE: &str = "execution_failed_sell_sweep_cursors";
fn db() -> Result<Db> {
    Db::new(
        Kind::TerminalNoRoute { max_attempts: 1 },
        TokenQuantity::new(7000, 3),
    )
}

#[test]
fn cursor_is_route_scoped_wraps_after_removal_and_never_writes_business_rows() -> Result<()> {
    let mut d = db()?;
    let before = snapshot(&d.conn()?, &[TABLE])?;
    assert_eq!(
        d.store.advance_execution_failed_sell_sweep("other-route")?,
        Visit::SkippedOtherRoute
    );
    assert_eq!(
        d.store.advance_execution_failed_sell_sweep("tiny")?,
        Visit::Order(d.order.order_id.clone())
    );
    assert_eq!(snapshot(&d.conn()?, &[TABLE])?, before);
    d.conn()?
        .execute("DELETE FROM orders WHERE order_id=?1", [&d.order.order_id])?;
    d.reopen()?;
    assert_eq!(
        d.store.advance_execution_failed_sell_sweep("tiny")?,
        Visit::Wrapped
    );
    assert_eq!(
        d.store.advance_execution_failed_sell_sweep("other-route")?,
        Visit::Wrapped
    );
    assert!(d.store.advance_execution_failed_sell_sweep(" ").is_err());
    assert!(d
        .conn()?
        .execute(
            "INSERT INTO execution_failed_sell_sweep_cursors(route,last_rowid) VALUES('bad',5)",
            []
        )
        .is_err());
    Ok(())
}

#[test]
fn cursor_insert_update_affected_rows_and_commit_errors_roll_back_the_checkpoint() -> Result<()> {
    for phase in ["INSERT", "UPDATE"] {
        for (fault, action, expected) in [
            ("abort", "BEFORE", "injected_cursor_failure"),
            ("ignore", "BEFORE", "updated 0 rows"),
            ("missing", "AFTER", "Query returned no rows"),
            ("changed", "AFTER", "changed after write"),
            ("commit", "AFTER", "FOREIGN KEY constraint failed"),
        ] {
            let mut d = db()?;
            if phase == "UPDATE" {
                d.store.advance_execution_failed_sell_sweep("tiny")?;
            }
            let body=match fault {
                "abort"=>"SELECT RAISE(ABORT,'injected_cursor_failure');",
                "ignore"=>"SELECT RAISE(IGNORE);",
                "missing"=>"DELETE FROM execution_failed_sell_sweep_cursors WHERE route=NEW.route;",
                "changed"=>"UPDATE execution_failed_sell_sweep_cursors SET last_submit_ts='wrong',last_rowid=7 WHERE route=NEW.route;",
                _=>"INSERT INTO injected_child VALUES('absent');",
            };
            if fault == "commit" {
                d.conn()?.execute_batch("CREATE TABLE injected_parent(id TEXT PRIMARY KEY); CREATE TABLE injected_child(id TEXT REFERENCES injected_parent(id) DEFERRABLE INITIALLY DEFERRED);")?;
            }
            d.conn()?.execute_batch(&format!("CREATE TRIGGER inject_cursor {action} {phase} ON execution_failed_sell_sweep_cursors BEGIN {body} END;"))?;
            let before = snapshot(&d.conn()?, &[])?;
            let error = d
                .store
                .advance_execution_failed_sell_sweep("tiny")
                .expect_err(fault);
            assert!(
                format!("{error:#}").contains(expected),
                "{phase}/{fault}: {error:#}"
            );
            assert_eq!(snapshot(&d.conn()?, &[])?, before);
            d.reopen()?;
            assert_eq!(snapshot(&d.conn()?, &[])?, before);
            d.conn()?.execute_batch("DROP TRIGGER inject_cursor;")?;
            assert_eq!(
                d.store.advance_execution_failed_sell_sweep("tiny")?,
                if phase == "UPDATE" {
                    Visit::Wrapped
                } else {
                    Visit::Order(d.order.order_id.clone())
                }
            );
        }
    }
    Ok(())
}

#[test]
fn migration_0060_clean_upgrade_and_reopen_preserve_existing_history_and_cursor_0053() -> Result<()>
{
    let temp = tempfile::tempdir()?;
    let migration_dir = temp.path().join("migrations");
    std::fs::create_dir(&migration_dir)?;
    for file in std::fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../../migrations"))? {
        let file = file?;
        let name = file.file_name();
        if name.to_string_lossy().ends_with(".sql") && name.to_string_lossy().as_ref() < "0060" {
            std::fs::copy(file.path(), migration_dir.join(name))?;
        }
    }
    let path = temp.path().join("upgrade.db");
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(&migration_dir)?;
    let now = "2026-09-07T12:00:00Z".parse()?;
    historical::buy(
        &Connection::open(&path)?,
        "upgrade-buy",
        "source-a",
        "receipt:upgrade-buy",
        now,
    )?;
    let through61 = temp.path().join("through61");
    historical::prefix(&through61, "0062")?;
    let before = snapshot(&Connection::open(&path)?, &["schema_migrations"])?;
    assert!(!before.contains_key(TABLE));
    assert_eq!(store.run_migrations(&through61)?, 2);
    assert_eq!(
        snapshot(&Connection::open(&path)?, &["schema_migrations", TABLE])?,
        before
    );
    drop(store);
    let mut reopened = SqliteStore::open(&path)?;
    assert_eq!(reopened.run_migrations(&through61)?, 0);
    assert_eq!(
        reopened.advance_execution_failed_sell_sweep("tiny")?,
        Visit::Wrapped
    );
    assert_eq!(
        snapshot(&Connection::open(&path)?, &["schema_migrations", TABLE])?,
        before
    );
    // Other tests create a clean current-schema DB using all migrations directly.
    Ok(())
}

#[test]
fn exact_cursor_sql_uses_index_seek_including_equal_timestamp_boundary() -> Result<()> {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    let d = db()?;
    let mut c = d.conn()?;
    let tx = c.transaction()?;
    for n in 0..20_000 {
        tx.execute("INSERT INTO orders(order_id,signal_id,route,submit_ts,status,client_order_id) VALUES(?1,?2,'noise-route',?3,?4,?1)",
            params![format!("noise-{n}"),d.order.signal_id,d.now.to_rfc3339(),EXECUTION_STATUS_CANARY_FAILED])?;
    }
    tx.commit()?;
    let source = include_str!("../src/execution_failed_sell_sweep_cursor.rs");
    let sql = source
        .split("const SAME_TIME_SQL: &str = \"")
        .nth(1)
        .unwrap()
        .split("\";")
        .next()
        .unwrap();
    let arguments = params![EXECUTION_STATUS_CANARY_FAILED, d.now.to_rfc3339(), 10];
    let plan = c
        .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?
        .query_map(arguments, |r| r.get::<_, String>(3))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    assert!(plan
        .iter()
        .any(|line| line.contains("SEARCH orders USING INDEX idx_orders_status_submit_ts")));
    assert!(plan
        .iter()
        .all(|line| !line.contains("SCAN") && !line.contains("TEMP B-TREE")));
    let steps = Arc::new(AtomicUsize::new(0));
    let counter = steps.clone();
    c.progress_handler(
        1,
        Some(move || {
            counter.fetch_add(1, Ordering::Relaxed);
            false
        }),
    );
    let row: i64 = c.query_row(
        sql,
        params![EXECUTION_STATUS_CANARY_FAILED, d.now.to_rfc3339(), 10],
        |r| r.get(0),
    )?;
    c.progress_handler(0, None::<fn() -> bool>);
    assert_eq!(row, 9);
    assert!(
        steps.load(Ordering::Relaxed) < 150,
        "equal-time keyset must seek, not walk 20k rows: {} steps",
        steps.load(Ordering::Relaxed)
    );
    eprintln!(
        "B34_R1_CURSOR_PLAN {plan:?}; VM steps={}",
        steps.load(Ordering::Relaxed)
    );
    Ok(())
}
