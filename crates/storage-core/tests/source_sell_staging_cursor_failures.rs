#[path = "common/source_sell_fixture.rs"]
mod fixture;
use anyhow::Result;
use fixture::Db;

#[test]
fn checkpoint_insert_update_readback_and_commit_failures_are_not_local_skips() -> Result<()> {
    for phase in ["INSERT", "UPDATE"] {
        for fault in ["abort", "ignore", "missing", "changed", "commit"] {
            let mut d = Db::new()?;
            d.proven("buy-a", "source-a")?;
            let event = d.observed("a", "source-a")?;
            d.store
                .stage_execution_source_sell_intent(&event, &d.position()?)?;
            if phase == "UPDATE" {
                d.store.advance_execution_source_sell_staging()?;
            }
            let (when, body, expected) = match fault {
                "abort" => (
                    "BEFORE",
                    "SELECT RAISE(ABORT,'checkpoint_failed');",
                    "checkpoint_failed",
                ),
                "ignore" => ("BEFORE", "SELECT RAISE(IGNORE);", "updated 0 rows"),
                "missing" => (
                    "AFTER",
                    "DELETE FROM execution_source_sell_staging_cursor;",
                    "Query returned no rows",
                ),
                "changed" => (
                    "AFTER",
                    "UPDATE execution_source_sell_staging_cursor SET last_rowid=12345;",
                    "changed after write",
                ),
                _ => (
                    "AFTER",
                    "INSERT INTO injected_child VALUES('absent');",
                    "FOREIGN KEY constraint failed",
                ),
            };
            if fault == "commit" {
                d.conn()?.execute_batch("CREATE TABLE injected_parent(id TEXT PRIMARY KEY);
                    CREATE TABLE injected_child(id TEXT REFERENCES injected_parent(id) DEFERRABLE INITIALLY DEFERRED);")?;
            }
            d.conn()?.execute_batch(&format!(
                "CREATE TRIGGER fail_checkpoint {when} {phase}
                ON execution_source_sell_staging_cursor BEGIN {body} END;"
            ))?;
            let before = fixture::snapshot(&d.conn()?, &[])?;
            let error = d
                .store
                .advance_execution_source_sell_staging()
                .expect_err(fault);
            assert!(
                format!("{error:#}").contains(expected),
                "{phase}/{fault}: {error:#}"
            );
            assert_eq!(fixture::snapshot(&d.conn()?, &[])?, before);
            d.reopen()?;
            assert_eq!(fixture::snapshot(&d.conn()?, &[])?, before);
        }
    }
    Ok(())
}

#[test]
fn schema_and_busy_checkpoint_errors_surface() -> Result<()> {
    let d = Db::new()?;
    let locked = d.conn()?;
    locked.execute_batch("BEGIN IMMEDIATE")?;
    // SqliteStore's existing bounded busy/retry policy remains authoritative.
    assert!(d.store.advance_execution_source_sell_staging().is_err());
    locked.execute_batch("ROLLBACK; DROP TABLE execution_source_sell_staging_cursor;")?;
    assert!(format!(
        "{:#}",
        d.store.advance_execution_source_sell_staging().unwrap_err()
    )
    .contains("no such table"));
    Ok(())
}
