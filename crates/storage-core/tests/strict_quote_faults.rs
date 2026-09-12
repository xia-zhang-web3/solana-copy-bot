#[path = "common/strict_quote_fixture.rs"]
mod f;
use anyhow::Result;
use chrono::Utc;
use f::*;
#[test]
fn strict_quote_ignored_abort_conflicting_insert_rolls_back_cursor() -> Result<()> {
    for trigger in [
        "SELECT RAISE(IGNORE);",
        "SELECT RAISE(ABORT,'fixture abort');",
        "UPDATE ordered_sell_quote_results SET owner='wrong' WHERE intent_id=NEW.intent_id;",
    ] {
        let f = fixture()?;
        let sql = f.db.conn()?;
        let when = if trigger.starts_with("UPDATE") {
            "AFTER"
        } else {
            "BEFORE"
        };
        sql.execute_batch(&format!("CREATE TRIGGER fixture_fault {when} INSERT ON ordered_sell_quote_results BEGIN {trigger} END;"))?;
        assert!(f
            .db
            .store
            .claim_strict_sell_quote(limits(), ENDPOINT, || Utc::now())
            .is_err());
        assert_eq!(count(&f, "ordered_sell_quote_results")?, 0);
        assert_eq!(count(&f, "ordered_sell_quote_cursor")?, 0);
        sql.execute_batch("DROP TRIGGER fixture_fault")?;
        claim(&f, Utc::now())?;
    }
    Ok(())
}
#[test]
fn strict_quote_completion_ignore_abort_readback_and_commit_failure() -> Result<()> {
    for variant in ["ignore", "abort", "readback", "commit"] {
        let f = fixture()?;
        let now = Utc::now();
        let c = claim(&f, now)?;
        let sql = f.db.conn()?;
        let body = match variant {
            "ignore" => "SELECT RAISE(IGNORE);",
            "abort" => "SELECT RAISE(ABORT,'fixture abort');",
            "readback" => {
                "UPDATE ordered_sell_quote_results SET record='{}' WHERE intent_id=NEW.intent_id;"
            }
            "commit" => {
                sql.execute_batch("CREATE TABLE fault_parent(id PRIMARY KEY); CREATE TABLE fault_child(id REFERENCES fault_parent(id) DEFERRABLE INITIALLY DEFERRED);")?;
                "INSERT INTO fault_child VALUES(1);"
            }
            _ => unreachable!(),
        };
        let when = if variant == "readback" || variant == "commit" {
            "AFTER"
        } else {
            "BEFORE"
        };
        sql.execute_batch(&format!("CREATE TRIGGER fixture_fault {when} UPDATE ON ordered_sell_quote_results WHEN NEW.record IS NOT NULL BEGIN {body} END;"))?;
        assert!(
            f.db.store
                .complete_strict_sell_quote(&c, limits(), observation(&c, now), || now)
                .is_err(),
            "{variant}"
        );
        assert!(f
            .db
            .store
            .load_strict_sell_quote(ID, limits(), now)?
            .is_none());
        sql.execute_batch("DROP TRIGGER fixture_fault")?;
        assert_eq!(
            f.db.store
                .complete_strict_sell_quote(&c, limits(), observation(&c, now), || now)?
                .outcome,
            QuoteOutcome::Current
        );
    }
    Ok(())
}
#[test]
fn strict_quote_busy_schema_and_retention_budget_refuse() -> Result<()> {
    let f = fixture()?;
    f.db.store
        .set_busy_timeout(std::time::Duration::from_millis(1))?;
    let sql = f.db.conn()?;
    sql.execute_batch("BEGIN IMMEDIATE")?;
    assert!(f
        .db
        .store
        .claim_strict_sell_quote(limits(), ENDPOINT, || Utc::now())
        .is_err());
    sql.execute_batch("ROLLBACK")?;
    let mut low = limits();
    low.bytes = 1;
    // Fresh evaluation may refuse due to finance budget; that refusal still cannot
    // exceed the independent retained quote budget. R1 records only the cursor visit.
    assert!(matches!(
        f.db.store
            .claim_strict_sell_quote(low, ENDPOINT, Utc::now)?,
        QuoteClaimStep::CapacityRefused(_)
    ));
    assert_eq!(count(&f, "ordered_sell_quote_results")?, 0);
    sql.execute(
        "DELETE FROM schema_migrations WHERE version='0074_ordered_sell_quote_only.sql'",
        [],
    )?;
    assert!(f
        .db
        .store
        .claim_strict_sell_quote(limits(), ENDPOINT, || Utc::now())
        .is_err());
    Ok(())
}
