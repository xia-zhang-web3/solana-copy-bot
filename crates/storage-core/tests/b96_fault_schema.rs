#[path = "common/b96_ordered.rs"]
mod f;
use anyhow::Result;
use f::*;
#[test]
fn b96_ignore_abort_and_readback_faults_roll_back_both_rows() -> Result<()> {
    for table in [
        "source_sell_signature_claims",
        "ordered_source_sell_intents",
    ] {
        for action in ["IGNORE", "ABORT,'fixture refusal'"] {
            let mut f = within()?;
            f.db.conn()?.execute_batch(&format!(
                "CREATE TRIGGER fault BEFORE INSERT ON {table} BEGIN SELECT RAISE({action}); END;"
            ))?;
            assert!(stage(&mut f).is_err(), "{table}:{action}");
            no_intent(&f)?;
        }
    }
    let mut f = within()?;
    // A failure after intent insertion must roll back its claim as well.
    f.db.conn()?.execute_batch("CREATE TRIGGER fault AFTER INSERT ON ordered_source_sell_intents BEGIN DELETE FROM association_sell_preparations WHERE signature=NEW.signature; SELECT RAISE(ABORT,'fixture insert fault'); END;")?;
    assert!(stage(&mut f).is_err());
    no_intent(&f)?;
    assert!(f.read().is_ok());
    Ok(())
}
#[test]
fn b96_fresh_migration_repeat_schema_missing_partial_and_altered_fail_closed() -> Result<()> {
    use copybot_storage_core::ordered_source_sell::schema;
    let mut f = within()?;
    let migrations = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../migrations"));
    assert_eq!(f.db.store.run_migrations(migrations)?, 0);
    assert_eq!(count(&f, "ordered_source_sell_intents")?, 0);
    for object in schema::DDL.split("-- object ").skip(1) {
        let (name, sql) = object.split_once('\n').unwrap();
        let mut f = within()?;
        let c = f.db.conn()?;
        let kind = if sql.starts_with("CREATE TABLE") {
            "TABLE"
        } else {
            "TRIGGER"
        };
        c.execute_batch(&format!("DROP {kind} {name}"))?;
        assert!(stage(&mut f).is_err(), "missing {name}");
        assert!(
            f.db.store.run_migrations(migrations).is_err(),
            "registry {name}"
        );
    }
    let mut f = within()?;
    f.db.conn()?.execute(
        "DELETE FROM schema_migrations WHERE version=?1",
        [schema::MIGRATION],
    )?;
    assert!(stage(&mut f).is_err());
    let s = observed(&f)?;
    assert!(
        legacy(&f, &s).is_err(),
        "partial new schema cannot become legacy fallback"
    );
    let mut f = within()?;
    f.db.conn()?.execute_batch("DROP TRIGGER ordered_source_sell_no_delete; CREATE TRIGGER ordered_source_sell_no_delete BEFORE DELETE ON ordered_source_sell_intents BEGIN SELECT 1; END;")?;
    assert!(stage(&mut f).is_err());
    Ok(())
}
#[test]
fn b96_unknown_policy_version_and_corrupt_saved_identity_never_fallback() -> Result<()> {
    let mut f = within()?;
    assert_eq!(
        f.inbox
            .stage_ordered_source_sell_intent("sell", "provider_order_strict_v2")?,
        OrderedSellStage::Unknown(OrderedSellReason::UnsupportedPolicy)
    );
    no_intent(&f)?;
    assert_eq!(
        fresh(&f)?,
        OrderedSellDecision::Unknown(OrderedSellReason::MissingIntent)
    );
    for mutation in [
        "version=2",
        "policy='future'",
        "record=json_set(record,'$.version',2)",
        "record=json_set(record,'$.first.sell.admission.facts.signature','substituted')",
        "record=json_remove(record,'$.first')",
    ] {
        let mut f = within()?;
        inserted(&mut f)?;
        let c = f.db.conn()?;
        let trigger: String = c.query_row(
            "SELECT sql FROM sqlite_master WHERE name='ordered_source_sell_no_update'",
            [],
            |r| r.get(0),
        )?;
        c.execute_batch(
            "DROP TRIGGER ordered_source_sell_no_update; PRAGMA ignore_check_constraints=ON;",
        )?;
        c.execute_batch(&format!(
            "UPDATE ordered_source_sell_intents SET {mutation}"
        ))?;
        c.execute_batch(&trigger)?;
        assert!(fresh(&f).is_err(), "{mutation}");
        assert!(stage(&mut f).is_err(), "{mutation}");
    }
    Ok(())
}
