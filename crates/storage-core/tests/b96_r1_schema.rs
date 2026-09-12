#[path = "common/b96_r1.rs"]
mod f;
use anyhow::Result;
use copybot_storage_core::ordered_source_sell::schema;
use f::*;
#[test]
#[ignore = "requires frozen original96 0072 SQL and actual accepted95 0071 database"]
fn r1_original96_experimental_schema_is_explicitly_rejected_without_repair() -> Result<()> {
    let mut f = pre0072()?;
    let c = f.db.conn()?;
    // Materialize the exact experimental DDL on a disposable copy, never a historical DB.
    c.execute_batch(&std::fs::read_to_string(std::env::var(
        "B96_ORIGINAL0072_DDL",
    )?)?)?;
    c.execute(
        "INSERT INTO schema_migrations(version,applied_at) VALUES(?1,'2026-09-10T00:00:00Z')",
        [schema::MIGRATION],
    )?;
    let before: String = c.query_row(
        "SELECT group_concat(sql,char(10)) FROM (SELECT sql FROM sqlite_master ORDER BY name)",
        [],
        |r| r.get(0),
    )?;
    let migrations = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../migrations"));
    for _ in 0..2 {
        assert!(
            format!("{:#}", f.db.store.run_migrations(migrations).unwrap_err())
                .contains("source_sell_signal_claim_insert")
        );
        assert!(stage(&mut f).is_err());
        let swap = observed(&f)?;
        assert!(f.db.store.record_execution_sell_intent(&swap).is_err());
        // Remove only the newly generated observed fixture to keep next public insertion independent.
        f.db.store
            .delete_observed_swaps_before_batch(f.db.now + chrono::Duration::seconds(1), 10)?;
        let after: String = c.query_row(
            "SELECT group_concat(sql,char(10)) FROM (SELECT sql FROM sqlite_master ORDER BY name)",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(after, before);
        no_intent(&f)?;
        reopen(&mut f)?;
    }
    Ok(())
}
