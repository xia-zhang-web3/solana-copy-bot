#[path = "common/buy_attribution_fixture.rs"]
mod f;
use anyhow::Result;
use copybot_core_types::association_delivery::*;
use copybot_storage_core::association_inbox::{AssociationInbox, InboxLimits};
const DDL: &str = include_str!("../../../migrations/0073_association_budget_indexes.sql");
const VERSION: &str = "0073_association_budget_indexes.sql";
fn limits() -> InboxLimits {
    InboxLimits {
        count: 10000,
        bytes: 128 << 20,
        busy_ms: 10,
    }
}
fn remove(c: &rusqlite::Connection) -> Result<()> {
    for o in DDL.split("-- object ").skip(1) {
        let name = o.split_once('\n').unwrap().0;
        c.execute_batch(&format!("DROP INDEX {name}"))?;
    }
    c.execute("DELETE FROM schema_migrations WHERE version=?1", [VERSION])?;
    Ok(())
}
#[test]
fn b99_populated_upgrade_and_wholly_absent_legacy_api_keep_exact_budget() -> Result<()> {
    let mut db = f::Db::new()?;
    let c = db.conn()?;
    remove(&c)?;
    let mut i = AssociationInbox::open(&db.path, limits())?;
    i.persist(
        &Delivery {
            session: "upgrade".into(),
            sequence: 1,
            arrival_offset_ns: 1,
            event: DeliveryEvent::Session(SessionGap::Reset),
        },
        &CandidateGeneration::Unknown,
    )?;
    let before = i.usage()?;
    db.store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    assert_eq!(i.usage()?, before);
    assert_eq!(AssociationInbox::open(&db.path, limits())?.usage()?, before);
    Ok(())
}
#[test]
fn b99_missing_partial_modified_or_unrecorded_indexes_refuse_open_and_existing_usage() -> Result<()>
{
    for attack in [
        "DROP INDEX b99_association_inbox_events",
        "DROP INDEX b99_association_inbox_events; CREATE INDEX b99_association_inbox_events ON association_inbox_events(sequence)",
        "DELETE FROM schema_migrations WHERE version='0073_association_budget_indexes.sql'",
    ] {
        let db=f::Db::new()?;let c=db.conn()?;
        let mut i=AssociationInbox::open(&db.path,limits())?;
        c.execute_batch(attack)?;
        assert!(i.usage().is_err());
        assert!(AssociationInbox::open(&db.path,limits()).is_err());
        assert!(i.persist(&Delivery {session:"refused".into(),sequence:0,arrival_offset_ns:0,event:DeliveryEvent::Session(SessionGap::Reset)}, &CandidateGeneration::Unknown).is_err());
        assert_eq!(c.query_row("SELECT count(*) FROM association_inbox_events",[],|r|r.get::<_,i64>(0))?,0);
    }
    Ok(())
}
#[test]
fn b99_index_corruption_detected_before_startup_recovery_ack() -> Result<()> {
    let db = f::Db::new()?;
    let c = db.conn()?;
    c.execute(
        "INSERT INTO association_inbox_events VALUES('persisted',1,'payload')",
        [],
    )?;
    // Keep exact DDL text but point its index at an empty index root page.
    c.execute_batch("PRAGMA writable_schema=ON; UPDATE sqlite_master SET rootpage=(SELECT rootpage FROM sqlite_master WHERE name='b99_association_sell_preparations') WHERE name='b99_association_inbox_events'; PRAGMA writable_schema=OFF; PRAGMA schema_version=9999;")?;
    drop(c);
    assert!(AssociationInbox::open(&db.path, limits()).is_err());
    Ok(())
}
#[test]
fn b99_rolled_back_upgrade_leaves_legacy_exact_path_available() -> Result<()> {
    let db = f::Db::new()?;
    let c = db.conn()?;
    remove(&c)?;
    let i = AssociationInbox::open(&db.path, limits())?;
    let before = i.usage()?;
    c.execute_batch("BEGIN IMMEDIATE")?;
    c.execute_batch(DDL)?;
    c.execute(
        "INSERT INTO schema_migrations VALUES(?1,'fixture')",
        [VERSION],
    )?;
    c.execute_batch("ROLLBACK")?;
    assert_eq!(i.usage()?, before);
    assert_eq!(AssociationInbox::open(&db.path, limits())?.usage()?, before);
    Ok(())
}
