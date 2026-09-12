#[path = "common/receipt_facts_fixture.rs"]
mod fixture;
use anyhow::Result;
use copybot_storage_core::*;
use fixture::*;
use tempfile::tempdir;
#[test]
fn native_observations_0057_upgrade_keeps_old_completed_uncovered_and_fk() -> Result<()> {
    let dir = tempdir()?;
    let old = dir.path().join("old");
    std::fs::create_dir(&old)?;
    for file in std::fs::read_dir(migrations())? {
        let file = file?;
        if file.file_name().to_string_lossy().as_ref() < "0057" {
            std::fs::copy(file.path(), old.join(file.file_name()))?;
        }
    }
    let path = dir.path().join("upgrade.db");
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(&old)?;
    let (id, now) = seed(&store, Some(42))?;
    let mut db = Db {
        _dir: dir,
        path,
        store,
        id,
        now,
    };
    db.store
        .record_execution_canary_receipt_facts(&db.facts(), now)?;
    db.account()?;
    let before = snapshot(&db.conn()?)?;
    let r = db.store.receipt_native_observations_report(
        now - chrono::Duration::seconds(1),
        now + chrono::Duration::seconds(1),
        1,
    )?;
    assert_eq!(r.coverage, "schema_unavailable");
    assert_eq!(r.uncovered_orders, "1");
    assert_eq!(db.store.run_migrations(migrations())?, 5);
    db.reopen()?;
    assert_eq!(db.store.run_migrations(migrations())?, 0);
    assert_eq!(snapshot(&db.conn()?)?, before);
    assert!(db.store.load_receipt_native_observations(&db.id)?.is_none());
    let r = db.store.receipt_native_observations_report(
        now - chrono::Duration::seconds(1),
        now + chrono::Duration::seconds(1),
        0,
    )?;
    assert_eq!(r.uncovered_orders, "1");
    assert_eq!(r.account_rows, "0");
    assert_eq!(r.coverage, "partial_unresolved");
    let b = ReceiptObservationBundle {
        facts: db.facts(),
        native: NativeAccountObservations::empty(&db.facts()),
    };
    assert!(db.store.record_receipt_observation_bundle(&b, now).is_err());
    assert_eq!(snapshot(&db.conn()?)?, before);
    assert!(!db.conn()?.prepare("PRAGMA foreign_key_check")?.exists([])?);
    Ok(())
}
