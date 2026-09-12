use anyhow::Result;
use copybot_storage_core::SqliteStore;
use rusqlite::Connection;
use std::path::Path;
use tempfile::tempdir;

#[test]
fn failed_expense_0056_upgrade_keeps_legacy_unknown_and_preserves_fk_on_reopen() -> Result<()> {
    let dir = tempdir()?;
    let old = dir.path().join("before");
    std::fs::create_dir(&old)?;
    let migrations = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../migrations"));
    for file in std::fs::read_dir(migrations)? {
        let file = file?;
        if file.file_name().to_string_lossy().as_ref() < "0056" {
            std::fs::copy(file.path(), old.join(file.file_name()))?;
        }
    }
    let through_0056 = dir.path().join("through-0056");
    std::fs::create_dir(&through_0056)?;
    for file in std::fs::read_dir(migrations)? {
        let file = file?;
        if file.file_name().to_string_lossy().as_ref() < "0057" {
            std::fs::copy(file.path(), through_0056.join(file.file_name()))?;
        }
    }
    let migrations = through_0056.as_path();
    let path = dir.path().join("upgrade.db");
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(&old)?;
    let c = Connection::open(&path)?;
    c.execute_batch("INSERT INTO copy_signals(signal_id,wallet_id,token,side,notional_sol,ts,status) VALUES('s','w','m','sell',1,'2026-09-05T00:00:00Z','x');
        INSERT INTO orders(order_id,signal_id,route,submit_ts,status,tx_signature,client_order_id,attempt) VALUES('exec-canary:old','s','r','2026-09-05T00:00:00Z','execution_canary_failed','sig','c',1);")?;
    assert_eq!(store.run_migrations(migrations)?, 1);
    assert_eq!(store.run_migrations(migrations)?, 0);
    assert_eq!(
        c.query_row(
            "SELECT COUNT(*) FROM execution_failed_expense_tasks",
            [],
            |r| r.get::<_, u64>(0)
        )?,
        0
    );
    drop(store);
    let mut store = SqliteStore::open(&path)?;
    assert_eq!(store.run_migrations(migrations)?, 0);
    let report = store.execution_failed_expense_report(
        "2026-09-04T00:00:00Z".parse()?,
        "2026-09-06T00:00:00Z".parse()?,
        1,
    )?;
    assert_eq!(report.legacy_uncovered_orders, 1);
    assert!(report.cohort_wallet_fee_lamports.is_none());
    assert!(!c.prepare("PRAGMA foreign_key_check")?.exists([])?);
    Ok(())
}
