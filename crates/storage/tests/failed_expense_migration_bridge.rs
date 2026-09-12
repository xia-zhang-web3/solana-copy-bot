use anyhow::Result;
use copybot_storage::SqliteStore;
use rusqlite::Connection;
use std::path::Path;
use tempfile::tempdir;

#[test]
fn legacy_runner_applies_only_0056_without_failed_history_backfill() -> Result<()> {
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
    drop(store);
    let mut store = SqliteStore::open(&path)?;
    assert_eq!(store.run_migrations(migrations)?, 0);
    for table in [
        "execution_failed_expense_tasks",
        "execution_failed_expense_ledger",
    ] {
        assert_eq!(
            c.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r
                .get::<_, u64>(0))?,
            0
        );
    }
    assert_eq!(
        c.query_row("SELECT status FROM orders", [], |r| r.get::<_, String>(0))?,
        "execution_canary_failed"
    );
    assert!(!c.prepare("PRAGMA foreign_key_check")?.exists([])?);
    Ok(())
}
