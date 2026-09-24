use anyhow::Result;
use copybot_storage::SqliteStore;
use std::path::{Path, PathBuf};

fn migrations() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations")
}

#[test]
fn legacy_runner_upgrades_owner_source_without_losing_existing_order() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let old = dir.path().join("old");
    std::fs::create_dir(&old)?;
    for entry in std::fs::read_dir(migrations())? {
        let path = entry?.path();
        if path.extension().is_some_and(|v| v == "sql")
            && path.file_name().unwrap().to_str().unwrap() < "0085_owner_technical_buy_intent.sql"
        {
            std::fs::copy(&path, old.join(path.file_name().unwrap()))?;
        }
    }
    let path = dir.path().join("state.db");
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(&old)?;
    let conn = rusqlite::Connection::open(&path)?;
    conn.execute_batch("PRAGMA foreign_keys=ON;
        INSERT INTO copy_signals(signal_id,wallet_id,side,token,notional_sol,ts,status)
          VALUES('copy','leader','buy','mint',0.01,'2026-09-24T12:00:00Z','shadow_recorded');
        INSERT INTO orders(order_id,signal_id,route,submit_ts,status,client_order_id)
          VALUES('exec-canary:copy','copy','metis','2026-09-24T12:00:00Z','execution_canary_candidate','client');")?;
    assert_eq!(store.run_migrations(&migrations())?, 1);
    assert_eq!(store.run_migrations(&migrations())?, 0);
    let old_order: String = conn.query_row(
        "SELECT signal_id FROM orders WHERE order_id='exec-canary:copy'", [], |r| r.get(0),
    )?;
    assert_eq!(old_order, "copy");
    assert!(!conn.prepare("PRAGMA foreign_key_check")?.exists([])?);
    Ok(())
}
