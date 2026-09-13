use anyhow::Result;
use copybot_storage::SqliteStore;
use rusqlite::Connection;
use std::path::Path;
use tempfile::tempdir;

#[test]
fn legacy_runner_uses_shared_cash_rebuild_and_preserves_incoming_links() -> Result<()> {
    let dir = tempdir()?;
    let old = dir.path().join("old");
    std::fs::create_dir(&old)?;
    let migrations = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../migrations"));
    let mut pending = 0;
    for file in std::fs::read_dir(migrations)? {
        let file = file?;
        if file.path().extension().is_none_or(|e| e != "sql") {
            continue;
        }
        if file.file_name().to_string_lossy().as_ref() < "0055" {
            std::fs::copy(file.path(), old.join(file.file_name()))?;
        } else {
            pending += 1;
        }
    }
    let path = dir.path().join("legacy.db");
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(&old)?;
    let conn = Connection::open(&path)?;
    conn.execute_batch("INSERT INTO copy_signals(signal_id,wallet_id,token,side,notional_sol,ts,status) VALUES('s','w','mint','sell',1,'t','x');
        INSERT INTO orders(order_id,signal_id,route,submit_ts,status,client_order_id,attempt) VALUES('o','s','r','t','x','c',1);
        INSERT INTO fills(id,order_id,token,qty,avg_price) VALUES(71,'o','mint',1,2);
        CREATE INDEX legacy_fill_token ON fills(token);
        CREATE TABLE linked(fill_id INTEGER REFERENCES fills(id) ON DELETE CASCADE);
        INSERT INTO linked VALUES(71);")?;
    assert_eq!(store.run_migrations(migrations)?, pending);
    assert_eq!(store.run_migrations(migrations)?, 0);
    assert_eq!(
        conn.query_row("SELECT fill_id FROM linked", [], |r| r.get::<_, i64>(0))?,
        71
    );
    assert_eq!(
        conn.query_row("SELECT accounting_basis FROM fills", [], |r| r
            .get::<_, String>(0))?,
        "legacy_unclassified"
    );
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM sqlite_master WHERE name='legacy_fill_token'",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        1
    );
    conn.execute(
        "UPDATE fills SET avg_price=NULL,fee=NULL,slippage_bps=NULL",
        [],
    )?;
    assert!(!conn.prepare("PRAGMA foreign_key_check")?.exists([])?);
    drop(store);
    let mut store = SqliteStore::open(&path)?;
    assert_eq!(store.run_migrations(migrations)?, 0);
    Ok(())
}
