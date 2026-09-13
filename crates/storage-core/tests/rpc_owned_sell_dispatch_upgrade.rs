use anyhow::Result;
use copybot_storage_core::SqliteStore;
use std::path::{Path, PathBuf};
fn subset(root: &Path, name: &str, end: &str) -> Result<PathBuf> {
    let dir = root.join(name);
    std::fs::create_dir(&dir)?;
    for file in std::fs::read_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations"))? {
        let p = file?.path();
        if p.extension().is_some_and(|v| v == "sql")
            && p.file_name().unwrap().to_str().unwrap() < end
        {
            std::fs::copy(&p, dir.join(p.file_name().unwrap()))?;
        }
    }
    Ok(dir)
}
fn seed(c: &rusqlite::Connection) -> Result<()> {
    c.execute_batch("PRAGMA foreign_keys=ON;
      INSERT INTO copy_signals(signal_id,wallet_id,side,token,notional_sol,ts,status) VALUES('legacy','w','sell','mint',0.01,'2026-09-13T00:00:00Z','shadow_recorded');
      INSERT INTO orders(order_id,signal_id,route,submit_ts,status,client_order_id,attempt) VALUES('legacy-order','legacy','tiny','2026-09-13T00:00:00Z','execution_canary_reserved','client',1);
      ALTER TABLE orders ADD COLUMN deployed_extra BLOB;
      UPDATE orders SET deployed_extra=x'000102ff';
      CREATE INDEX b136_custom ON orders(deployed_extra);
      CREATE TABLE b136_triggered(n INTEGER);
      CREATE TRIGGER b136_custom_trigger AFTER UPDATE OF deployed_extra ON orders BEGIN INSERT INTO b136_triggered VALUES(1); END;")?;
    Ok(())
}
fn rows(c: &rusqlite::Connection, table: &str) -> Result<Vec<String>> {
    let mut s = c.prepare(&format!("SELECT * FROM {table} ORDER BY 1"))?;
    let n = s.column_count();
    let result = s
        .query_map([], |r| {
            Ok(format!(
                "{:?}",
                (0..n)
                    .map(|i| r.get::<_, rusqlite::types::Value>(i))
                    .collect::<rusqlite::Result<Vec<_>>>()?
            ))
        })?
        .collect::<rusqlite::Result<_>>()?;
    Ok(result)
}
#[test]
fn b136_order_identity_upgrade_preserves_rows_fks_indexes_triggers_and_reopen() -> Result<()> {
    let root = tempfile::tempdir()?;
    let old = subset(root.path(), "old", "0080")?;
    let all = subset(root.path(), "all", "0081")?;
    let path = root.path().join("state.db");
    let mut s = SqliteStore::open(&path)?;
    s.run_migrations(&old)?;
    let c = rusqlite::Connection::open(&path)?;
    seed(&c)?;
    let before = rows(&c, "orders")?;
    let signals = rows(&c, "copy_signals")?;
    let custom_ddl = || -> Result<Vec<String>> {
        let mut stmt=c.prepare("SELECT sql FROM sqlite_master WHERE name IN ('b136_custom','b136_custom_trigger') ORDER BY name")?;
        let rows = stmt
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        Ok(rows)
    };
    let ddl = custom_ddl()?;
    assert_eq!(s.run_migrations(&all)?, 1);
    assert_eq!(s.run_migrations(&all)?, 0);
    assert_eq!(rows(&c, "orders")?, before);
    assert_eq!(rows(&c, "copy_signals")?, signals);
    let after = custom_ddl()?;
    // sqlite rootpage can legitimately change when rebuilding the index; compare SQL.
    assert_eq!(ddl, after);
    assert!(c
        .execute("DELETE FROM copy_signals WHERE signal_id='legacy'", [])
        .is_err());
    assert!(c
        .execute("UPDATE orders SET signal_id='unowned'", [])
        .is_err());
    c.execute("UPDATE orders SET deployed_extra=x'42'", [])?;
    assert_eq!(rows(&c, "b136_triggered")?.len(), 1);
    assert!(!c.prepare("PRAGMA foreign_key_check")?.exists([])?);
    assert_eq!(rows(&c, "execution_order_sources")?.len(), 1);
    assert!(rows(&c, "rpc_owned_sell_dispatches")?.is_empty());
    drop(s);
    let mut s = SqliteStore::open(&path)?;
    assert_eq!(s.run_migrations(&all)?, 0);
    Ok(())
}
#[test]
fn b136_order_identity_failed_upgrade_rolls_back_and_restores_foreign_keys() -> Result<()> {
    let root = tempfile::tempdir()?;
    let old = subset(root.path(), "old", "0080")?;
    let all = subset(root.path(), "all", "0081")?;
    let path = root.path().join("state.db");
    let mut s = SqliteStore::open(&path)?;
    s.run_migrations(&old)?;
    let c = rusqlite::Connection::open(&path)?;
    seed(&c)?;
    let before = rows(&c, "orders")?;
    let ddl: String = c.query_row(
        "SELECT sql FROM sqlite_master WHERE name='orders'",
        [],
        |r| r.get(0),
    )?;
    let migration = all.join("0080_rpc_owned_sell_dispatch.sql");
    let good = std::fs::read_to_string(&migration)?;
    std::fs::write(
        &migration,
        format!("{good}\nSELECT * FROM deliberate_missing_table;"),
    )?;
    assert!(s.run_migrations(&all).is_err());
    assert_eq!(rows(&c, "orders")?, before);
    assert_eq!(
        c.query_row(
            "SELECT sql FROM sqlite_master WHERE name='orders'",
            [],
            |r| r.get::<_, String>(0)
        )?,
        ddl
    );
    assert!(!c
        .prepare("SELECT name FROM sqlite_master WHERE name='execution_order_sources'")?
        .exists([])?);
    assert!(s
        .reserve_execution_canary_order("missing-signal", "tiny", chrono::Utc::now())
        .is_err());
    std::fs::write(&migration, good)?;
    assert_eq!(s.run_migrations(&all)?, 1);
    Ok(())
}
