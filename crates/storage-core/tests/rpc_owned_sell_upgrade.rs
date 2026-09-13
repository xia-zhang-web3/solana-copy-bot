use anyhow::Result;
use copybot_storage_core::SqliteStore;
use std::path::Path;
#[test]
fn owned_sell_0079_upgrade_preserves_history_and_adds_no_authority_or_money() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("state.db");
    let old = dir.path().join("old");
    std::fs::create_dir(&old)?;
    let all = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../migrations"));
    for p in std::fs::read_dir(all)? {
        let p = p?.path();
        if p.extension().is_some_and(|x| x == "sql")
            && p.file_name().unwrap().to_str().unwrap() < "0079"
        {
            std::fs::copy(&p, old.join(p.file_name().unwrap()))?;
        }
    }
    let mut s = SqliteStore::open(&path)?;
    assert_eq!(s.run_migrations(&old)?, 79);
    let sql = rusqlite::Connection::open(&path)?;
    let rows = |sql: &rusqlite::Connection| -> Result<Vec<(String, String)>> {
        let mut q=sql.prepare("SELECT version,applied_at FROM schema_migrations WHERE version!='0079_rpc_owned_sell_handoff.sql' ORDER BY version")?;
        let result = q
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?;
        Ok(result)
    };
    let before = rows(&sql)?;
    assert!(s.has_owned_sell_handoff("missing").is_err());
    let through = dir.path().join("through79");
    std::fs::create_dir(&through)?;
    for file in std::fs::read_dir(all)? {
        let p = file?.path();
        if p.extension().is_some_and(|v| v == "sql")
            && p.file_name().unwrap().to_str().unwrap() < "0080"
        {
            std::fs::copy(&p, through.join(p.file_name().unwrap()))?;
        }
    }
    assert_eq!(s.run_migrations(&through)?, 1);
    assert_eq!(s.run_migrations(&through)?, 0);
    assert_eq!(rows(&sql)?, before);
    assert!(!s.has_owned_sell_handoff("missing")?);
    for table in [
        "rpc_owned_sell_handoffs",
        "execution_tiny_experiment",
        "execution_tiny_reservations",
        "orders",
        "positions",
        "fills",
    ] {
        assert_eq!(
            sql.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r
                .get::<_, u64>(0))?,
            0
        );
    }
    drop(s);
    let s = SqliteStore::open(&path)?;
    assert!(!s.has_owned_sell_handoff("missing")?);
    sql.execute_batch("DROP TABLE rpc_owned_sell_handoffs;")?;
    assert!(s.has_owned_sell_handoff("missing").is_err());
    Ok(())
}
