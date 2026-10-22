//! Protected0091 upgrade rollback, preservation, FK and zero-reserve scope.
use super::receipt_lot_custody_fixture as f;
use anyhow::Result;
use copybot_storage_core::SqliteStore;
#[tokio::test]
async fn receipt_lot_custody_migration_atomic_upgrade_and_legacy_reserve() -> Result<()> {
    let c = f::Case::new().await?;
    let dir = c.root.path().join("migrations");
    std::fs::create_dir_all(&dir)?;
    let input = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../migrations"));
    for e in std::fs::read_dir(input)? {
        let p = e?.path();
        let name = p.file_name().unwrap().to_string_lossy();
        if name.ends_with(".sql") && name.as_ref() < "0091" {
            std::fs::copy(&p, dir.join(p.file_name().unwrap()))?;
        }
    }
    let path = c.root.path().join("upgrade.db");
    let mut db = SqliteStore::open(&path)?;
    db.run_migrations(&dir)?;
    let mut legacy = c.buy.clone();
    legacy.min_reserve_lamports = 1;
    db.register_owner_technical_buy_intent(&legacy)?;
    let migration = "0091_controlled_receipt_lot_custody.sql";
    let sql = std::fs::read_to_string(input.join(migration))?;
    std::fs::write(
        dir.join(migration),
        format!("{sql}\nSELECT missing_column;"),
    )?;
    assert!(db.run_migrations(&dir).is_err());
    assert_eq!(
        db.load_owner_technical_buy_intent(&legacy.intent_id)?,
        Some(legacy.clone())
    );
    let check = rusqlite::Connection::open(&path)?;
    let table: bool = check.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='controlled_receipt_lot_scope')",
        [],
        |r| r.get(0),
    )?;
    assert!(!table);
    std::fs::write(dir.join(migration), sql)?;
    assert_eq!(db.run_migrations(&dir)?, 1);
    assert_eq!(db.run_migrations(&dir)?, 0);
    assert_eq!(
        db.load_owner_technical_buy_intent(&legacy.intent_id)?,
        Some(legacy)
    );
    assert!(!check.prepare("PRAGMA foreign_key_check")?.exists([])?);
    let mut zero = c.buy.clone();
    zero.intent_id = "generic-zero".into();
    zero.run_id = "generic-zero".into();
    assert!(db.register_owner_technical_buy_intent(&zero).is_err());
    assert!(check
        .execute(
            "UPDATE owner_technical_buy_intents SET min_reserve_lamports=0",
            []
        )
        .is_err());
    Ok(())
}
