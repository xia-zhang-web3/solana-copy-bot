use anyhow::Result;
use copybot_storage_core::SqliteStore;
use std::path::Path;
#[test]
fn tiny_budget_upgrade_has_no_activation_and_reopen_never_creates_money() -> Result<()> {
    let d = tempfile::tempdir()?;
    let path = d.path().join("old.db");
    let migrations = d.path().join("migrations");
    std::fs::create_dir(&migrations)?;
    for f in std::fs::read_dir(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))? {
        let f = f?.path();
        if f.extension().is_some_and(|e| e == "sql")
            && !f
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("0077_")
            && !f
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("0078_")
        {
            std::fs::copy(&f, migrations.join(f.file_name().unwrap()))?;
        }
    }
    let mut s = SqliteStore::open(&path)?;
    s.run_migrations(&migrations)?;
    assert!(s.load_tiny_experiment(chrono::Utc::now()).is_err());
    s.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    assert!(s.load_tiny_experiment(chrono::Utc::now())?.is_none());
    drop(s);
    let s = SqliteStore::open(&path)?;
    assert!(s.load_tiny_experiment(chrono::Utc::now())?.is_none());
    Ok(())
}
