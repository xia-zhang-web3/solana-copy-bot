//! Immutable closed schema bytes; each case owns a new file and a normal store.
use anyhow::{ensure, Context, Result};
use copybot_storage_core::SqliteStore;
use std::{fs, io::Write, path::Path, sync::OnceLock};

static EMPTY: OnceLock<std::result::Result<Vec<u8>, String>> = OnceLock::new();

pub(super) fn migrations() -> &'static Path {
    Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../migrations"))
}

fn template() -> Result<Vec<u8>> {
    let root = super::super::temporary_output_fixture::OutputRoot::new("migrated-empty")?;
    let path = root.path().join("template.db");
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(migrations())?;
    drop(store); // Last connection closes/checkpoints its owned WAL before reading main bytes.
    for suffix in ["-wal", "-shm", "-journal"] {
        ensure!(
            !Path::new(&format!("{}{suffix}", path.display())).exists(),
            "unclosed template sidecar"
        );
    }
    let bytes = fs::read(&path)?;
    ensure!(
        bytes.starts_with(b"SQLite format 3\0") && bytes.len() <= 16 * 1024 * 1024,
        "invalid or oversized empty fixture template"
    );
    Ok(bytes) // root removes only this template; static retains immutable bytes, no temp leak.
}

pub(in crate::app_tests) fn open(path: &Path) -> Result<SqliteStore> {
    let bytes = EMPTY
        .get_or_init(|| template().map_err(|error| format!("{error:#}")))
        .as_ref()
        .map_err(|error| anyhow::anyhow!(error.clone()))?;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .context("create independent migrated fixture")?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    let mut store = SqliteStore::open(path)?;
    // Keep the migration API/idempotence check on every independent case.
    ensure!(
        store.run_migrations(migrations())? == 0,
        "template migrations changed"
    );
    Ok(store)
}

#[path = "migrated_empty_fixture_tests.rs"]
mod tests;
