use anyhow::{anyhow, Result};
use copybot_storage_core::{
    ensure_discovery_v2_schema, sqlite_contention_snapshot, SqliteDiscoveryStore,
};
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

#[test]
fn quality_prepare_begin_retries_retryable_sqlite_lock() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let blocker = SqliteDiscoveryStore::open(&db_path)?;
    ensure_discovery_v2_schema(&blocker)?;
    blocker.set_busy_timeout(Duration::from_millis(10))?;
    blocker.begin_discovery_v2_quality_prepare_update()?;

    let before = sqlite_contention_snapshot();
    let contender_path = db_path.clone();
    let contender = thread::spawn(move || -> Result<()> {
        let store = SqliteDiscoveryStore::open(&contender_path)?;
        store.set_busy_timeout(Duration::from_millis(10))?;
        store.begin_discovery_v2_quality_prepare_update()?;
        store.rollback_discovery_v2_quality_prepare_update();
        Ok(())
    });

    thread::sleep(Duration::from_millis(75));
    blocker.rollback_discovery_v2_quality_prepare_update();
    contender
        .join()
        .map_err(|_| anyhow!("contender thread panicked"))??;

    let after = sqlite_contention_snapshot();
    assert!(
        after.busy_error_total > before.busy_error_total,
        "expected retryable sqlite busy errors to be counted: before={before:?} after={after:?}"
    );
    assert!(
        after.write_retry_total > before.write_retry_total,
        "expected sqlite write retries to be counted: before={before:?} after={after:?}"
    );
    Ok(())
}
