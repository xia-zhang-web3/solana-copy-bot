//! Explicit historical selector rows; elapsed time never authorizes a real retry.
#![allow(dead_code)]
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_storage_core::{SqliteStore, EXECUTION_STATUS_CANARY_SIMULATED};
use rusqlite::{params, Connection};

pub fn import_simulated_history(
    store: &SqliteStore,
    conn: &Connection,
    id: &str,
    at: DateTime<Utc>,
    reason: &str,
) -> Result<()> {
    let before = store.load_execution_canary_order(id)?.unwrap();
    assert_eq!(before.status, EXECUTION_STATUS_CANARY_SIMULATED);
    assert!(before.tx_signature.is_none());
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM execution_canary_unresolved_dispatch WHERE order_id=?1",
            [id],
            |r| r.get::<_, i64>(0)
        )?,
        0
    );
    // Construct legacy selector data BEFORE any submit. A modern Unknown dispatch
    // must keep its hold, never be deleted/relabelled to satisfy a selector test.
    // execution_canary_lifecycle separately proves the current timeout refusal.
    assert_eq!(conn.execute("UPDATE orders SET status=?2,submit_ts=?3,confirm_ts=NULL,err_code=NULL,simulation_error=?4,attempt=attempt+1 WHERE order_id=?1 AND tx_signature IS NULL", params![id,EXECUTION_STATUS_CANARY_SIMULATED,at.to_rfc3339(),reason])?, 1);
    // The current view deliberately recognizes this history as unresolved.
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM execution_canary_unresolved_dispatch WHERE order_id=?1",
            [id],
            |r| r.get::<_, i64>(0)
        )?,
        1
    );
    let imported = store.load_execution_canary_order(id)?.unwrap();
    let refusal = store
        .mark_execution_canary_retry_after_submit_timeout(
            id,
            at + Duration::minutes(10),
            Duration::seconds(60),
            reason,
        )
        .expect_err("historical Unknown must not authorize retry");
    assert!(format!("{refusal:#}").contains("order_not_submitted"));
    assert_eq!(store.load_execution_canary_order(id)?, Some(imported));
    eprintln!("B133_HISTORY id={id} unresolved=1 timeout_retry=refused row_unchanged=true");
    Ok(())
}

pub struct StoreFixture {
    pub store: SqliteStore,
    pub path: std::path::PathBuf,
    _dir: tempfile::TempDir,
}
impl StoreFixture {
    pub fn open(name: &str) -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join(format!("{name}.db"));
        let mut store = SqliteStore::open(&path)?;
        store.run_migrations(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))?;
        Ok(Self {
            store,
            path,
            _dir: dir,
        })
    }
}
impl std::ops::Deref for StoreFixture {
    type Target = SqliteStore;
    fn deref(&self) -> &Self::Target {
        &self.store
    }
}
