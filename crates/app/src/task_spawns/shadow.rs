use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_shadow::{ShadowService, ShadowSnapshot};
use copybot_storage_core::SqliteStore;
use std::path::Path;

pub(crate) fn spawn_shadow_snapshot_task(
    sqlite_path: String,
    shadow: ShadowService,
    now: DateTime<Utc>,
) -> impl FnOnce() -> Result<ShadowSnapshot> {
    move || {
        let store = SqliteStore::open(Path::new(&sqlite_path)).with_context(|| {
            format!("failed to open sqlite db for shadow snapshot task: {sqlite_path}")
        })?;
        shadow.snapshot_24h(&store, now)
    }
}
