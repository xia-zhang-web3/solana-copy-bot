use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_discovery::DiscoveryService;
use copybot_execution::{ExecutionBatchReport, ExecutionRuntime};
use copybot_shadow::{ShadowService, ShadowSnapshot};
use copybot_storage::SqliteStore;
use std::path::Path;
use std::sync::Arc;

pub(crate) fn spawn_discovery_task(
    sqlite_path: String,
    discovery: DiscoveryService,
    now: DateTime<Utc>,
) -> impl FnOnce() -> Result<super::DiscoveryTaskOutput> {
    move || {
        let store = SqliteStore::open(Path::new(&sqlite_path)).with_context(|| {
            format!("failed to open sqlite db for discovery task: {sqlite_path}")
        })?;
        let summary = discovery.run_cycle(&store, now)?;
        let active_wallets = store.list_active_follow_wallets()?;
        Ok(super::DiscoveryTaskOutput {
            active_wallets,
            cycle_ts: now,
            eligible_wallets: summary.eligible_wallets,
            active_follow_wallets: summary.active_follow_wallets,
        })
    }
}

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

pub(crate) fn spawn_execution_task(
    sqlite_path: String,
    execution_runtime: Arc<ExecutionRuntime>,
    now: DateTime<Utc>,
    buy_submit_pause_reason: Option<String>,
) -> impl FnOnce() -> Result<ExecutionBatchReport> {
    move || {
        let store = SqliteStore::open(Path::new(&sqlite_path)).with_context(|| {
            format!("failed to open sqlite db for execution task: {sqlite_path}")
        })?;
        execution_runtime.process_batch(&store, now, buy_submit_pause_reason.as_deref())
    }
}
