use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct CheckpointResult {
    pub(super) busy: i64,
    pub(super) log_frames: i64,
    pub(super) checkpointed_frames: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum CheckpointFailure {
    Timeout(String),
    Unproven(String),
}

pub(super) fn run_sqlite_wal_checkpoint_truncate(
    db_path: &Path,
    timeout_seconds: u64,
) -> Result<CheckpointResult, CheckpointFailure> {
    let deadline = Instant::now() + Duration::from_secs(timeout_seconds);
    let timed_out = Arc::new(AtomicBool::new(false));
    let timeout_flag = timed_out.clone();
    let conn = Connection::open(db_path).map_err(|error| {
        CheckpointFailure::Unproven(format!(
            "failed to open runtime SQLite DB for WAL maintenance {}: {error:#}",
            db_path.display()
        ))
    })?;
    let busy_timeout = Duration::from_secs(timeout_seconds).min(Duration::from_secs(5));
    conn.busy_timeout(busy_timeout).map_err(|error| {
        CheckpointFailure::Unproven(format!("failed to set SQLite busy_timeout: {error:#}"))
    })?;
    conn.progress_handler(
        1_000,
        Some(move || {
            let expired = Instant::now() >= deadline;
            if expired {
                timeout_flag.store(true, Ordering::SeqCst);
            }
            expired
        }),
    );
    let query_result = run_checkpoint_pragma(&conn);
    conn.progress_handler(0, None::<fn() -> bool>);

    match query_result {
        Ok(result) => Ok(result),
        Err(error) if timed_out.load(Ordering::SeqCst) => Err(CheckpointFailure::Timeout(format!(
            "SQLite-managed WAL checkpoint/truncate exceeded timeout_seconds={timeout_seconds}: {error:#}"
        ))),
        Err(error) => Err(CheckpointFailure::Unproven(format!(
            "failed running SQLite-managed WAL checkpoint/truncate: {error:#}"
        ))),
    }
}

fn run_checkpoint_pragma(conn: &Connection) -> Result<CheckpointResult> {
    let mut stmt = conn
        .prepare("PRAGMA wal_checkpoint(TRUNCATE)")
        .context("failed preparing SQLite-managed WAL checkpoint/truncate")?;
    stmt.query_row([], |row| {
        Ok(CheckpointResult {
            busy: row.get(0)?,
            log_frames: row.get(1)?,
            checkpointed_frames: row.get(2)?,
        })
    })
    .context("failed executing SQLite-managed WAL checkpoint/truncate")
}
