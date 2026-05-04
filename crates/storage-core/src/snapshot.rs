use crate::{
    SqliteDiscoveryStore, SqliteSnapshotDeferredReason, SqliteSnapshotOutcome,
    SqliteSnapshotPolicy, SqliteSnapshotRetryReason, SqliteSnapshotSourceMetrics,
    SqliteSnapshotSummary,
};
use anyhow::{anyhow, Context, Result};
use rusqlite::backup::{Backup, StepResult};
use rusqlite::Connection;
use std::fs;
use std::path::Path;
use std::time::{Duration as StdDuration, Instant};

impl SqliteDiscoveryStore {
    pub fn set_busy_timeout(&self, timeout: StdDuration) -> Result<()> {
        self.conn
            .busy_timeout(timeout)
            .context("failed to set sqlite busy_timeout")?;
        Ok(())
    }

    #[doc(hidden)]
    pub fn set_sqlite_length_limit_for_test(&self, new_val: i32) -> i32 {
        unsafe {
            rusqlite::ffi::sqlite3_limit(
                self.conn.handle(),
                rusqlite::ffi::SQLITE_LIMIT_LENGTH,
                new_val,
            )
        }
    }

    pub fn snapshot_source_metrics(&self) -> Result<SqliteSnapshotSourceMetrics> {
        let page_size: i64 = self
            .conn
            .query_row("PRAGMA page_size", [], |row| row.get(0))
            .context("failed to read sqlite snapshot source page_size")?;
        let page_count: i64 = self
            .conn
            .query_row("PRAGMA page_count", [], |row| row.get(0))
            .context("failed to read sqlite snapshot source page_count")?;
        Ok(SqliteSnapshotSourceMetrics {
            page_size_bytes: page_size.max(0) as usize,
            page_count: page_count.max(0) as usize,
        })
    }

    pub fn snapshot_into_path_with_policy(
        &self,
        destination_path: &Path,
        policy: &SqliteSnapshotPolicy,
    ) -> Result<SqliteSnapshotOutcome> {
        if let Some(parent) = destination_path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create sqlite snapshot parent dir: {}",
                    parent.display()
                )
            })?;
        }
        let started = Instant::now();
        let mut summary = SqliteSnapshotSummary::default();
        let mut destination = Connection::open(destination_path).with_context(|| {
            format!(
                "failed to open destination sqlite snapshot db: {}",
                destination_path.display()
            )
        })?;
        destination
            .busy_timeout(policy.busy_timeout)
            .context("failed to set sqlite snapshot busy_timeout")?;
        prepare_snapshot_destination(&destination)?;
        let _source_read_tx = if policy.pin_source_snapshot {
            Some(SqliteSnapshotReadTransactionGuard::begin(&self.conn)?)
        } else {
            None
        };
        let backup = Backup::new(&self.conn, &mut destination)
            .context("failed to initialize sqlite online backup")?;

        loop {
            summary.backup_step_count = summary.backup_step_count.saturating_add(1);
            let step_result = backup
                .step(policy.pages_per_step)
                .context("failed to complete sqlite online backup step")?;
            set_snapshot_progress(&mut summary, backup.progress());
            match step_result {
                StepResult::Done => {
                    summary.duration_ms =
                        started.elapsed().as_millis().min(u64::MAX as u128) as u64;
                    return Ok(SqliteSnapshotOutcome::Written(summary));
                }
                StepResult::More => {
                    let elapsed = started.elapsed();
                    summary.duration_ms = elapsed.as_millis().min(u64::MAX as u128) as u64;
                    if policy
                        .max_attempt_duration
                        .is_some_and(|budget| elapsed >= budget)
                    {
                        summary.deferred_reason =
                            Some(SqliteSnapshotDeferredReason::AttemptDurationBudgetExceeded);
                        return Ok(SqliteSnapshotOutcome::Deferred(summary));
                    }
                    if !policy.pause_between_steps.is_zero() {
                        std::thread::sleep(policy.pause_between_steps);
                    }
                }
                StepResult::Busy | StepResult::Locked => {
                    let reason = match step_result {
                        StepResult::Busy => SqliteSnapshotRetryReason::Busy,
                        StepResult::Locked => SqliteSnapshotRetryReason::Locked,
                        _ => unreachable!(),
                    };
                    record_snapshot_retry(&mut summary, reason);
                    if let Some(backoff_ms) =
                        policy.retry_backoff_ms.get(summary.backup_retry_count)
                    {
                        summary.backup_retry_count = summary.backup_retry_count.saturating_add(1);
                        std::thread::sleep(StdDuration::from_millis(*backoff_ms));
                        continue;
                    }
                    summary.duration_ms =
                        started.elapsed().as_millis().min(u64::MAX as u128) as u64;
                    summary.retry_exhausted_reason = Some(retry_reason_from_summary(&summary));
                    return Ok(SqliteSnapshotOutcome::RetryableBusy(summary));
                }
                other => {
                    return Err(anyhow!(
                        "unsupported sqlite online backup step result: {:?}",
                        other
                    ));
                }
            }
        }
    }
}

fn retry_reason_from_summary(summary: &SqliteSnapshotSummary) -> SqliteSnapshotRetryReason {
    match (summary.busy_retry_count > 0, summary.locked_retry_count > 0) {
        (true, true) => SqliteSnapshotRetryReason::BusyAndLocked,
        (true, false) => SqliteSnapshotRetryReason::Busy,
        (false, true) => SqliteSnapshotRetryReason::Locked,
        (false, false) => SqliteSnapshotRetryReason::Busy,
    }
}

fn record_snapshot_retry(summary: &mut SqliteSnapshotSummary, reason: SqliteSnapshotRetryReason) {
    match reason {
        SqliteSnapshotRetryReason::Busy => {
            summary.busy_retry_count = summary.busy_retry_count.saturating_add(1);
        }
        SqliteSnapshotRetryReason::Locked => {
            summary.locked_retry_count = summary.locked_retry_count.saturating_add(1);
        }
        SqliteSnapshotRetryReason::BusyAndLocked => {
            summary.busy_retry_count = summary.busy_retry_count.saturating_add(1);
            summary.locked_retry_count = summary.locked_retry_count.saturating_add(1);
        }
    }
}

fn set_snapshot_progress(
    summary: &mut SqliteSnapshotSummary,
    progress: rusqlite::backup::Progress,
) {
    let total_page_count = progress.pagecount.max(0) as usize;
    let remaining_page_count = progress.remaining.max(0) as usize;
    summary.total_page_count = total_page_count;
    summary.remaining_page_count = remaining_page_count.min(total_page_count);
    summary.copied_page_count = total_page_count.saturating_sub(summary.remaining_page_count);
}

struct SqliteSnapshotReadTransactionGuard<'a> {
    conn: &'a Connection,
    active: bool,
}

impl<'a> SqliteSnapshotReadTransactionGuard<'a> {
    fn begin(conn: &'a Connection) -> Result<Self> {
        conn.execute_batch("BEGIN DEFERRED TRANSACTION")
            .context("failed to begin sqlite snapshot read transaction")?;
        conn.query_row("SELECT COUNT(*) FROM sqlite_schema", [], |_row| Ok(()))
            .context("failed to materialize sqlite snapshot read transaction")?;
        Ok(Self { conn, active: true })
    }
}

impl Drop for SqliteSnapshotReadTransactionGuard<'_> {
    fn drop(&mut self) {
        if self.active {
            let _ = self.conn.execute_batch("ROLLBACK");
        }
    }
}

fn prepare_snapshot_destination(destination: &Connection) -> Result<()> {
    destination
        .pragma_update(None, "journal_mode", "OFF")
        .context("failed to set sqlite snapshot destination journal_mode=OFF")?;
    destination
        .pragma_update(None, "synchronous", "OFF")
        .context("failed to set sqlite snapshot destination synchronous=OFF")?;
    destination
        .pragma_update(None, "temp_store", "MEMORY")
        .context("failed to set sqlite snapshot destination temp_store=MEMORY")?;
    Ok(())
}
