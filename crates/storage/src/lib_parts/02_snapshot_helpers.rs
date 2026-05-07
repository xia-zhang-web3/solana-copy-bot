use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqliteSnapshotSourceMetrics {
    pub page_size_bytes: usize,
    pub page_count: usize,
}

pub(super) fn retry_reason_from_summary(
    summary: &SqliteSnapshotSummary,
) -> SqliteSnapshotRetryReason {
    match (summary.busy_retry_count > 0, summary.locked_retry_count > 0) {
        (true, true) => SqliteSnapshotRetryReason::BusyAndLocked,
        (true, false) => SqliteSnapshotRetryReason::Busy,
        (false, true) => SqliteSnapshotRetryReason::Locked,
        (false, false) => SqliteSnapshotRetryReason::Busy,
    }
}

pub(super) fn retry_reason_from_sqlite_error(
    error: &anyhow::Error,
) -> Option<SqliteSnapshotRetryReason> {
    let mut saw_busy = false;
    let mut saw_locked = false;
    for cause in error.chain() {
        let lowered = cause.to_string().to_ascii_lowercase();
        if lowered.contains("database is busy") {
            saw_busy = true;
        }
        if lowered.contains("database is locked") || lowered.contains("database table is locked") {
            saw_locked = true;
        }
    }
    match (saw_busy, saw_locked) {
        (true, true) => Some(SqliteSnapshotRetryReason::BusyAndLocked),
        (true, false) => Some(SqliteSnapshotRetryReason::Busy),
        (false, true) => Some(SqliteSnapshotRetryReason::Locked),
        (false, false) => None,
    }
}

pub(super) fn record_snapshot_retry(
    summary: &mut SqliteSnapshotSummary,
    reason: SqliteSnapshotRetryReason,
) {
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

pub(super) fn set_snapshot_progress(
    summary: &mut SqliteSnapshotSummary,
    progress: rusqlite::backup::Progress,
) {
    let total_page_count = progress.pagecount.max(0) as usize;
    let remaining_page_count = progress.remaining.max(0) as usize;
    summary.total_page_count = total_page_count;
    summary.remaining_page_count = remaining_page_count.min(total_page_count);
    summary.copied_page_count = total_page_count.saturating_sub(summary.remaining_page_count);
}

pub(super) struct SqliteSnapshotReadTransactionGuard<'a> {
    conn: &'a Connection,
    active: bool,
}

impl<'a> SqliteSnapshotReadTransactionGuard<'a> {
    pub(super) fn begin(conn: &'a Connection) -> Result<Self> {
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

pub(super) fn prepare_snapshot_destination(destination: &Connection) -> Result<()> {
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

pub(super) fn build_sqlite_immutable_read_only_uri(path: &Path) -> String {
    let path = path.to_string_lossy();
    if path.starts_with('/') {
        format!("file://{path}?mode=ro&immutable=1")
    } else {
        format!("file:{path}?mode=ro&immutable=1")
    }
}

fn sqlite_wal_path(path: &Path) -> PathBuf {
    let mut wal_path = path.as_os_str().to_os_string();
    wal_path.push("-wal");
    PathBuf::from(wal_path)
}

pub(super) fn sqlite_wal_size_bytes(path: &Path) -> Result<Option<u64>> {
    let wal_path = sqlite_wal_path(path);
    match fs::metadata(&wal_path) {
        Ok(metadata) => Ok(Some(metadata.len())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error)
            .with_context(|| format!("failed to inspect sqlite wal file {}", wal_path.display())),
    }
}
