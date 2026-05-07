use super::*;

pub(crate) fn checkpoint_large_startup_wal_if_needed(
    path: &Path,
    conn: Connection,
    policy: StartupStepRuntimePolicy,
    reporter: Option<&StartupStepProgressReporter>,
    threshold_bytes: u64,
) -> Result<Connection> {
    let before_wal_bytes = sqlite_wal_size_bytes(path)?;
    let should_checkpoint = before_wal_bytes
        .map(|bytes| bytes >= threshold_bytes)
        .unwrap_or(false);
    if !should_checkpoint {
        let started_at = Instant::now();
        report_startup_step_progress(
            reporter,
            SQLITE_STARTUP_LARGE_WAL_CHECKPOINT_TRUNCATE_STAGE,
            StartupStepOutcome::Started,
            StdDuration::ZERO,
            policy.timeout,
            None,
        );
        let reason = if before_wal_bytes.is_some() {
            "wal_below_threshold"
        } else {
            "wal_missing"
        };
        report_startup_step_progress(
            reporter,
            SQLITE_STARTUP_LARGE_WAL_CHECKPOINT_TRUNCATE_STAGE,
            StartupStepOutcome::Skipped,
            started_at.elapsed(),
            policy.timeout,
            Some(sqlite_startup_large_wal_checkpoint_skip_detail(
                reason,
                threshold_bytes,
                before_wal_bytes,
            )),
        );
        return Ok(conn);
    }

    let sqlite_path = path.to_path_buf();
    let (conn, _) = run_observed_startup_step_with_completion_detail(
        SQLITE_STARTUP_LARGE_WAL_CHECKPOINT_TRUNCATE_STAGE,
        policy,
        reporter,
        move || {
            let operation_started = Instant::now();
            let before_wal_bytes = sqlite_wal_size_bytes(&sqlite_path)?.unwrap_or(0);
            let (busy, log_frames, checkpointed_frames): (i64, i64, i64) = {
                let mut stmt = conn
                    .prepare("PRAGMA wal_checkpoint(TRUNCATE)")
                    .context("failed preparing sqlite startup large WAL checkpoint truncate")?;
                stmt.query_row([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
                    .context("failed running sqlite startup large WAL checkpoint truncate")?
            };
            let after_wal_bytes = sqlite_wal_size_bytes(&sqlite_path)?.unwrap_or(0);
            let summary = SqliteStartupLargeWalCheckpointSummary {
                threshold_bytes,
                before_wal_bytes,
                after_wal_bytes,
                busy,
                log_frames,
                checkpointed_frames,
            };
            let elapsed_ms = startup_step_elapsed_ms(operation_started.elapsed());
            tracing::info!(
                startup_stage = SQLITE_STARTUP_LARGE_WAL_CHECKPOINT_TRUNCATE_STAGE,
                before_wal_bytes = summary.before_wal_bytes,
                after_wal_bytes = summary.after_wal_bytes,
                threshold_bytes = summary.threshold_bytes,
                wal_checkpoint_busy = summary.busy,
                wal_log_frames = summary.log_frames,
                wal_checkpointed_frames = summary.checkpointed_frames,
                startup_stage_elapsed_ms = elapsed_ms,
                "sqlite startup large WAL checkpoint truncate completed"
            );
            if busy != 0 {
                anyhow::bail!(
                    "sqlite startup large WAL checkpoint truncate remained busy: {}",
                    sqlite_startup_large_wal_checkpoint_detail(summary)
                );
            }
            Ok((conn, summary))
        },
        |(_, summary)| Some(sqlite_startup_large_wal_checkpoint_detail(*summary)),
    )?;
    Ok(conn)
}

pub fn note_sqlite_write_retry() {
    SQLITE_WRITE_RETRY_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn note_sqlite_busy_error() {
    SQLITE_BUSY_ERROR_TOTAL.fetch_add(1, Ordering::Relaxed);
}
