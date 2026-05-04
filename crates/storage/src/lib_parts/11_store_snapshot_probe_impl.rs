impl SqliteStore {
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
        loop {
            match prepare_snapshot_destination(&destination) {
                Ok(()) => break,
                Err(error) => {
                    if let Some(reason) = retry_reason_from_sqlite_error(&error) {
                        record_snapshot_retry(&mut summary, reason);
                        note_sqlite_busy_error();
                        if let Some(backoff_ms) =
                            policy.retry_backoff_ms.get(summary.backup_retry_count)
                        {
                            summary.backup_retry_count =
                                summary.backup_retry_count.saturating_add(1);
                            note_sqlite_write_retry();
                            thread::sleep(StdDuration::from_millis(*backoff_ms));
                            continue;
                        }
                        summary.duration_ms =
                            started.elapsed().as_millis().min(u64::MAX as u128) as u64;
                        summary.retry_exhausted_reason = Some(retry_reason_from_summary(&summary));
                        return Ok(SqliteSnapshotOutcome::RetryableBusy(summary));
                    }
                    return Err(error);
                }
            }
        }
        let _source_read_tx = if policy.pin_source_snapshot {
            let guard = loop {
                match SqliteSnapshotReadTransactionGuard::begin(&self.conn) {
                    Ok(guard) => break guard,
                    Err(error) => {
                        if let Some(reason) = retry_reason_from_sqlite_error(&error) {
                            record_snapshot_retry(&mut summary, reason);
                            note_sqlite_busy_error();
                            if let Some(backoff_ms) =
                                policy.retry_backoff_ms.get(summary.backup_retry_count)
                            {
                                summary.backup_retry_count =
                                    summary.backup_retry_count.saturating_add(1);
                                note_sqlite_write_retry();
                                thread::sleep(StdDuration::from_millis(*backoff_ms));
                                continue;
                            }
                            summary.duration_ms =
                                started.elapsed().as_millis().min(u64::MAX as u128) as u64;
                            summary.retry_exhausted_reason =
                                Some(retry_reason_from_summary(&summary));
                            return Ok(SqliteSnapshotOutcome::RetryableBusy(summary));
                        }
                        return Err(error);
                    }
                }
            };
            Some(guard)
        } else {
            None
        };
        let backup = loop {
            match Backup::new(&self.conn, &mut destination) {
                Ok(backup) => break backup,
                Err(error) => {
                    let error = anyhow!(error).context("failed to initialize sqlite online backup");
                    if let Some(reason) = retry_reason_from_sqlite_error(&error) {
                        record_snapshot_retry(&mut summary, reason);
                        note_sqlite_busy_error();
                        if let Some(backoff_ms) =
                            policy.retry_backoff_ms.get(summary.backup_retry_count)
                        {
                            summary.backup_retry_count =
                                summary.backup_retry_count.saturating_add(1);
                            note_sqlite_write_retry();
                            thread::sleep(StdDuration::from_millis(*backoff_ms));
                            continue;
                        }
                        summary.duration_ms =
                            started.elapsed().as_millis().min(u64::MAX as u128) as u64;
                        summary.retry_exhausted_reason = Some(retry_reason_from_summary(&summary));
                        return Ok(SqliteSnapshotOutcome::RetryableBusy(summary));
                    }
                    return Err(error);
                }
            }
        };

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
                        thread::sleep(policy.pause_between_steps);
                    }
                }
                StepResult::Busy => {
                    record_snapshot_retry(&mut summary, SqliteSnapshotRetryReason::Busy);
                    note_sqlite_busy_error();
                    if let Some(backoff_ms) =
                        policy.retry_backoff_ms.get(summary.backup_retry_count)
                    {
                        summary.backup_retry_count = summary.backup_retry_count.saturating_add(1);
                        note_sqlite_write_retry();
                        thread::sleep(StdDuration::from_millis(*backoff_ms));
                        continue;
                    }
                    summary.duration_ms =
                        started.elapsed().as_millis().min(u64::MAX as u128) as u64;
                    summary.retry_exhausted_reason = Some(retry_reason_from_summary(&summary));
                    return Ok(SqliteSnapshotOutcome::RetryableBusy(summary));
                }
                StepResult::Locked => {
                    record_snapshot_retry(&mut summary, SqliteSnapshotRetryReason::Locked);
                    note_sqlite_busy_error();
                    if let Some(backoff_ms) =
                        policy.retry_backoff_ms.get(summary.backup_retry_count)
                    {
                        summary.backup_retry_count = summary.backup_retry_count.saturating_add(1);
                        note_sqlite_write_retry();
                        thread::sleep(StdDuration::from_millis(*backoff_ms));
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

    pub fn snapshot_database(source_path: &Path, destination_path: &Path) -> Result<()> {
        let source = Self::open_read_only(source_path)?;
        source.snapshot_into_path(destination_path)
    }

    pub fn sqlite_read_only_probe_facts(&self) -> Result<SqliteReadOnlyProbeFacts> {
        let page_size: i64 = self
            .conn
            .query_row("PRAGMA page_size", [], |row| row.get(0))
            .context("failed reading sqlite read-only probe page_size")?;
        let page_count: i64 = self
            .conn
            .query_row("PRAGMA page_count", [], |row| row.get(0))
            .context("failed reading sqlite read-only probe page_count")?;
        let freelist_count: i64 = self
            .conn
            .query_row("PRAGMA freelist_count", [], |row| row.get(0))
            .context("failed reading sqlite read-only probe freelist_count")?;
        let journal_mode: String = self
            .conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .context("failed reading sqlite read-only probe journal_mode")?;
        let locking_mode: String = self
            .conn
            .query_row("PRAGMA locking_mode", [], |row| row.get(0))
            .context("failed reading sqlite read-only probe locking_mode")?;
        Ok(SqliteReadOnlyProbeFacts {
            page_size: page_size.max(0) as usize,
            page_count: page_count.max(0) as usize,
            freelist_count: freelist_count.max(0) as usize,
            journal_mode,
            locking_mode,
        })
    }

    pub fn sqlite_read_only_driver_compare_facts(
        &self,
    ) -> Result<SqliteReadOnlyDriverCompareFacts> {
        let busy_timeout_ms: i64 = self
            .conn
            .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
            .context("failed reading sqlite driver-compare busy_timeout")?;
        let cache_size: i64 = self
            .conn
            .query_row("PRAGMA cache_size", [], |row| row.get(0))
            .context("failed reading sqlite driver-compare cache_size")?;
        let mmap_size: i64 = self
            .conn
            .query_row("PRAGMA mmap_size", [], |row| row.get(0))
            .context("failed reading sqlite driver-compare mmap_size")?;
        let query_only: i64 = self
            .conn
            .query_row("PRAGMA query_only", [], |row| row.get(0))
            .context("failed reading sqlite driver-compare query_only")?;
        let journal_mode: String = self
            .conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .context("failed reading sqlite driver-compare journal_mode")?;
        let locking_mode: String = self
            .conn
            .query_row("PRAGMA locking_mode", [], |row| row.get(0))
            .context("failed reading sqlite driver-compare locking_mode")?;
        Ok(SqliteReadOnlyDriverCompareFacts {
            busy_timeout_ms: busy_timeout_ms.max(0) as u64,
            cache_size,
            mmap_size,
            query_only: query_only != 0,
            journal_mode,
            locking_mode,
        })
    }

    #[doc(hidden)]
    pub fn sqlite_active_statement_count_for_debug(&self) -> usize {
        let mut count = 0usize;
        let mut stmt =
            unsafe { rusqlite::ffi::sqlite3_next_stmt(self.conn.handle(), std::ptr::null_mut()) };
        while !stmt.is_null() {
            count = count.saturating_add(1);
            stmt = unsafe { rusqlite::ffi::sqlite3_next_stmt(self.conn.handle(), stmt) };
        }
        count
    }

    pub(crate) fn sqlite_table_exists(&self, table_name: &str) -> Result<bool> {
        Ok(self
            .conn
            .query_row(
                "SELECT 1
                 FROM sqlite_master
                 WHERE type = 'table'
                   AND name = ?1
                 LIMIT 1",
                params![table_name],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .with_context(|| format!("failed checking sqlite table presence for {table_name}"))?
            .is_some())
    }
}
