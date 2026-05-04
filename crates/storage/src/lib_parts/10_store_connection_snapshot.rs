impl SqliteStore {
    pub fn snapshot_into_path(&self, destination_path: &Path) -> Result<()> {
        match self
            .snapshot_into_path_with_policy(destination_path, &SqliteSnapshotPolicy::default())?
        {
            SqliteSnapshotOutcome::Written(_) => Ok(()),
            SqliteSnapshotOutcome::RetryableBusy(summary) => {
                anyhow::bail!(
                    "sqlite snapshot exhausted retryable backup contention retries (reason={}, retries={})",
                    summary
                        .retry_exhausted_reason
                        .map(|reason| reason.as_str())
                        .unwrap_or("unknown"),
                    summary.backup_retry_count
                );
            }
            SqliteSnapshotOutcome::Deferred(summary) => {
                anyhow::bail!(
                    "sqlite snapshot deferred after bounded attempt budget (reason={}, duration_ms={}, copied_pages={}, total_pages={})",
                    summary
                        .deferred_reason
                        .map(|reason| reason.as_str())
                        .unwrap_or("unknown"),
                    summary.duration_ms,
                    summary.copied_page_count,
                    summary.total_page_count
                );
            }
        }
    }
}
