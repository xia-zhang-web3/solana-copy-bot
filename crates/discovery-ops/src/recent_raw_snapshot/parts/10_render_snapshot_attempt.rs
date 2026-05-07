use super::*;

pub(crate) fn append_render_snapshot_attempt(lines: &mut Vec<String>, output: &SnapshotOutput) {
    lines.extend([
        format!("source_db_bytes={}", output.source_db_bytes),
        format!("source_wal_bytes={}", output.source_wal_bytes),
        format!("source_total_bytes={}", output.source_total_bytes),
        format!("source_page_size_bytes={}", output.source_page_size_bytes),
        format!("source_page_count={}", output.source_page_count),
        format!("snapshot_pages_per_step={}", output.snapshot_pages_per_step),
        format!(
            "snapshot_pause_between_steps_ms={}",
            output.snapshot_pause_between_steps_ms
        ),
        format!(
            "snapshot_max_attempt_duration_ms={}",
            output
                .snapshot_max_attempt_duration_ms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!("attempt_duration_ms={}", output.attempt_duration_ms),
        format!("backup_step_count={}", output.backup_step_count),
        format!("backup_retry_count={}", output.backup_retry_count),
        format!("busy_retry_count={}", output.busy_retry_count),
        format!("locked_retry_count={}", output.locked_retry_count),
        format!(
            "backup_total_page_count={}",
            output
                .backup_total_page_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "backup_remaining_page_count={}",
            output
                .backup_remaining_page_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "backup_copied_page_count={}",
            output
                .backup_copied_page_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "terminal_reason={}",
            output.terminal_reason.as_deref().unwrap_or("null")
        ),
        format!(
            "retryable_reason={}",
            output.retryable_reason.as_deref().unwrap_or("null")
        ),
        format!(
            "deferred_reason={}",
            output.deferred_reason.as_deref().unwrap_or("null")
        ),
        format!(
            "hard_failure_reason={}",
            output.hard_failure_reason.as_deref().unwrap_or("null")
        ),
    ]);
}
