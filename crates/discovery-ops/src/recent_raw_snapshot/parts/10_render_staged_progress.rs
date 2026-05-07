use super::*;

pub(crate) fn append_render_staged_progress(lines: &mut Vec<String>, output: &SnapshotOutput) {
    lines.extend([
        format!("staged_progress_resumed={}", output.staged_progress_resumed),
        format!(
            "staged_seeded_from_latest_surface={}",
            output.staged_seeded_from_latest_surface
        ),
        format!(
            "staged_progress_preserved_for_retry={}",
            output.staged_progress_preserved_for_retry
        ),
        format!(
            "staged_progress_advanced={}",
            output.staged_progress_advanced
        ),
        format!(
            "staged_completed_batches={}",
            output.staged_completed_batches
        ),
        format!(
            "staged_source_rows_loaded={}",
            output.staged_source_rows_loaded
        ),
        format!("staged_rows_processed={}", output.staged_rows_processed),
        format!("staged_rows_inserted={}", output.staged_rows_inserted),
        format!(
            "staged_row_count_before_attempt={}",
            output
                .staged_row_count_before_attempt
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_row_count_after_attempt={}",
            output
                .staged_row_count_after_attempt
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_covered_through_cursor_before_attempt={}",
            output
                .staged_covered_through_cursor_before_attempt
                .as_ref()
                .map(|cursor| format!(
                    "{}/{}/{}",
                    cursor.ts_utc.to_rfc3339(),
                    cursor.slot,
                    cursor.signature
                ))
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_covered_through_cursor_after_attempt={}",
            output
                .staged_covered_through_cursor_after_attempt
                .as_ref()
                .map(|cursor| format!(
                    "{}/{}/{}",
                    cursor.ts_utc.to_rfc3339(),
                    cursor.slot,
                    cursor.signature
                ))
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_terminal_phase={}",
            output.staged_terminal_phase.as_deref().unwrap_or("null")
        ),
        format!(
            "staged_source_read_duration_ms={}",
            output.staged_source_read_duration_ms
        ),
        format!(
            "staged_write_duration_ms={}",
            output.staged_write_duration_ms
        ),
        format!(
            "staged_write_rows_per_second={}",
            output
                .staged_write_rows_per_second
                .map(|value| format!("{value:.2}"))
                .unwrap_or_else(|| "null".to_string())
        ),
    ]);
}
