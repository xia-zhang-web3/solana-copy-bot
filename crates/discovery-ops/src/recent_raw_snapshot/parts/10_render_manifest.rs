use super::*;

pub(super) fn append_render_manifest(lines: &mut Vec<String>, output: &SnapshotOutput) {
    lines.extend([
        format!(
            "created_at={}",
            output
                .created_at
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "row_count={}",
            output
                .row_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "covered_since={}",
            output
                .covered_since
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "covered_through_cursor={}",
            output
                .covered_through_cursor
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
            "last_batch_completed_at={}",
            output
                .last_batch_completed_at
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "snapshot_bytes={}",
            output
                .snapshot_bytes
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
    ]);
}
