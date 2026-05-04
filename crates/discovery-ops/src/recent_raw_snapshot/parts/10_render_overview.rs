fn append_render_overview(lines: &mut Vec<String>, output: &SnapshotOutput) {
    lines.extend([
        format!("event={}", output.event),
        format!("state={}", output.state),
        format!("latest_surface_status={}", output.latest_surface_status),
        format!("latest_surface_action={}", output.latest_surface_action),
        format!("config_path={}", output.config_path),
        format!("source_db_path={}", output.source_db_path),
        format!("snapshot_path={}", output.snapshot_path),
        format!("metadata_path={}", output.metadata_path),
        format!(
            "archive_path={}",
            output.archive_path.as_deref().unwrap_or("null")
        ),
        format!(
            "staged_snapshot_path={}",
            output.staged_snapshot_path.as_deref().unwrap_or("null")
        ),
        format!(
            "staged_metadata_path={}",
            output.staged_metadata_path.as_deref().unwrap_or("null")
        ),
        format!(
            "cadence_minutes={}",
            output
                .cadence_minutes
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "retention={}",
            output
                .retention
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!("pruned_snapshots={}", output.pruned_snapshot_paths.len()),
        format!(
            "cleanup_removed_paths={}",
            output.cleanup_removed_paths.len()
        ),
        format!("archive_promoted={}", output.archive_promoted),
        format!(
            "archive_set_count_before={}",
            output
                .archive_set_count_before
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "archive_set_count_after={}",
            output
                .archive_set_count_after
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
    ]);
}
