use super::*;

pub(crate) fn publish_latest_snapshot_from_staged(
    config_path: &Path,
    source_db_path: &Path,
    latest_snapshot_path: &Path,
    latest_metadata_path: &Path,
    now: DateTime<Utc>,
    cadence_minutes: u64,
    retention: usize,
    latest_surface_status: LatestSurfaceStatus,
    archive_path: &Path,
    staged_archive_path: &Path,
    snapshot_context: &SnapshotContext,
    staged_attempt: &StagedSnapshotAttempt,
    archive_maintenance: &SnapshotArchiveMaintenance,
) -> Result<RecentRawJournalSnapshotManifest, SnapshotOutput> {
    if let Err(error) = link_or_copy_atomic(staged_archive_path, latest_snapshot_path)
        .with_context(|| format!("failed updating {}", latest_snapshot_path.display()))
    {
        return Err(render_output(
            SnapshotState::HardFailure,
            latest_surface_status,
            LatestSurfaceAction::UnchangedDueToHardFailure,
            snapshot_context,
            config_path,
            source_db_path,
            latest_snapshot_path,
            latest_metadata_path,
            Some(archive_path),
            Some(cadence_minutes),
            Some(retention),
            Some(&staged_attempt.manifest),
            None,
            None,
            None,
            Some(error.to_string()),
            SnapshotOutputContext {
                archive_promoted: false,
                archive_maintenance: archive_maintenance.clone(),
                staged_progress: staged_attempt.progress.clone(),
                attempt_duration_ms_override: Some(staged_attempt.attempt_duration_ms),
                terminal_reason_override: None,
            },
        ));
    }
    if let Err(error) = invoke_post_snapshot_publish_hook(latest_snapshot_path) {
        return Err(render_output(
            SnapshotState::HardFailure,
            latest_surface_status,
            LatestSurfaceAction::UnchangedDueToHardFailure,
            snapshot_context,
            config_path,
            source_db_path,
            latest_snapshot_path,
            latest_metadata_path,
            Some(archive_path),
            Some(cadence_minutes),
            Some(retention),
            Some(&staged_attempt.manifest),
            None,
            None,
            None,
            Some(format!(
                "failed running post-snapshot publish hook for {}: {error}",
                latest_snapshot_path.display()
            )),
            SnapshotOutputContext {
                archive_promoted: false,
                archive_maintenance: archive_maintenance.clone(),
                staged_progress: staged_attempt.progress.clone(),
                attempt_duration_ms_override: Some(staged_attempt.attempt_duration_ms),
                terminal_reason_override: None,
            },
        ));
    }

    let latest_manifest = manifest_for_snapshot(source_db_path, latest_snapshot_path, now)
        .map_err(|error| {
            render_output(
                SnapshotState::HardFailure,
                latest_surface_status,
                LatestSurfaceAction::UnchangedDueToHardFailure,
                snapshot_context,
                config_path,
                source_db_path,
                latest_snapshot_path,
                latest_metadata_path,
                Some(archive_path),
                Some(cadence_minutes),
                Some(retention),
                Some(&staged_attempt.manifest),
                None,
                None,
                None,
                Some(format!(
                    "failed building latest recent_raw snapshot manifest from {}: {error}",
                    latest_snapshot_path.display()
                )),
                SnapshotOutputContext {
                    archive_promoted: false,
                    archive_maintenance: archive_maintenance.clone(),
                    staged_progress: staged_attempt.progress.clone(),
                    attempt_duration_ms_override: Some(staged_attempt.attempt_duration_ms),
                    terminal_reason_override: None,
                },
            )
        })?;

    if let Err(error) = write_json_atomic(latest_metadata_path, &latest_manifest)
        .with_context(|| format!("failed writing {}", latest_metadata_path.display()))
    {
        return Err(render_output(
            SnapshotState::HardFailure,
            latest_surface_status,
            LatestSurfaceAction::UnchangedDueToHardFailure,
            snapshot_context,
            config_path,
            source_db_path,
            latest_snapshot_path,
            latest_metadata_path,
            Some(archive_path),
            Some(cadence_minutes),
            Some(retention),
            Some(&latest_manifest),
            None,
            None,
            None,
            Some(error.to_string()),
            SnapshotOutputContext {
                archive_promoted: false,
                archive_maintenance: archive_maintenance.clone(),
                staged_progress: staged_attempt.progress.clone(),
                attempt_duration_ms_override: Some(staged_attempt.attempt_duration_ms),
                terminal_reason_override: None,
            },
        ));
    }

    Ok(latest_manifest)
}
