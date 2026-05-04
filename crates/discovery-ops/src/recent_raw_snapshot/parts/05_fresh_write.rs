include!("05_fresh_write_publish_latest.rs");
include!("05_fresh_write_promote_archive.rs");

fn write_fresh_scheduled_snapshot(
    config_path: &Path,
    source_db_path: &Path,
    source_store: &SqliteStore,
    latest_snapshot_path: &Path,
    latest_metadata_path: &Path,
    now: DateTime<Utc>,
    cadence_minutes: u64,
    retention: usize,
    latest_surface_status: LatestSurfaceStatus,
    action: LatestSurfaceAction,
    snapshot_dir: &Path,
    snapshot_context: &SnapshotContext,
    latest_manifest: Option<&RecentRawJournalSnapshotManifest>,
    mut archive_maintenance: SnapshotArchiveMaintenance,
) -> Result<SnapshotOutput> {
    let archive_path = journal_snapshot_archive_path(snapshot_dir, now);
    let archive_metadata_path = journal_snapshot_metadata_path(&archive_path);
    let staged_archive_path = staged_snapshot_archive_path(snapshot_dir);
    let staged_archive_metadata_path = staged_snapshot_metadata_path(snapshot_dir);
    let staged_preserve_paths = vec![
        staged_archive_path.clone(),
        staged_archive_metadata_path.clone(),
    ];
    let staged_attempt = match resume_staged_snapshot_with_policy(
        source_db_path,
        source_store,
        snapshot_dir,
        now,
        &snapshot_context.policy,
    ) {
        StagedSnapshotAttemptResult::Completed(attempt) => attempt,
        StagedSnapshotAttemptResult::Deferred(attempt) => {
            let reason = staged_attempt_budget_reason(&attempt.progress);
            let (latest_action, deferred_reason) =
                scheduled_duration_budget_contract(latest_surface_status, &reason);
            return Ok(render_output(
                SnapshotState::Deferred,
                latest_surface_status,
                latest_action,
                snapshot_context,
                config_path,
                source_db_path,
                latest_snapshot_path,
                latest_metadata_path,
                None,
                Some(cadence_minutes),
                Some(retention),
                latest_manifest,
                None,
                None,
                deferred_reason,
                None,
                SnapshotOutputContext {
                    archive_promoted: false,
                    archive_maintenance,
                    staged_progress: attempt.progress,
                    attempt_duration_ms_override: Some(attempt.attempt_duration_ms),
                    terminal_reason_override: Some(reason),
                },
            ));
        }
        StagedSnapshotAttemptResult::HardFailure {
            manifest,
            progress,
            attempt_duration_ms,
            reason,
        } => {
            return Ok(render_output(
                SnapshotState::HardFailure,
                latest_surface_status,
                LatestSurfaceAction::UnchangedDueToHardFailure,
                snapshot_context,
                config_path,
                source_db_path,
                latest_snapshot_path,
                latest_metadata_path,
                None,
                Some(cadence_minutes),
                Some(retention),
                manifest.as_ref().or(latest_manifest),
                None,
                None,
                None,
                Some(reason),
                SnapshotOutputContext {
                    archive_promoted: false,
                    archive_maintenance,
                    staged_progress: progress,
                    attempt_duration_ms_override: Some(attempt_duration_ms),
                    terminal_reason_override: None,
                },
            ));
        }
    };

    match enforce_snapshot_archive_retention(
        snapshot_dir,
        retention.saturating_sub(1),
        &staged_preserve_paths,
    ) {
        Ok(maintenance) => {
            archive_maintenance.record_pass(
                maintenance.archive_set_count_before.unwrap_or_default(),
                maintenance.archive_set_count_after.unwrap_or_default(),
                maintenance.cleanup_removed_paths,
                maintenance.pruned_snapshot_paths,
            );
        }
        Err(error) => {
            return Ok(render_output(
                SnapshotState::HardFailure,
                latest_surface_status,
                LatestSurfaceAction::UnchangedDueToHardFailure,
                snapshot_context,
                config_path,
                source_db_path,
                latest_snapshot_path,
                latest_metadata_path,
                Some(&archive_path),
                Some(cadence_minutes),
                Some(retention),
                Some(&staged_attempt.manifest),
                None,
                None,
                None,
                Some(error.to_string()),
                SnapshotOutputContext {
                    archive_promoted: false,
                    archive_maintenance,
                    staged_progress: staged_attempt.progress.clone(),
                    attempt_duration_ms_override: Some(staged_attempt.attempt_duration_ms),
                    terminal_reason_override: None,
                },
            ));
        }
    }

    let latest_manifest = match publish_latest_snapshot_from_staged(
        config_path,
        source_db_path,
        latest_snapshot_path,
        latest_metadata_path,
        now,
        cadence_minutes,
        retention,
        latest_surface_status,
        &archive_path,
        &staged_archive_path,
        snapshot_context,
        &staged_attempt,
        &archive_maintenance,
    ) {
        Ok(manifest) => manifest,
        Err(output) => return Ok(output),
    };

    let archive_manifest = match promote_staged_snapshot_to_archive(
        config_path,
        source_db_path,
        latest_snapshot_path,
        latest_metadata_path,
        now,
        cadence_minutes,
        retention,
        latest_surface_status,
        &archive_path,
        &archive_metadata_path,
        &staged_archive_path,
        snapshot_context,
        &staged_attempt,
        &archive_maintenance,
        &latest_manifest,
    ) {
        Ok(manifest) => manifest,
        Err(output) => return Ok(output),
    };

    archive_maintenance
        .cleanup_removed_paths
        .extend(remove_staged_snapshot_artifacts(
            &staged_archive_path,
            &staged_archive_metadata_path,
        )?);
    archive_maintenance.archive_set_count_after =
        Some(list_archive_snapshot_paths(snapshot_dir)?.len());

    Ok(render_output(
        SnapshotState::Written,
        latest_surface_status,
        action,
        snapshot_context,
        config_path,
        source_db_path,
        latest_snapshot_path,
        latest_metadata_path,
        Some(&archive_path),
        Some(cadence_minutes),
        Some(retention),
        Some(&archive_manifest),
        None,
        None,
        None,
        None,
        SnapshotOutputContext {
            archive_promoted: true,
            archive_maintenance,
            staged_progress: staged_attempt.progress,
            attempt_duration_ms_override: Some(staged_attempt.attempt_duration_ms),
            terminal_reason_override: Some("written".to_string()),
        },
    ))
}
