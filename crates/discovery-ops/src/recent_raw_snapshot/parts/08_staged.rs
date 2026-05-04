include!("08_staged_advance.rs");

fn resume_staged_snapshot_with_policy(
    source_db_path: &Path,
    source_store: &SqliteStore,
    snapshot_dir: &Path,
    now: DateTime<Utc>,
    snapshot_policy: &SqliteSnapshotPolicy,
) -> StagedSnapshotAttemptResult {
    let staged_snapshot_path = staged_snapshot_archive_path(snapshot_dir);
    let staged_metadata_path = staged_snapshot_metadata_path(snapshot_dir);
    let mut progress = StagedSnapshotProgress {
        staged_snapshot_path: Some(staged_snapshot_path.clone()),
        staged_metadata_path: Some(staged_metadata_path.clone()),
        ..StagedSnapshotProgress::default()
    };

    if let Err(error) = fs::create_dir_all(snapshot_dir)
        .with_context(|| format!("failed creating {}", snapshot_dir.display()))
    {
        return StagedSnapshotAttemptResult::HardFailure {
            manifest: None,
            progress,
            attempt_duration_ms: 0,
            reason: error.to_string(),
        };
    }

    let source_state = match load_required_cached_recent_raw_state(
        source_store,
        source_db_path,
        "scheduled source resume state",
    ) {
        Ok(state) => state,
        Err(error) => {
            return StagedSnapshotAttemptResult::HardFailure {
                manifest: None,
                progress,
                attempt_duration_ms: 0,
                reason: format!("failed reading source recent_raw journal state: {error:#}"),
            };
        }
    };

    let latest_snapshot_path = journal_snapshot_latest_path(snapshot_dir);
    let mut existing_manifest = match load_existing_staged_manifest(
        source_db_path,
        &staged_snapshot_path,
        &staged_metadata_path,
    ) {
        Ok(existing_manifest) => existing_manifest,
        Err(error) => {
            return StagedSnapshotAttemptResult::HardFailure {
                manifest: None,
                progress,
                attempt_duration_ms: 0,
                reason: format!(
                    "failed reading staged recent_raw snapshot progress from {}: {error:#}",
                    staged_snapshot_path.display()
                ),
            };
        }
    };

    if existing_manifest
        .as_ref()
        .is_some_and(|manifest| manifest.source_db_path != source_db_path.display().to_string())
    {
        match remove_staged_snapshot_artifacts(&staged_snapshot_path, &staged_metadata_path) {
            Ok(_) => {
                existing_manifest = None;
            }
            Err(error) => {
                return StagedSnapshotAttemptResult::HardFailure {
                    manifest: None,
                    progress,
                    attempt_duration_ms: 0,
                    reason: format!(
                        "failed resetting staged recent_raw snapshot progress in {} after source path changed: {error}",
                        snapshot_dir.display()
                    ),
                };
            }
        }
    }

    let latest_surface = assess_latest_surface(
        &latest_snapshot_path,
        &journal_snapshot_latest_metadata_path(snapshot_dir),
    )
    .ok();
    let latest_resume_manifest = latest_surface.as_ref().and_then(|surface| {
        if surface.status == LatestSurfaceStatus::Healthy {
            surface.manifest.as_ref()
        } else {
            None
        }
    });

    let should_reset_existing_stage = existing_manifest.as_ref().is_some_and(|manifest| {
        source_contract_no_longer_matches_staged_progress(&source_state, manifest)
    });
    let should_seed_from_latest = latest_resume_manifest.is_some_and(|latest_manifest| {
        latest_surface_can_seed_staged_progress(
            source_db_path,
            &source_state,
            latest_manifest,
            existing_manifest.as_ref(),
        )
    });

    if should_reset_existing_stage && !should_seed_from_latest {
        match remove_staged_snapshot_artifacts(&staged_snapshot_path, &staged_metadata_path) {
            Ok(_) => {
                existing_manifest = None;
            }
            Err(error) => {
                return StagedSnapshotAttemptResult::HardFailure {
                    manifest: None,
                    progress,
                    attempt_duration_ms: 0,
                    reason: format!(
                        "failed resetting stale staged recent_raw snapshot progress in {}: {error}",
                        snapshot_dir.display()
                    ),
                };
            }
        }
    }

    if should_seed_from_latest {
        let latest_manifest = latest_resume_manifest.expect("checked is_some above");
        match seed_staged_snapshot_from_latest_surface(
            source_db_path,
            &latest_snapshot_path,
            latest_manifest,
            &staged_snapshot_path,
            &staged_metadata_path,
        ) {
            Ok(staged_manifest) => {
                progress.seeded_from_latest_surface = true;
                existing_manifest = Some(staged_manifest);
            }
            Err(error) => {
                return StagedSnapshotAttemptResult::HardFailure {
                    manifest: existing_manifest,
                    progress,
                    attempt_duration_ms: 0,
                    reason: format!(
                        "failed seeding staged recent_raw snapshot progress in {} from published latest: {error:#}",
                        snapshot_dir.display()
                    ),
                };
            }
        }
    }

    let staged_created_at = existing_manifest
        .as_ref()
        .map(|manifest| manifest.created_at)
        .unwrap_or(now);
    let staged_store = match SqliteStore::open(&staged_snapshot_path) {
        Ok(store) => store,
        Err(error) => {
            return StagedSnapshotAttemptResult::HardFailure {
                manifest: existing_manifest,
                progress,
                attempt_duration_ms: 0,
                reason: format!(
                    "failed opening staged recent_raw snapshot {}: {error}",
                    staged_snapshot_path.display()
                ),
            };
        }
    };
    if let Err(error) = staged_store.ensure_recent_raw_journal_tables() {
        return StagedSnapshotAttemptResult::HardFailure {
            manifest: existing_manifest,
            progress,
            attempt_duration_ms: 0,
            reason: format!(
                "failed ensuring staged recent_raw snapshot tables in {}: {error}",
                staged_snapshot_path.display()
            ),
        };
    }

    let before_state = match staged_store.recent_raw_journal_state_cached() {
        Ok(state) => state,
        Err(error) => {
            return StagedSnapshotAttemptResult::HardFailure {
                manifest: existing_manifest,
                progress,
                attempt_duration_ms: 0,
                reason: format!(
                    "failed reading staged recent_raw snapshot state from {}: {error}",
                    staged_snapshot_path.display()
                ),
            };
        }
    };
    progress.row_count_before_attempt = Some(before_state.row_count);
    progress.covered_through_cursor_before_attempt = before_state.covered_through_cursor.clone();
    progress.resumed_from_existing_stage =
        !progress.seeded_from_latest_surface && before_state.row_count > 0;

    let deadline = snapshot_attempt_deadline(snapshot_policy.max_attempt_duration);
    let started = Instant::now();
    let batch_limit = snapshot_resume_batch_size(snapshot_policy);
    progress.write_batch_rows = batch_limit;
    let (current_state, mut progress, budget_exhausted) = match advance_staged_snapshot(
        source_db_path,
        source_store,
        &staged_store,
        &staged_snapshot_path,
        staged_created_at,
        &source_state,
        &before_state,
        progress,
        now,
        started,
        deadline,
        batch_limit,
    ) {
        Ok(result) => result,
        Err(result) => return result,
    };

    progress.row_count_after_attempt = Some(current_state.row_count);
    progress.covered_through_cursor_after_attempt = current_state.covered_through_cursor.clone();

    let manifest = match staged_manifest_for_state(
        source_db_path,
        &staged_snapshot_path,
        staged_created_at,
        &current_state,
    ) {
        Ok(manifest) => manifest,
        Err(error) => {
            return StagedSnapshotAttemptResult::HardFailure {
                manifest: existing_manifest,
                progress,
                attempt_duration_ms: started.elapsed().as_millis().min(u64::MAX as u128) as u64,
                reason: format!(
                    "failed building staged recent_raw snapshot manifest from {}: {error}",
                    staged_snapshot_path.display()
                ),
            };
        }
    };

    if current_state.row_count > 0 || progress.resumed_from_existing_stage {
        if let Err(error) = write_json_atomic(&staged_metadata_path, &manifest)
            .with_context(|| format!("failed writing {}", staged_metadata_path.display()))
        {
            return StagedSnapshotAttemptResult::HardFailure {
                manifest: Some(manifest),
                progress,
                attempt_duration_ms: started.elapsed().as_millis().min(u64::MAX as u128) as u64,
                reason: error.to_string(),
            };
        }
    }

    if budget_exhausted {
        progress.preserved_for_retry =
            progress.resumed_from_existing_stage || current_state.row_count > 0;
        if !progress.preserved_for_retry {
            let _ = remove_staged_snapshot_artifacts(&staged_snapshot_path, &staged_metadata_path);
        }
        return StagedSnapshotAttemptResult::Deferred(StagedSnapshotAttempt {
            manifest,
            progress,
            attempt_duration_ms: started.elapsed().as_millis().min(u64::MAX as u128) as u64,
        });
    }

    StagedSnapshotAttemptResult::Completed(StagedSnapshotAttempt {
        manifest,
        progress,
        attempt_duration_ms: started.elapsed().as_millis().min(u64::MAX as u128) as u64,
    })
}
