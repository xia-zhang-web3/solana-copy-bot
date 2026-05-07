use super::*;

pub(crate) fn run_scheduled(
    config: &Config,
    source_db_path: &Path,
    source_store: &SqliteStore,
    configured_snapshot_dir: &str,
    cadence_minutes: u64,
    retention: usize,
    snapshot_context: &SnapshotContext,
) -> Result<SnapshotOutput> {
    let snapshot_dir =
        resolve_relative_to_config(&config.config_path, Path::new(configured_snapshot_dir));
    let staged_paths = vec![
        staged_snapshot_archive_path(&snapshot_dir),
        staged_snapshot_metadata_path(&snapshot_dir),
    ];
    let latest_snapshot_path = journal_snapshot_latest_path(&snapshot_dir);
    let latest_metadata_path = journal_snapshot_latest_metadata_path(&snapshot_dir);
    let cadence = Duration::minutes(cadence_minutes.max(1) as i64);
    let latest_surface = assess_latest_surface(&latest_snapshot_path, &latest_metadata_path)
        .with_context(|| {
            format!(
                "failed assessing latest snapshot surface in {}",
                snapshot_dir.display()
            )
        })?;
    let latest_reference_manifest = reference_manifest_for_cadence(
        source_db_path,
        &snapshot_dir,
        &latest_snapshot_path,
        &latest_surface,
    )?;
    let latest_is_due = latest_reference_manifest
        .as_ref()
        .map(|manifest| config.now.signed_duration_since(manifest.created_at) >= cadence)
        .unwrap_or(true);

    if !config.force && latest_surface.status == LatestSurfaceStatus::Healthy && !latest_is_due {
        let archive_maintenance =
            enforce_snapshot_archive_retention(&snapshot_dir, retention, &staged_paths)?;
        let manifest = latest_surface
            .manifest
            .as_ref()
            .expect("healthy latest surface includes manifest");
        return Ok(render_output(
            SnapshotState::SkippedNotDue,
            latest_surface.status,
            LatestSurfaceAction::HealthySkip,
            snapshot_context,
            &config.config_path,
            source_db_path,
            &latest_snapshot_path,
            &latest_metadata_path,
            None,
            Some(cadence_minutes),
            Some(retention),
            Some(manifest),
            None,
            None,
            None,
            None,
            SnapshotOutputContext {
                archive_promoted: false,
                archive_maintenance,
                ..SnapshotOutputContext::default()
            },
        ));
    }

    if !config.force && latest_surface.status != LatestSurfaceStatus::Healthy && !latest_is_due {
        if let Some((manifest, action)) = try_self_heal_latest_surface(
            source_db_path,
            &snapshot_dir,
            &latest_snapshot_path,
            &latest_metadata_path,
            latest_surface.clone(),
        )? {
            let archive_maintenance =
                enforce_snapshot_archive_retention(&snapshot_dir, retention, &staged_paths)?;
            return Ok(render_output(
                SnapshotState::SelfHealedLatestSurface,
                latest_surface.status,
                action,
                snapshot_context,
                &config.config_path,
                source_db_path,
                &latest_snapshot_path,
                &latest_metadata_path,
                None,
                Some(cadence_minutes),
                Some(retention),
                Some(&manifest),
                None,
                None,
                None,
                None,
                SnapshotOutputContext {
                    archive_promoted: false,
                    archive_maintenance,
                    ..SnapshotOutputContext::default()
                },
            ));
        }
    }

    let archive_maintenance =
        enforce_snapshot_archive_retention(&snapshot_dir, retention, &staged_paths)?;

    let action = if latest_surface.status == LatestSurfaceStatus::Healthy {
        LatestSurfaceAction::RefreshedFromSource
    } else {
        LatestSurfaceAction::RecreatedLatestSurfaceFromSource
    };
    let output = write_fresh_scheduled_snapshot(
        &config.config_path,
        source_db_path,
        source_store,
        &latest_snapshot_path,
        &latest_metadata_path,
        config.now,
        cadence_minutes,
        retention,
        latest_surface.status,
        action,
        &snapshot_dir,
        snapshot_context,
        latest_surface.manifest.as_ref(),
        archive_maintenance,
    )?;
    persist_latest_attempt_telemetry_best_effort(&snapshot_dir, &output);
    Ok(output)
}

pub(crate) fn latest_attempt_telemetry_path(snapshot_dir: &Path) -> PathBuf {
    snapshot_dir.join(LATEST_ATTEMPT_TELEMETRY_FILE_NAME)
}

pub(crate) fn persist_latest_attempt_telemetry_best_effort(
    snapshot_dir: &Path,
    output: &SnapshotOutput,
) {
    let telemetry_path = latest_attempt_telemetry_path(snapshot_dir);
    if let Err(error) = write_json_atomic(&telemetry_path, output)
        .with_context(|| format!("failed writing {}", telemetry_path.display()))
    {
        warn!(
            telemetry_path = %telemetry_path.display(),
            error = %error,
            "failed to persist latest recent_raw snapshot attempt telemetry"
        );
    }
}

pub(crate) fn assess_latest_surface(
    latest_snapshot_path: &Path,
    latest_metadata_path: &Path,
) -> Result<LatestSurfaceAssessment> {
    let latest_snapshot_exists = latest_snapshot_path.exists();
    let latest_metadata_exists = latest_metadata_path.exists();
    match (latest_metadata_exists, latest_snapshot_exists) {
        (true, true) => match load_json::<RecentRawJournalSnapshotManifest>(latest_metadata_path) {
            Ok(manifest) => Ok(LatestSurfaceAssessment {
                status: LatestSurfaceStatus::Healthy,
                manifest: Some(manifest),
            }),
            Err(_) => Ok(LatestSurfaceAssessment {
                status: LatestSurfaceStatus::InvalidLatestMetadata,
                manifest: None,
            }),
        },
        (true, false) => {
            match load_json::<RecentRawJournalSnapshotManifest>(latest_metadata_path) {
                Ok(manifest) => Ok(LatestSurfaceAssessment {
                    status: LatestSurfaceStatus::MissingLatestSnapshot,
                    manifest: Some(manifest),
                }),
                Err(_) => Ok(LatestSurfaceAssessment {
                    status: LatestSurfaceStatus::InvalidLatestMetadata,
                    manifest: None,
                }),
            }
        }
        (false, true) => Ok(LatestSurfaceAssessment {
            status: LatestSurfaceStatus::MissingLatestMetadata,
            manifest: None,
        }),
        (false, false) => Ok(LatestSurfaceAssessment {
            status: LatestSurfaceStatus::MissingBoth,
            manifest: None,
        }),
    }
}

pub(crate) fn try_self_heal_latest_surface(
    source_db_path: &Path,
    snapshot_dir: &Path,
    latest_snapshot_path: &Path,
    latest_metadata_path: &Path,
    latest_surface: LatestSurfaceAssessment,
) -> Result<Option<(RecentRawJournalSnapshotManifest, LatestSurfaceAction)>> {
    match latest_surface.status {
        LatestSurfaceStatus::NotApplicable => Ok(None),
        LatestSurfaceStatus::MissingLatestSnapshot => {
            if let Some(archive_path) =
                archive_candidate(snapshot_dir, latest_surface.manifest.as_ref())
            {
                copy_atomic(&archive_path, latest_snapshot_path).with_context(|| {
                    format!("failed restoring {}", latest_snapshot_path.display())
                })?;
                let manifest = manifest_for_existing_snapshot(source_db_path, &archive_path)?;
                write_json_atomic(latest_metadata_path, &manifest).with_context(|| {
                    format!("failed writing {}", latest_metadata_path.display())
                })?;
                Ok(Some((
                    manifest,
                    LatestSurfaceAction::RecreatedLatestSnapshotFromArchive,
                )))
            } else {
                Ok(None)
            }
        }
        LatestSurfaceStatus::MissingLatestMetadata | LatestSurfaceStatus::InvalidLatestMetadata => {
            if !latest_snapshot_path.exists() {
                return Ok(None);
            }
            if let Some(archive_path) =
                archive_candidate(snapshot_dir, latest_surface.manifest.as_ref())
            {
                let manifest = manifest_for_existing_snapshot(source_db_path, &archive_path)?;
                write_json_atomic(latest_metadata_path, &manifest).with_context(|| {
                    format!("failed writing {}", latest_metadata_path.display())
                })?;
                Ok(Some((
                    manifest,
                    LatestSurfaceAction::RewroteLatestMetadataFromArchive,
                )))
            } else {
                let manifest =
                    manifest_for_existing_snapshot(source_db_path, latest_snapshot_path)?;
                write_json_atomic(latest_metadata_path, &manifest).with_context(|| {
                    format!("failed writing {}", latest_metadata_path.display())
                })?;
                Ok(Some((
                    manifest,
                    LatestSurfaceAction::RewroteLatestMetadataFromLatestSqlite,
                )))
            }
        }
        LatestSurfaceStatus::Healthy | LatestSurfaceStatus::MissingBoth => Ok(None),
    }
}

pub(crate) fn reference_manifest_for_cadence(
    source_db_path: &Path,
    snapshot_dir: &Path,
    latest_snapshot_path: &Path,
    latest_surface: &LatestSurfaceAssessment,
) -> Result<Option<RecentRawJournalSnapshotManifest>> {
    match latest_surface.status {
        LatestSurfaceStatus::NotApplicable => Ok(None),
        LatestSurfaceStatus::Healthy | LatestSurfaceStatus::MissingLatestSnapshot => {
            Ok(latest_surface.manifest.clone())
        }
        LatestSurfaceStatus::MissingLatestMetadata | LatestSurfaceStatus::InvalidLatestMetadata => {
            if !latest_snapshot_path.exists() {
                return Ok(None);
            }
            if let Some(archive_path) =
                archive_candidate(snapshot_dir, latest_surface.manifest.as_ref())
            {
                return manifest_for_existing_snapshot(source_db_path, &archive_path).map(Some);
            }
            manifest_for_existing_snapshot(source_db_path, latest_snapshot_path).map(Some)
        }
        LatestSurfaceStatus::MissingBoth => Ok(None),
    }
}
