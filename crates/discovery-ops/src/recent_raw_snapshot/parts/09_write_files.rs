fn write_snapshot_with_policy(
    source_db_path: &Path,
    source_store: &SqliteStore,
    snapshot_path: &Path,
    now: DateTime<Utc>,
    snapshot_policy: &SqliteSnapshotPolicy,
) -> Result<(RecentRawJournalSnapshotManifest, SqliteSnapshotSummary), SnapshotWriteError> {
    if let Some(parent) = snapshot_path.parent() {
        fs::create_dir_all(parent).map_err(|error| SnapshotWriteError::HardFailure {
            summary: None,
            reason: format!(
                "failed creating snapshot parent dir {}: {error}",
                parent.display()
            ),
        })?;
    }
    let temp_snapshot_path = snapshot_temp_path(snapshot_path);
    cleanup_snapshot_temp(&temp_snapshot_path);
    let snapshot_outcome = source_store
        .snapshot_into_path_with_policy(&temp_snapshot_path, snapshot_policy)
        .map_err(|error| {
            cleanup_snapshot_temp(&temp_snapshot_path);
            SnapshotWriteError::HardFailure {
                summary: None,
                reason: format!("failed writing {}: {error}", snapshot_path.display()),
            }
        })?;
    let summary = match snapshot_outcome {
        SqliteSnapshotOutcome::Written(summary) => summary,
        SqliteSnapshotOutcome::RetryableBusy(summary) => {
            cleanup_snapshot_temp(&temp_snapshot_path);
            return Err(SnapshotWriteError::RetryableBusy { summary });
        }
        SqliteSnapshotOutcome::Deferred(summary) => {
            cleanup_snapshot_temp(&temp_snapshot_path);
            return Err(SnapshotWriteError::Deferred { summary });
        }
    };
    fs::rename(&temp_snapshot_path, snapshot_path).map_err(|error| {
        cleanup_snapshot_temp(&temp_snapshot_path);
        SnapshotWriteError::HardFailure {
            summary: Some(summary.clone()),
            reason: format!(
                "failed renaming {} to {}: {error}",
                temp_snapshot_path.display(),
                snapshot_path.display()
            ),
        }
    })?;
    invoke_post_snapshot_publish_hook(snapshot_path).map_err(|error| {
        SnapshotWriteError::HardFailure {
            summary: Some(summary.clone()),
            reason: format!(
                "failed running post-snapshot publish hook for {}: {error}",
                snapshot_path.display()
            ),
        }
    })?;
    let manifest = manifest_for_snapshot(source_db_path, snapshot_path, now).map_err(|error| {
        SnapshotWriteError::HardFailure {
            summary: Some(summary.clone()),
            reason: format!(
                "failed building manifest from snapshot {}: {error}",
                snapshot_path.display()
            ),
        }
    })?;
    Ok((manifest, summary))
}

fn snapshot_temp_path(snapshot_path: &Path) -> PathBuf {
    let file_name = snapshot_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("snapshot.sqlite");
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    snapshot_path.with_file_name(format!(
        ".{file_name}.snapshot-tmp-{}-{nonce}",
        process::id()
    ))
}

fn cleanup_snapshot_temp(path: &Path) {
    let _ = fs::remove_file(path);
}

fn link_or_copy_atomic(source_path: &Path, destination_path: &Path) -> Result<()> {
    if let Some(parent) = destination_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed creating {}", parent.display()))?;
    }
    let temp_path = snapshot_temp_path(destination_path);
    cleanup_snapshot_temp(&temp_path);
    match fs::hard_link(source_path, &temp_path) {
        Ok(()) => fs::rename(&temp_path, destination_path).with_context(|| {
            format!(
                "failed renaming {} to {}",
                temp_path.display(),
                destination_path.display()
            )
        }),
        Err(_) => copy_atomic(source_path, destination_path),
    }
}

fn snapshot_manifest(
    created_at: DateTime<Utc>,
    source_db_path: &Path,
    snapshot_path: &Path,
    state: &RecentRawJournalStateRow,
    snapshot_bytes: u64,
) -> RecentRawJournalSnapshotManifest {
    RecentRawJournalSnapshotManifest {
        created_at,
        source_db_path: source_db_path.display().to_string(),
        snapshot_path: snapshot_path.display().to_string(),
        row_count: state.row_count,
        covered_since: state.covered_since,
        covered_through_cursor: state.covered_through_cursor.clone(),
        last_batch_completed_at: state.last_batch_completed_at,
        updated_at: state.updated_at,
        snapshot_bytes,
    }
}
