use super::*;

#[cfg(test)]
pub(crate) fn source_window_outran_staged_progress(
    source_state: &RecentRawJournalStateRow,
    staged_manifest: &RecentRawJournalSnapshotManifest,
) -> bool {
    if source_state.row_count == 0 {
        return staged_manifest.row_count > 0;
    }
    if let (Some(source_since), Some(staged_since)) =
        (source_state.covered_since, staged_manifest.covered_since)
    {
        if source_since > staged_since {
            return true;
        }
    }
    if let (Some(source_cursor), Some(staged_cursor)) = (
        source_state.covered_through_cursor.as_ref(),
        staged_manifest.covered_through_cursor.as_ref(),
    ) {
        return discovery_runtime_cursor_cmp(source_cursor, staged_cursor)
            == std::cmp::Ordering::Greater;
    }
    false
}

pub(crate) fn reference_surface_outran_staged_progress(
    reference_manifest: &RecentRawJournalSnapshotManifest,
    staged_manifest: &RecentRawJournalSnapshotManifest,
) -> bool {
    if reference_manifest.row_count == 0 {
        return staged_manifest.row_count > 0;
    }
    if reference_manifest.row_count > staged_manifest.row_count {
        return true;
    }
    if let (Some(reference_since), Some(staged_since)) = (
        reference_manifest.covered_since,
        staged_manifest.covered_since,
    ) {
        if reference_since > staged_since {
            return true;
        }
    }
    if let (Some(reference_cursor), Some(staged_cursor)) = (
        reference_manifest.covered_through_cursor.as_ref(),
        staged_manifest.covered_through_cursor.as_ref(),
    ) {
        return discovery_runtime_cursor_cmp(reference_cursor, staged_cursor)
            == std::cmp::Ordering::Greater;
    }
    false
}

pub(crate) fn source_contract_no_longer_matches_staged_progress(
    source_state: &RecentRawJournalStateRow,
    staged_manifest: &RecentRawJournalSnapshotManifest,
) -> bool {
    if source_state.row_count < staged_manifest.row_count {
        return true;
    }
    if let (Some(source_since), Some(staged_since)) =
        (source_state.covered_since, staged_manifest.covered_since)
    {
        if source_since > staged_since {
            return true;
        }
    }
    if let (Some(source_cursor), Some(staged_cursor)) = (
        source_state.covered_through_cursor.as_ref(),
        staged_manifest.covered_through_cursor.as_ref(),
    ) {
        return discovery_runtime_cursor_cmp(source_cursor, staged_cursor)
            == std::cmp::Ordering::Less;
    }
    false
}

pub(crate) fn published_latest_supersedes_staged_progress(
    latest_manifest: &RecentRawJournalSnapshotManifest,
    staged_manifest: &RecentRawJournalSnapshotManifest,
) -> bool {
    latest_manifest.source_db_path == staged_manifest.source_db_path
        && reference_surface_outran_staged_progress(latest_manifest, staged_manifest)
}

pub(crate) fn latest_surface_can_seed_staged_progress(
    source_db_path: &Path,
    source_state: &RecentRawJournalStateRow,
    latest_manifest: &RecentRawJournalSnapshotManifest,
    staged_manifest: Option<&RecentRawJournalSnapshotManifest>,
) -> bool {
    latest_manifest.source_db_path == source_db_path.display().to_string()
        && latest_manifest.row_count > 0
        && !source_contract_no_longer_matches_staged_progress(source_state, latest_manifest)
        && staged_manifest.map_or(true, |staged_manifest| {
            published_latest_supersedes_staged_progress(latest_manifest, staged_manifest)
        })
}

pub(crate) fn seed_staged_snapshot_from_latest_surface(
    source_db_path: &Path,
    latest_snapshot_path: &Path,
    latest_manifest: &RecentRawJournalSnapshotManifest,
    staged_snapshot_path: &Path,
    staged_metadata_path: &Path,
) -> Result<RecentRawJournalSnapshotManifest> {
    let _ = remove_file_if_exists(&sqlite_snapshot_wal_path(staged_snapshot_path))?;
    let _ = remove_file_if_exists(&sqlite_snapshot_shm_path(staged_snapshot_path))?;
    link_or_copy_atomic(latest_snapshot_path, staged_snapshot_path).with_context(|| {
        format!(
            "failed seeding staged recent_raw snapshot {} from {}",
            staged_snapshot_path.display(),
            latest_snapshot_path.display()
        )
    })?;
    let staged_manifest = manifest_for_snapshot(
        source_db_path,
        staged_snapshot_path,
        latest_manifest.created_at,
    )?;
    write_json_atomic(staged_metadata_path, &staged_manifest)
        .with_context(|| format!("failed writing {}", staged_metadata_path.display()))?;
    Ok(staged_manifest)
}

pub(crate) fn staged_manifest_for_state(
    source_db_path: &Path,
    staged_snapshot_path: &Path,
    created_at: DateTime<Utc>,
    state: &RecentRawJournalStateRow,
) -> Result<RecentRawJournalSnapshotManifest> {
    let snapshot_bytes = fs::metadata(staged_snapshot_path)
        .with_context(|| format!("failed stat {}", staged_snapshot_path.display()))?
        .len();
    Ok(snapshot_manifest(
        created_at,
        source_db_path,
        staged_snapshot_path,
        state,
        snapshot_bytes,
    ))
}
