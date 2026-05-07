use super::*;

pub(super) fn archive_candidate(
    snapshot_dir: &Path,
    manifest: Option<&RecentRawJournalSnapshotManifest>,
) -> Option<PathBuf> {
    if let Some(manifest) = manifest {
        let candidate = PathBuf::from(&manifest.snapshot_path);
        if candidate.exists() && candidate.is_file() {
            return Some(candidate);
        }
    }
    newest_snapshot_archive(snapshot_dir)
}

pub(super) fn newest_snapshot_archive(snapshot_dir: &Path) -> Option<PathBuf> {
    let mut archives = fs::read_dir(snapshot_dir)
        .ok()?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.is_file()
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name.starts_with(JOURNAL_SNAPSHOT_ARCHIVE_PREFIX)
                            && name.ends_with(JOURNAL_SNAPSHOT_ARCHIVE_SUFFIX)
                    })
        })
        .collect::<Vec<_>>();
    archives.sort_by(|left, right| right.file_name().cmp(&left.file_name()));
    archives.into_iter().next()
}

pub(super) fn staged_snapshot_archive_path(snapshot_dir: &Path) -> PathBuf {
    snapshot_dir.join(format!(
        ".{JOURNAL_SNAPSHOT_ARCHIVE_PREFIX}staged{JOURNAL_SNAPSHOT_ARCHIVE_SUFFIX}{STAGED_ARCHIVE_SNAPSHOT_SUFFIX}"
    ))
}

pub(super) fn staged_snapshot_metadata_path(snapshot_dir: &Path) -> PathBuf {
    snapshot_dir.join(format!(
        ".{JOURNAL_SNAPSHOT_ARCHIVE_PREFIX}staged{JOURNAL_SNAPSHOT_ARCHIVE_SUFFIX}{STAGED_ARCHIVE_METADATA_SUFFIX}"
    ))
}

pub(super) fn sqlite_snapshot_wal_path(snapshot_path: &Path) -> PathBuf {
    let mut wal_path = snapshot_path.as_os_str().to_os_string();
    wal_path.push("-wal");
    PathBuf::from(wal_path)
}

pub(super) fn sqlite_snapshot_shm_path(snapshot_path: &Path) -> PathBuf {
    let mut shm_path = snapshot_path.as_os_str().to_os_string();
    shm_path.push("-shm");
    PathBuf::from(shm_path)
}

pub(super) fn remove_file_if_exists(path: &Path) -> Result<bool> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).with_context(|| format!("failed removing {}", path.display())),
    }
}

pub(super) fn remove_snapshot_set(snapshot_path: &Path) -> Result<Vec<PathBuf>> {
    let mut removed_paths = Vec::new();
    for path in [
        snapshot_path.to_path_buf(),
        journal_snapshot_metadata_path(snapshot_path),
        sqlite_snapshot_wal_path(snapshot_path),
        sqlite_snapshot_shm_path(snapshot_path),
    ] {
        if remove_file_if_exists(&path)? {
            removed_paths.push(path);
        }
    }
    Ok(removed_paths)
}

pub(super) fn remove_staged_snapshot_artifacts(
    staged_snapshot_path: &Path,
    staged_metadata_path: &Path,
) -> Result<Vec<PathBuf>> {
    let mut removed_paths = Vec::new();
    for path in [
        staged_snapshot_path.to_path_buf(),
        staged_metadata_path.to_path_buf(),
        sqlite_snapshot_wal_path(staged_snapshot_path),
        sqlite_snapshot_shm_path(staged_snapshot_path),
    ] {
        if remove_file_if_exists(&path)? {
            removed_paths.push(path);
        }
    }
    Ok(removed_paths)
}

pub(super) fn load_existing_staged_manifest(
    source_db_path: &Path,
    staged_snapshot_path: &Path,
    staged_metadata_path: &Path,
) -> Result<Option<RecentRawJournalSnapshotManifest>> {
    if !staged_snapshot_path.exists() {
        let _ = remove_file_if_exists(staged_metadata_path)?;
        return Ok(None);
    }
    let created_at = match load_json::<RecentRawJournalSnapshotManifest>(staged_metadata_path) {
        Ok(manifest) => manifest.created_at,
        Err(_) => infer_created_at(staged_snapshot_path)?,
    };
    manifest_for_snapshot(source_db_path, staged_snapshot_path, created_at).map(Some)
}
