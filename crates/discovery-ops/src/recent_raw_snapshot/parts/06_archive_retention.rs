use super::*;

pub(crate) fn list_archive_snapshot_paths(snapshot_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut archives = Vec::new();
    if !snapshot_dir.exists() {
        return Ok(archives);
    }
    for entry in fs::read_dir(snapshot_dir)
        .with_context(|| format!("failed reading {}", snapshot_dir.display()))?
    {
        let entry = entry.with_context(|| format!("failed reading {}", snapshot_dir.display()))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.starts_with(JOURNAL_SNAPSHOT_ARCHIVE_PREFIX)
            && name.ends_with(JOURNAL_SNAPSHOT_ARCHIVE_SUFFIX)
        {
            archives.push(path);
        }
    }
    archives.sort_by(|left, right| right.file_name().cmp(&left.file_name()));
    Ok(archives)
}

pub(crate) fn remove_orphan_archive_sidecars(snapshot_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut removed_paths = Vec::new();
    if !snapshot_dir.exists() {
        return Ok(removed_paths);
    }
    for entry in fs::read_dir(snapshot_dir)
        .with_context(|| format!("failed reading {}", snapshot_dir.display()))?
    {
        let entry = entry.with_context(|| format!("failed reading {}", snapshot_dir.display()))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.starts_with(JOURNAL_SNAPSHOT_ARCHIVE_PREFIX) && name.ends_with(".json") {
            let sqlite_path = path.with_extension("sqlite");
            if !sqlite_path.exists() {
                if remove_file_if_exists(&path)? {
                    removed_paths.push(path);
                }
            }
            continue;
        }
        if name.starts_with(JOURNAL_SNAPSHOT_ARCHIVE_PREFIX)
            && (name.ends_with(".sqlite-wal") || name.ends_with(".sqlite-shm"))
        {
            let sqlite_name = name
                .trim_end_matches("-wal")
                .trim_end_matches("-shm")
                .to_string();
            let sqlite_path = snapshot_dir.join(sqlite_name);
            if !sqlite_path.exists() && remove_file_if_exists(&path)? {
                removed_paths.push(path);
            }
        }
    }
    Ok(removed_paths)
}

pub(crate) fn cleanup_stale_staged_snapshot_artifacts(
    snapshot_dir: &Path,
    preserve_paths: &[PathBuf],
) -> Result<Vec<PathBuf>> {
    let mut removed_paths = Vec::new();
    if !snapshot_dir.exists() {
        return Ok(removed_paths);
    }
    for entry in fs::read_dir(snapshot_dir)
        .with_context(|| format!("failed reading {}", snapshot_dir.display()))?
    {
        let entry = entry.with_context(|| format!("failed reading {}", snapshot_dir.display()))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let is_hidden_recent_raw_temp = name.starts_with('.')
            && name.contains(JOURNAL_SNAPSHOT_ARCHIVE_PREFIX)
            && (name.contains(".snapshot-tmp-")
                || name.contains(".tmp-")
                || name.ends_with(STAGED_ARCHIVE_SNAPSHOT_SUFFIX)
                || name.ends_with(STAGED_ARCHIVE_METADATA_SUFFIX));
        if is_hidden_recent_raw_temp
            && !preserve_paths
                .iter()
                .any(|preserve_path| preserve_path == &path)
            && remove_file_if_exists(&path)?
        {
            removed_paths.push(path);
        }
    }
    Ok(removed_paths)
}

pub(crate) fn enforce_snapshot_archive_retention(
    snapshot_dir: &Path,
    keep: usize,
    preserve_paths: &[PathBuf],
) -> Result<SnapshotArchiveMaintenance> {
    let cleanup_removed_paths =
        cleanup_stale_staged_snapshot_artifacts(snapshot_dir, preserve_paths)?;
    let mut orphan_cleanup_removed_paths = remove_orphan_archive_sidecars(snapshot_dir)?;
    let archive_paths = list_archive_snapshot_paths(snapshot_dir)?;
    let archive_set_count_before = archive_paths.len();
    let mut pruned_snapshot_paths = Vec::new();
    for snapshot_path in archive_paths.into_iter().skip(keep) {
        let removed_paths = remove_snapshot_set(&snapshot_path)?;
        if removed_paths.iter().any(|path| path == &snapshot_path) {
            pruned_snapshot_paths.push(snapshot_path);
        }
    }
    let archive_set_count_after = list_archive_snapshot_paths(snapshot_dir)?.len();
    let mut cleanup_removed_paths_all = cleanup_removed_paths;
    cleanup_removed_paths_all.append(&mut orphan_cleanup_removed_paths);
    let mut maintenance = SnapshotArchiveMaintenance::default();
    maintenance.record_pass(
        archive_set_count_before,
        archive_set_count_after,
        cleanup_removed_paths_all,
        pruned_snapshot_paths,
    );
    Ok(maintenance)
}
