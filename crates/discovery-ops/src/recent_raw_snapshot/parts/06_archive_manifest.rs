fn manifest_for_existing_snapshot(
    source_db_path: &Path,
    snapshot_path: &Path,
) -> Result<RecentRawJournalSnapshotManifest> {
    let created_at = infer_created_at(snapshot_path)?;
    manifest_for_snapshot(source_db_path, snapshot_path, created_at)
}

fn infer_created_at(path: &Path) -> Result<DateTime<Utc>> {
    let modified = fs::metadata(path)
        .with_context(|| format!("failed stat {}", path.display()))?
        .modified()
        .with_context(|| format!("failed reading modified time for {}", path.display()))?;
    Ok(DateTime::<Utc>::from(modified))
}

fn manifest_for_snapshot(
    source_db_path: &Path,
    snapshot_path: &Path,
    created_at: DateTime<Utc>,
) -> Result<RecentRawJournalSnapshotManifest> {
    let snapshot_store = SqliteStore::open_read_only(snapshot_path)
        .with_context(|| format!("failed opening {}", snapshot_path.display()))?;
    let state = load_required_cached_recent_raw_state(
        &snapshot_store,
        snapshot_path,
        "snapshot manifest derivation",
    )?;
    let snapshot_bytes = fs::metadata(snapshot_path)
        .with_context(|| format!("failed stat {}", snapshot_path.display()))?
        .len();
    Ok(snapshot_manifest(
        created_at,
        source_db_path,
        snapshot_path,
        &state,
        snapshot_bytes,
    ))
}

fn load_required_cached_recent_raw_state(
    store: &SqliteStore,
    db_path: &Path,
    context_label: &str,
) -> Result<RecentRawJournalStateRow> {
    let state = store
        .recent_raw_journal_state_cached_read_only_required()
        .with_context(|| {
            format!(
                "failed loading cached recent_raw journal state from {} for {context_label}",
                db_path.display()
            )
        })?;
    validate_cached_recent_raw_state_for_resume(&state, db_path, context_label)?;
    Ok(state)
}

fn validate_cached_recent_raw_state_for_resume(
    state: &RecentRawJournalStateRow,
    db_path: &Path,
    context_label: &str,
) -> Result<()> {
    if state.row_count == 0 {
        if state.covered_since.is_some() || state.covered_through_cursor.is_some() {
            bail!(
                "cached recent_raw journal state from {} for {context_label} is invalid: row_count=0 but coverage fields are populated",
                db_path.display()
            );
        }
        return Ok(());
    }
    if state.covered_since.is_none() {
        bail!(
            "cached recent_raw journal state from {} for {context_label} is invalid: covered_since is missing for non-empty state",
            db_path.display()
        );
    }
    if state.covered_through_cursor.is_none() {
        bail!(
            "cached recent_raw journal state from {} for {context_label} is invalid: covered_through_cursor is missing for non-empty state",
            db_path.display()
        );
    }
    Ok(())
}
