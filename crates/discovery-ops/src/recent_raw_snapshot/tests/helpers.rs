use super::prelude::*;

pub(super) struct Fixture {
    pub(super) journal_store: SqliteStore,
    pub(super) journal_db_path: PathBuf,
    pub(super) config_path: PathBuf,
    pub(super) _temp: tempfile::TempDir,
}

pub(super) fn make_fixture(name: &str) -> Result<Fixture> {
    let temp = tempdir().context("failed to create tempdir")?;
    let journal_db_path = temp.path().join(format!("{name}.db"));
    let config_path = temp.path().join(format!("{name}.toml"));
    let journal_store = SqliteStore::open(&journal_db_path)?;
    std::fs::write(
            &config_path,
            format!(
                "[recent_raw_journal]\npath = \"{}\"\n\n[runtime_restore_ops]\njournal_snapshot_retention = 2\njournal_snapshot_cadence_minutes = 10\n",
                journal_db_path.display(),
            ),
        )
        .context("failed writing config")?;
    Ok(Fixture {
        journal_store,
        journal_db_path,
        config_path,
        _temp: temp,
    })
}

impl Fixture {
    pub(super) fn snapshot_dir(&self) -> PathBuf {
        self.config_path
            .parent()
            .expect("config parent")
            .join("state/discovery_restore/recent_raw")
    }
}

pub(super) fn seed_recent_raw_journal(store: &SqliteStore, now: DateTime<Utc>) -> Result<()> {
    store.insert_recent_raw_journal_batch(
        &[
            make_swap("sig-a", now - Duration::minutes(2), 10),
            make_swap("sig-b", now - Duration::minutes(1), 11),
        ],
        now,
    )?;
    Ok(())
}

pub(super) fn seed_recent_raw_journal_many(
    store: &SqliteStore,
    now: DateTime<Utc>,
    count: usize,
) -> Result<()> {
    let mut swaps = Vec::with_capacity(count);
    for idx in 0..count {
        let ts = now - Duration::seconds((count.saturating_sub(idx)) as i64);
        swaps.push(make_swap(
            &format!("sig-many-{idx:04}"),
            ts,
            10 + idx as u64,
        ));
    }
    store.insert_recent_raw_journal_batch(&swaps, now)?;
    Ok(())
}

pub(super) fn seed_recent_raw_journal_range(
    store: &SqliteStore,
    start: DateTime<Utc>,
    slot_start: u64,
    signature_prefix: &str,
    count: usize,
    completed_at: DateTime<Utc>,
) -> Result<()> {
    let mut swaps = Vec::with_capacity(count);
    for idx in 0..count {
        swaps.push(make_swap(
            &format!("{signature_prefix}-{idx:05}"),
            start + Duration::seconds(idx as i64),
            slot_start + idx as u64,
        ));
    }
    store.insert_recent_raw_journal_batch(&swaps, completed_at)?;
    Ok(())
}

pub(super) fn load_snapshot_state(
    snapshot_path: &std::path::Path,
) -> Result<RecentRawJournalStateRow> {
    let snapshot_store = SqliteStore::open_read_only(snapshot_path)?;
    snapshot_store.recent_raw_journal_state_read_only()
}

pub(super) fn assert_snapshot_manifest_matches_state(
    manifest: &RecentRawJournalSnapshotManifest,
    state: &RecentRawJournalStateRow,
    snapshot_path: &std::path::Path,
) -> Result<()> {
    assert_eq!(manifest.row_count, state.row_count);
    assert_eq!(manifest.covered_since, state.covered_since);
    assert_eq!(
        manifest.covered_through_cursor,
        state.covered_through_cursor
    );
    assert_eq!(
        manifest.last_batch_completed_at,
        state.last_batch_completed_at
    );
    assert_eq!(manifest.updated_at, state.updated_at);
    assert_eq!(
        manifest.snapshot_bytes,
        std::fs::metadata(snapshot_path)?.len()
    );
    Ok(())
}

pub(super) fn make_swap(signature: &str, ts_utc: DateTime<Utc>, slot: u64) -> SwapEvent {
    SwapEvent {
        wallet: "wallet-restore".to_string(),
        dex: "raydium".to_string(),
        token_in: "So11111111111111111111111111111111111111112".to_string(),
        token_out: format!("token-{signature}"),
        amount_in: 1.0,
        amount_out: 10.0,
        signature: signature.to_string(),
        slot,
        ts_utc,
        exact_amounts: None,
    }
}

pub(super) fn parse_ts(raw: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
}

pub(super) fn archive_count(snapshot_dir: &PathBuf) -> Result<usize> {
    Ok(std::fs::read_dir(snapshot_dir)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with("discovery_recent_raw_") && name.ends_with(".sqlite")
                })
        })
        .count())
}

pub(super) fn staged_artifact_count(snapshot_dir: &Path) -> Result<usize> {
    Ok(std::fs::read_dir(snapshot_dir)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with(".discovery_recent_raw_")
                        && (name.ends_with(super::STAGED_ARCHIVE_SNAPSHOT_SUFFIX)
                            || name.ends_with(super::STAGED_ARCHIVE_METADATA_SUFFIX))
                })
        })
        .count())
}

pub(super) fn seed_fake_archive_set(snapshot_dir: &Path, slug: &str) -> Result<()> {
    std::fs::create_dir_all(snapshot_dir)?;
    let sqlite_path = snapshot_dir.join(format!("discovery_recent_raw_{slug}.sqlite"));
    std::fs::write(&sqlite_path, format!("archive-{slug}"))?;
    std::fs::write(
        sqlite_path.with_extension("json"),
        format!("manifest-{slug}"),
    )?;
    std::fs::write(
        super::sqlite_snapshot_wal_path(&sqlite_path),
        format!("wal-{slug}"),
    )?;
    std::fs::write(
        super::sqlite_snapshot_shm_path(&sqlite_path),
        format!("shm-{slug}"),
    )?;
    Ok(())
}

pub(super) fn assert_fake_archive_set_absent(snapshot_dir: &Path, slug: &str) {
    let sqlite_path = snapshot_dir.join(format!("discovery_recent_raw_{slug}.sqlite"));
    assert!(!sqlite_path.exists());
    assert!(!sqlite_path.with_extension("json").exists());
    assert!(!super::sqlite_snapshot_wal_path(&sqlite_path).exists());
    assert!(!super::sqlite_snapshot_shm_path(&sqlite_path).exists());
}

pub(super) fn assert_fake_archive_set_present(snapshot_dir: &Path, slug: &str) {
    let sqlite_path = snapshot_dir.join(format!("discovery_recent_raw_{slug}.sqlite"));
    assert!(sqlite_path.exists());
    assert!(sqlite_path.with_extension("json").exists());
    assert!(super::sqlite_snapshot_wal_path(&sqlite_path).exists());
    assert!(super::sqlite_snapshot_shm_path(&sqlite_path).exists());
}
