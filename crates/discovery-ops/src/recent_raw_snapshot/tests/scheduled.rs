use super::helpers::*;
use super::prelude::*;

#[test]
fn scheduled_run_completes_under_live_source_writes() -> Result<()> {
    let fixture = make_fixture("recent-raw-snapshot-live-writes")?;
    let now = parse_ts("2026-03-23T12:00:00Z")?;
    seed_recent_raw_journal_many(&fixture.journal_store, now, 1024)?;

    let start_barrier = Arc::new(Barrier::new(2));
    let stop_writes = Arc::new(AtomicBool::new(false));
    let writer_path = fixture.journal_db_path.clone();
    let writer_barrier = start_barrier.clone();
    let writer_stop = stop_writes.clone();
    let writer_now = now + Duration::minutes(1);
    let writer = std::thread::spawn(move || -> Result<()> {
        let writer_store = SqliteStore::open(&writer_path)?;
        writer_barrier.wait();
        let mut counter = 0usize;
        while !writer_stop.load(Ordering::Relaxed) {
            writer_store.insert_recent_raw_journal_batch(
                &[make_swap(
                    &format!("sig-live-{counter:04}"),
                    writer_now + Duration::seconds(counter as i64),
                    20_000 + counter as u64,
                )],
                writer_now + Duration::seconds(counter as i64),
            )?;
            counter += 1;
        }
        Ok(())
    });

    start_barrier.wait();
    let written = run(Config {
        config_path: fixture.config_path.clone(),
        journal_db_path: Some(fixture.journal_db_path.clone()),
        output_path: None,
        scheduled: true,
        force: true,
        json: true,
        now,
    })?;
    stop_writes.store(true, Ordering::Relaxed);
    writer
        .join()
        .expect("writer thread panicked")
        .context("live writer thread failed")?;

    assert_eq!(written.exit_code, 0);
    let output: Value = serde_json::from_str(&written.rendered_output)?;
    assert_eq!(output["state"], "written");
    assert_eq!(output["terminal_reason"], "written");
    assert!(
        output["backup_copied_page_count"]
            .as_u64()
            .unwrap_or_default()
            >= output["backup_total_page_count"]
                .as_u64()
                .unwrap_or_default(),
        "completed snapshot must report full backup page coverage"
    );

    let latest_snapshot_path = fixture.snapshot_dir().join("latest.sqlite");
    let latest_manifest: RecentRawJournalSnapshotManifest =
        load_json(&fixture.snapshot_dir().join("latest.json"))?;
    let latest_state = load_snapshot_state(&latest_snapshot_path)?;
    assert_snapshot_manifest_matches_state(&latest_manifest, &latest_state, &latest_snapshot_path)?;
    Ok(())
}

#[test]
fn snapshot_service_and_timer_templates_match_bounded_attempt_contract() -> Result<()> {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let service = std::fs::read_to_string(
        repo_root.join("ops/server_templates/copybot-discovery-recent-raw-snapshot.service"),
    )
    .context("failed reading snapshot service template")?;
    let timer = std::fs::read_to_string(
        repo_root.join("ops/server_templates/copybot-discovery-recent-raw-snapshot.timer"),
    )
    .context("failed reading snapshot timer template")?;
    assert!(
        service.contains("SuccessExitStatus=75"),
        "snapshot service must keep transient deferred outcomes non-fatal for systemd"
    );
    assert!(
            service.contains("TimeoutStartSec=10min"),
            "snapshot service must leave enough outer runtime budget for bounded finalize and retention cleanup"
        );
    assert!(
        timer.contains("OnUnitActiveSec=5m"),
        "snapshot timer should keep periodic retries enabled for bounded deferred runs"
    );
    Ok(())
}

#[test]
fn scheduled_run_snapshots_latest_and_prunes_archives() -> Result<()> {
    let fixture = make_fixture("recent-raw-snapshot")?;
    let first_now = parse_ts("2026-03-23T12:00:00Z")?;
    let second_now = parse_ts("2026-03-23T12:11:00Z")?;
    let third_now = parse_ts("2026-03-23T12:22:00Z")?;
    seed_recent_raw_journal(&fixture.journal_store, third_now)?;

    for now in [first_now, second_now, third_now] {
        run(Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: None,
            scheduled: true,
            force: true,
            json: false,
            now,
        })?;
    }

    let snapshot_dir = fixture
        .config_path
        .parent()
        .expect("config parent")
        .join("state/discovery_restore/recent_raw");
    let archives = std::fs::read_dir(&snapshot_dir)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with("discovery_recent_raw_") && name.ends_with(".sqlite")
                })
        })
        .collect::<Vec<_>>();
    assert_eq!(archives.len(), 2, "retention should prune oldest snapshot");
    assert!(snapshot_dir.join("latest.sqlite").exists());
    assert!(snapshot_dir.join("latest.json").exists());
    Ok(())
}

#[test]
fn scheduled_run_skips_when_cadence_not_elapsed() -> Result<()> {
    let fixture = make_fixture("recent-raw-snapshot-skip")?;
    let now = parse_ts("2026-03-23T12:00:00Z")?;
    seed_recent_raw_journal(&fixture.journal_store, now)?;
    let snapshot_dir = fixture.snapshot_dir();

    run(Config {
        config_path: fixture.config_path.clone(),
        journal_db_path: Some(fixture.journal_db_path.clone()),
        output_path: None,
        scheduled: true,
        force: false,
        json: false,
        now,
    })?;
    assert!(snapshot_dir.join("latest.sqlite").exists());
    assert!(snapshot_dir.join("latest.json").exists());

    let skipped = run(Config {
        config_path: fixture.config_path.clone(),
        journal_db_path: Some(fixture.journal_db_path.clone()),
        output_path: None,
        scheduled: true,
        force: false,
        json: true,
        now: now + Duration::minutes(2),
    })?;
    assert_eq!(skipped.exit_code, 0);
    let output: Value = serde_json::from_str(&skipped.rendered_output)?;
    assert_eq!(output["state"], "skipped_not_due");
    assert_eq!(output["latest_surface_status"], "healthy");
    assert_eq!(output["latest_surface_action"], "healthy_skip");
    Ok(())
}

#[test]
fn scheduled_run_recreates_latest_sqlite_when_metadata_exists_but_latest_sqlite_missing(
) -> Result<()> {
    let fixture = make_fixture("recent-raw-snapshot-self-heal-sqlite")?;
    let now = parse_ts("2026-03-23T12:00:00Z")?;
    seed_recent_raw_journal(&fixture.journal_store, now)?;
    let snapshot_dir = fixture.snapshot_dir();

    run(Config {
        config_path: fixture.config_path.clone(),
        journal_db_path: Some(fixture.journal_db_path.clone()),
        output_path: None,
        scheduled: true,
        force: false,
        json: false,
        now,
    })?;
    let archive_count_before = archive_count(&snapshot_dir)?;
    std::fs::remove_file(snapshot_dir.join("latest.sqlite"))?;

    let healed = run(Config {
        config_path: fixture.config_path.clone(),
        journal_db_path: Some(fixture.journal_db_path.clone()),
        output_path: None,
        scheduled: true,
        force: false,
        json: true,
        now: now + Duration::minutes(2),
    })?;
    assert_eq!(healed.exit_code, 0);
    let output: Value = serde_json::from_str(&healed.rendered_output)?;
    assert_eq!(output["state"], "self_healed_latest_surface");
    assert_eq!(output["latest_surface_status"], "missing_latest_snapshot");
    assert_eq!(
        output["latest_surface_action"],
        "recreated_latest_snapshot_from_archive"
    );
    assert!(snapshot_dir.join("latest.sqlite").exists());
    assert_eq!(archive_count(&snapshot_dir)?, archive_count_before);
    Ok(())
}

#[test]
fn scheduled_run_rewrites_latest_metadata_when_metadata_missing_and_latest_sqlite_exists(
) -> Result<()> {
    let fixture = make_fixture("recent-raw-snapshot-self-heal-metadata")?;
    let now = parse_ts("2026-03-23T12:00:00Z")?;
    seed_recent_raw_journal(&fixture.journal_store, now)?;
    let snapshot_dir = fixture.snapshot_dir();

    run(Config {
        config_path: fixture.config_path.clone(),
        journal_db_path: Some(fixture.journal_db_path.clone()),
        output_path: None,
        scheduled: true,
        force: false,
        json: false,
        now,
    })?;
    let archive_count_before = archive_count(&snapshot_dir)?;
    std::fs::remove_file(snapshot_dir.join("latest.json"))?;

    let healed = run(Config {
        config_path: fixture.config_path.clone(),
        journal_db_path: Some(fixture.journal_db_path.clone()),
        output_path: None,
        scheduled: true,
        force: false,
        json: true,
        now: now + Duration::minutes(2),
    })?;
    assert_eq!(healed.exit_code, 0);
    let output: Value = serde_json::from_str(&healed.rendered_output)?;
    assert_eq!(output["state"], "self_healed_latest_surface");
    assert_eq!(output["latest_surface_status"], "missing_latest_metadata");
    assert_eq!(
        output["latest_surface_action"],
        "rewrote_latest_metadata_from_archive"
    );
    assert!(snapshot_dir.join("latest.sqlite").exists());
    assert!(snapshot_dir.join("latest.json").exists());
    assert_eq!(archive_count(&snapshot_dir)?, archive_count_before);
    Ok(())
}

#[test]
fn scheduled_run_retention_still_prunes_after_self_heal_path() -> Result<()> {
    let fixture = make_fixture("recent-raw-snapshot-self-heal-retention")?;
    let first_now = parse_ts("2026-03-23T12:00:00Z")?;
    let second_now = parse_ts("2026-03-23T12:11:00Z")?;
    let third_now = parse_ts("2026-03-23T12:22:00Z")?;
    seed_recent_raw_journal(&fixture.journal_store, third_now)?;
    let snapshot_dir = fixture.snapshot_dir();

    run(Config {
        config_path: fixture.config_path.clone(),
        journal_db_path: Some(fixture.journal_db_path.clone()),
        output_path: None,
        scheduled: true,
        force: true,
        json: false,
        now: first_now,
    })?;
    run(Config {
        config_path: fixture.config_path.clone(),
        journal_db_path: Some(fixture.journal_db_path.clone()),
        output_path: None,
        scheduled: true,
        force: true,
        json: false,
        now: second_now,
    })?;
    std::fs::remove_file(snapshot_dir.join("latest.sqlite"))?;
    let healed = run(Config {
        config_path: fixture.config_path.clone(),
        journal_db_path: Some(fixture.journal_db_path.clone()),
        output_path: None,
        scheduled: true,
        force: false,
        json: true,
        now: second_now + Duration::minutes(2),
    })?;
    assert_eq!(healed.exit_code, 0);
    let healed_output: Value = serde_json::from_str(&healed.rendered_output)?;
    assert_eq!(healed_output["state"], "self_healed_latest_surface");

    run(Config {
        config_path: fixture.config_path.clone(),
        journal_db_path: Some(fixture.journal_db_path.clone()),
        output_path: None,
        scheduled: true,
        force: true,
        json: false,
        now: third_now,
    })?;
    assert_eq!(archive_count(&snapshot_dir)?, 2);
    Ok(())
}

#[test]
fn scheduled_skip_prunes_full_snapshot_sets_and_sidecars() -> Result<()> {
    let fixture = make_fixture("recent-raw-snapshot-full-set-prune")?;
    let now = parse_ts("2026-03-23T12:00:00Z")?;
    seed_recent_raw_journal(&fixture.journal_store, now)?;
    let snapshot_dir = fixture.snapshot_dir();

    run(Config {
        config_path: fixture.config_path.clone(),
        journal_db_path: Some(fixture.journal_db_path.clone()),
        output_path: None,
        scheduled: true,
        force: false,
        json: false,
        now,
    })?;

    seed_fake_archive_set(&snapshot_dir, "20260323T115500Z")?;
    seed_fake_archive_set(&snapshot_dir, "20260323T115000Z")?;

    let skipped = run(Config {
        config_path: fixture.config_path.clone(),
        journal_db_path: Some(fixture.journal_db_path.clone()),
        output_path: None,
        scheduled: true,
        force: false,
        json: true,
        now: now + Duration::minutes(2),
    })?;
    assert_eq!(skipped.exit_code, 0);
    let output: Value = serde_json::from_str(&skipped.rendered_output)?;
    assert_eq!(output["state"], "skipped_not_due");
    assert_eq!(output["archive_set_count_before"], 3);
    assert_eq!(output["archive_set_count_after"], 2);
    assert_eq!(archive_count(&snapshot_dir)?, 2);
    assert_fake_archive_set_absent(&snapshot_dir, "20260323T115000Z");
    assert_fake_archive_set_present(&snapshot_dir, "20260323T115500Z");
    Ok(())
}

#[test]
fn scheduled_failure_before_archive_promotion_keeps_latest_surface_without_archive_growth(
) -> Result<()> {
    let fixture = make_fixture("recent-raw-snapshot-pre-promotion-failure")?;
    let now = parse_ts("2026-03-23T12:00:00Z")?;
    seed_recent_raw_journal(&fixture.journal_store, now)?;
    let snapshot_dir = fixture.snapshot_dir();
    let _guard = install_pre_archive_promotion_hook(|_| {
        Err(anyhow::anyhow!("synthetic_pre_archive_promotion_failure"))
    });

    let failed = run(Config {
        config_path: fixture.config_path.clone(),
        journal_db_path: Some(fixture.journal_db_path.clone()),
        output_path: None,
        scheduled: true,
        force: true,
        json: true,
        now,
    })?;
    assert_eq!(failed.exit_code, 1);
    let output: Value = serde_json::from_str(&failed.rendered_output)?;
    assert_eq!(output["state"], "hard_failure");
    assert_eq!(output["archive_promoted"], false);
    assert_eq!(output["archive_set_count_after"], 0);
    let archive_path = PathBuf::from(
        output["archive_path"]
            .as_str()
            .context("archive path must be present for failed scheduled snapshot")?,
    );
    assert!(
        !archive_path.exists(),
        "failed run must not promote archive sqlite"
    );
    assert!(!archive_path.with_extension("json").exists());
    assert!(snapshot_dir.join("latest.sqlite").exists());
    assert!(snapshot_dir.join("latest.json").exists());
    assert_eq!(archive_count(&snapshot_dir)?, 0);
    Ok(())
}

#[test]
fn explicit_run_writes_snapshot_and_manifest() -> Result<()> {
    let fixture = make_fixture("recent-raw-snapshot-explicit")?;
    let now = parse_ts("2026-03-23T12:00:00Z")?;
    seed_recent_raw_journal(&fixture.journal_store, now)?;

    run(Config {
        config_path: fixture.config_path.clone(),
        journal_db_path: Some(fixture.journal_db_path.clone()),
        output_path: Some(PathBuf::from("snapshots/manual.sqlite")),
        scheduled: false,
        force: false,
        json: false,
        now,
    })?;

    let snapshot_path = fixture
        .config_path
        .parent()
        .expect("config parent")
        .join("snapshots/manual.sqlite");
    let manifest_path = snapshot_path.with_extension("json");
    assert!(snapshot_path.exists());
    let manifest: RecentRawJournalSnapshotManifest = load_json(&manifest_path)?;
    assert_eq!(manifest.row_count, 2);

    let snapshot_store = SqliteStore::open_read_only(&snapshot_path)?;
    let state = snapshot_store.recent_raw_journal_state_read_only()?;
    assert_eq!(state.row_count, 2);
    assert_eq!(manifest.covered_since, state.covered_since);
    assert_eq!(
        manifest.covered_through_cursor,
        state.covered_through_cursor
    );
    assert_eq!(
        manifest.last_batch_completed_at,
        state.last_batch_completed_at
    );
    assert_eq!(
        manifest.snapshot_bytes,
        std::fs::metadata(&snapshot_path)?.len()
    );
    Ok(())
}

#[test]
fn manifest_for_snapshot_uses_cached_state_without_scanning_observed_swaps() -> Result<()> {
    let fixture = make_fixture("recent-raw-snapshot-cached-manifest")?;
    let now = parse_ts("2026-03-23T12:00:00Z")?;
    seed_recent_raw_journal(&fixture.journal_store, now)?;

    run(Config {
        config_path: fixture.config_path.clone(),
        journal_db_path: Some(fixture.journal_db_path.clone()),
        output_path: Some(PathBuf::from("snapshots/manual.sqlite")),
        scheduled: false,
        force: false,
        json: false,
        now,
    })?;

    let snapshot_path = fixture
        .config_path
        .parent()
        .expect("config parent")
        .join("snapshots/manual.sqlite");
    let manifest_path = snapshot_path.with_extension("json");
    let stored_manifest: RecentRawJournalSnapshotManifest = load_json(&manifest_path)?;

    let conn = rusqlite::Connection::open(&snapshot_path)?;
    conn.execute("DELETE FROM observed_swaps", [])?;

    let rebuilt_manifest = super::manifest_for_snapshot(
        &fixture.journal_db_path,
        &snapshot_path,
        stored_manifest.created_at,
    )?;
    assert_eq!(rebuilt_manifest.row_count, stored_manifest.row_count);
    assert_eq!(
        rebuilt_manifest.covered_since,
        stored_manifest.covered_since
    );
    assert_eq!(
        rebuilt_manifest.covered_through_cursor,
        stored_manifest.covered_through_cursor
    );
    assert_eq!(
        rebuilt_manifest.last_batch_completed_at,
        stored_manifest.last_batch_completed_at
    );
    Ok(())
}

#[test]
fn scheduled_run_manifest_metadata_matches_snapshot_files_even_if_source_advances_after_publish(
) -> Result<()> {
    let fixture = make_fixture("recent-raw-snapshot-drift")?;
    let now = parse_ts("2026-03-23T12:00:00Z")?;
    seed_recent_raw_journal(&fixture.journal_store, now)?;
    let live_journal_db_path = fixture.journal_db_path.clone();
    let live_advance_at = now + Duration::minutes(1);
    let _guard = super::install_post_snapshot_publish_hook(move |_| {
        let live_writer = SqliteStore::open(&live_journal_db_path)?;
        live_writer.insert_recent_raw_journal_batch(
            &[make_swap("sig-c", live_advance_at, 12)],
            live_advance_at,
        )?;
        Ok(())
    });

    let written = run(Config {
        config_path: fixture.config_path.clone(),
        journal_db_path: Some(fixture.journal_db_path.clone()),
        output_path: None,
        scheduled: true,
        force: true,
        json: true,
        now,
    })?;
    assert_eq!(written.exit_code, 0);
    let output: Value = serde_json::from_str(&written.rendered_output)?;
    assert_eq!(output["state"], "written");

    let latest_snapshot_path = fixture.snapshot_dir().join("latest.sqlite");
    let latest_metadata_path = fixture.snapshot_dir().join("latest.json");
    let latest_manifest: RecentRawJournalSnapshotManifest = load_json(&latest_metadata_path)?;
    let latest_state = load_snapshot_state(&latest_snapshot_path)?;
    assert_snapshot_manifest_matches_state(&latest_manifest, &latest_state, &latest_snapshot_path)?;

    let archive_path = PathBuf::from(
        output["archive_path"]
            .as_str()
            .context("archive path must be present for written scheduled snapshot")?,
    );
    let archive_manifest_path = archive_path.with_extension("json");
    let archive_manifest: RecentRawJournalSnapshotManifest = load_json(&archive_manifest_path)?;
    let archive_state = load_snapshot_state(&archive_path)?;
    assert_snapshot_manifest_matches_state(&archive_manifest, &archive_state, &archive_path)?;

    let source_state = SqliteStore::open_read_only(&fixture.journal_db_path)?
        .recent_raw_journal_state_read_only()?;
    assert_eq!(latest_manifest.row_count, 2);
    assert_eq!(archive_manifest.row_count, 2);
    assert_eq!(
        latest_manifest.covered_through_cursor,
        latest_state.covered_through_cursor
    );
    assert_eq!(
        archive_manifest.covered_through_cursor,
        archive_state.covered_through_cursor
    );
    assert!(source_state.row_count > latest_manifest.row_count);
    assert_ne!(
        source_state.covered_through_cursor,
        latest_manifest.covered_through_cursor
    );
    assert_ne!(
        source_state.last_batch_completed_at,
        latest_manifest.last_batch_completed_at
    );
    Ok(())
}
