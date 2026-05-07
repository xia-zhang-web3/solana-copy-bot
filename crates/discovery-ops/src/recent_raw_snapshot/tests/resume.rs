use super::helpers::*;
use super::prelude::*;

#[test]
fn scheduled_run_reseeds_from_latest_surface_when_latest_outruns_staged_progress() -> Result<()> {
    let fixture = make_fixture("recent-raw-snapshot-outrun-stage")?;
    let initial_now = parse_ts("2026-03-23T12:00:00Z")?;
    seed_recent_raw_journal_range(
        &fixture.journal_store,
        initial_now - Duration::minutes(5),
        10,
        "sig-outrun-stage",
        16,
        initial_now,
    )?;
    run(Config {
        config_path: fixture.config_path.clone(),
        journal_db_path: Some(fixture.journal_db_path.clone()),
        output_path: None,
        scheduled: true,
        force: true,
        json: false,
        now: initial_now,
    })?;

    let snapshot_dir = fixture.snapshot_dir();
    let stale_stage_seed_path = snapshot_dir.join("stale-stage-seed.sqlite");
    let stale_stage_seed_metadata_path = snapshot_dir.join("stale-stage-seed.json");
    let initial_latest_snapshot_path = snapshot_dir.join("latest.sqlite");
    let initial_latest_manifest_path = snapshot_dir.join("latest.json");
    let initial_latest_manifest: RecentRawJournalSnapshotManifest =
        load_json(&initial_latest_manifest_path)?;
    std::fs::copy(&initial_latest_snapshot_path, &stale_stage_seed_path).with_context(|| {
        format!(
            "failed copying {} to {}",
            initial_latest_snapshot_path.display(),
            stale_stage_seed_path.display()
        )
    })?;
    write_json_atomic(&stale_stage_seed_metadata_path, &initial_latest_manifest)?;

    seed_recent_raw_journal_range(
        &fixture.journal_store,
        initial_now + Duration::minutes(1),
        10_000,
        "sig-outrun-source",
        128,
        initial_now + Duration::minutes(1),
    )?;
    run(Config {
        config_path: fixture.config_path.clone(),
        journal_db_path: Some(fixture.journal_db_path.clone()),
        output_path: None,
        scheduled: true,
        force: true,
        json: false,
        now: initial_now + Duration::minutes(15),
    })?;

    let staged_snapshot_path = super::staged_snapshot_archive_path(&snapshot_dir);
    let staged_metadata_path = super::staged_snapshot_metadata_path(&snapshot_dir);
    std::fs::copy(&stale_stage_seed_path, &staged_snapshot_path).with_context(|| {
        format!(
            "failed copying {} to {}",
            stale_stage_seed_path.display(),
            staged_snapshot_path.display()
        )
    })?;
    let current_latest_manifest: RecentRawJournalSnapshotManifest =
        load_json(&snapshot_dir.join("latest.json"))?;
    let mut staged_manifest = super::manifest_for_snapshot(
        &fixture.journal_db_path,
        &staged_snapshot_path,
        current_latest_manifest.created_at + Duration::minutes(1),
    )?;
    staged_manifest.created_at = current_latest_manifest.created_at + Duration::minutes(1);
    write_json_atomic(&staged_metadata_path, &staged_manifest)?;
    let current_source_state = fixture.journal_store.recent_raw_journal_state()?;
    assert!(source_window_outran_staged_progress(
        &current_source_state,
        &staged_manifest
    ));
    assert!(super::reference_surface_outran_staged_progress(
        &current_latest_manifest,
        &staged_manifest
    ));
    assert!(staged_manifest.created_at > current_latest_manifest.created_at);

    let written = run(Config {
        config_path: fixture.config_path.clone(),
        journal_db_path: Some(fixture.journal_db_path.clone()),
        output_path: None,
        scheduled: true,
        force: false,
        json: true,
        now: initial_now + Duration::minutes(30),
    })?;
    assert_eq!(written.exit_code, 0);
    let output: Value = serde_json::from_str(&written.rendered_output)?;
    assert_eq!(output["state"], "written");
    assert_eq!(
        output["staged_progress_resumed"],
        false,
        "unexpected outrun-stage output: {}",
        serde_json::to_string_pretty(&output)?
    );
    assert_eq!(output["staged_seeded_from_latest_surface"], true);
    assert_eq!(
        output["staged_row_count_before_attempt"].as_u64(),
        Some(current_latest_manifest.row_count as u64)
    );

    let promoted_latest_manifest: RecentRawJournalSnapshotManifest =
        load_json(&snapshot_dir.join("latest.json"))?;
    assert!(
        promoted_latest_manifest.row_count > staged_manifest.row_count,
        "lagging staged progress must be reseeded from the farther healthy latest surface"
    );
    Ok(())
}

#[test]
fn scheduled_run_does_not_seed_stage_from_foreign_latest_source_contract() -> Result<()> {
    let fixture = make_fixture("recent-raw-snapshot-foreign-latest")?;
    let initial_now = parse_ts("2026-03-23T12:00:00Z")?;
    seed_recent_raw_journal_range(
        &fixture.journal_store,
        initial_now - Duration::minutes(5),
        10,
        "sig-foreign-latest-initial",
        64,
        initial_now,
    )?;
    run(Config {
        config_path: fixture.config_path.clone(),
        journal_db_path: Some(fixture.journal_db_path.clone()),
        output_path: None,
        scheduled: true,
        force: true,
        json: false,
        now: initial_now,
    })?;

    let latest_manifest_path = fixture.snapshot_dir().join("latest.json");
    let mut foreign_latest_manifest: RecentRawJournalSnapshotManifest =
        load_json(&latest_manifest_path)?;
    foreign_latest_manifest.source_db_path = fixture
        .snapshot_dir()
        .join("foreign-recent-raw.db")
        .display()
        .to_string();
    write_json_atomic(&latest_manifest_path, &foreign_latest_manifest)?;

    seed_recent_raw_journal_range(
        &fixture.journal_store,
        initial_now + Duration::minutes(1),
        50_000,
        "sig-foreign-latest-extra",
        1_024,
        initial_now + Duration::minutes(1),
    )?;

    let _guard =
        install_resumable_snapshot_progress_hook(|completed_batches, _staged_row_count| {
            completed_batches >= 1
        });
    let deferred = run_with_snapshot_policy_override(
        Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: None,
            scheduled: true,
            force: false,
            json: true,
            now: initial_now + Duration::minutes(15),
        },
        Some(copybot_storage_core::SqliteSnapshotPolicy {
            busy_timeout: StdDuration::from_millis(1),
            pages_per_step: 8,
            pause_between_steps: StdDuration::from_millis(0),
            retry_backoff_ms: vec![1, 1],
            max_attempt_duration: Some(StdDuration::from_secs(5)),
            pin_source_snapshot: true,
        }),
    )?;
    assert_eq!(deferred.exit_code, 75);
    let output: Value = serde_json::from_str(&deferred.rendered_output)?;
    assert_eq!(output["state"], "deferred");
    assert_eq!(output["latest_surface_status"], "healthy");
    assert_eq!(output["staged_seeded_from_latest_surface"], false);
    assert_eq!(output["staged_progress_resumed"], false);
    assert_eq!(output["staged_row_count_before_attempt"].as_u64(), Some(0));
    assert!(
        output["staged_row_count_after_attempt"]
            .as_u64()
            .unwrap_or_default()
            > 0
    );

    let staged_manifest: RecentRawJournalSnapshotManifest = load_json(
        &fixture
            .snapshot_dir()
            .join(".discovery_recent_raw_staged.sqlite.archive-staged.json"),
    )?;
    assert_eq!(
        staged_manifest.source_db_path,
        fixture.journal_db_path.display().to_string()
    );
    assert_ne!(
        staged_manifest.source_db_path,
        foreign_latest_manifest.source_db_path
    );
    Ok(())
}

#[test]
fn scheduled_run_resumes_preserved_staged_progress_and_eventually_replaces_stale_latest(
) -> Result<()> {
    let fixture = make_fixture("recent-raw-snapshot-resume-incident")?;
    let initial_now = parse_ts("2026-03-23T12:00:00Z")?;
    seed_recent_raw_journal_range(
        &fixture.journal_store,
        initial_now - Duration::minutes(5),
        10,
        "sig-initial",
        32,
        initial_now,
    )?;
    run(Config {
        config_path: fixture.config_path.clone(),
        journal_db_path: Some(fixture.journal_db_path.clone()),
        output_path: None,
        scheduled: true,
        force: true,
        json: false,
        now: initial_now,
    })?;

    let latest_manifest_path = fixture.snapshot_dir().join("latest.json");
    let stale_latest_manifest: RecentRawJournalSnapshotManifest = load_json(&latest_manifest_path)?;
    seed_recent_raw_journal_range(
        &fixture.journal_store,
        initial_now + Duration::minutes(1),
        1_000,
        "sig-incident",
        4096,
        initial_now + Duration::minutes(1),
    )?;

    let mut forced_budget_exhaustions = 0usize;
    let _guard = install_resumable_snapshot_progress_hook(move |completed_batches, _rows| {
        if forced_budget_exhaustions < 2 && completed_batches >= 2 {
            forced_budget_exhaustions += 1;
            true
        } else {
            false
        }
    });
    let policy = Some(copybot_storage_core::SqliteSnapshotPolicy {
        busy_timeout: StdDuration::from_millis(1),
        pages_per_step: 8,
        pause_between_steps: StdDuration::from_millis(0),
        retry_backoff_ms: vec![1, 1],
        max_attempt_duration: Some(StdDuration::from_secs(5)),
        pin_source_snapshot: true,
    });

    let first_deferred = run_with_snapshot_policy_override(
        Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: None,
            scheduled: true,
            force: false,
            json: true,
            now: initial_now + Duration::minutes(15),
        },
        policy.clone(),
    )?;
    let first_output: Value = serde_json::from_str(&first_deferred.rendered_output)?;
    assert_eq!(first_output["state"], "deferred");
    assert_eq!(first_output["latest_surface_status"], "healthy");
    assert_eq!(
        first_output["latest_surface_action"],
        "deferred_due_to_attempt_budget"
    );
    assert_eq!(first_output["staged_progress_resumed"], false);
    assert_eq!(first_output["staged_seeded_from_latest_surface"], true);
    assert_eq!(first_output["staged_progress_preserved_for_retry"], true);
    assert_eq!(first_output["staged_progress_advanced"], true);
    assert_eq!(
        first_output["staged_row_count_before_attempt"].as_u64(),
        Some(stale_latest_manifest.row_count as u64)
    );
    let first_staged_rows = first_output["staged_row_count_after_attempt"]
        .as_u64()
        .context("first deferred staged row count must be present")?;
    assert!(first_staged_rows > stale_latest_manifest.row_count as u64);
    let telemetry_path = super::latest_attempt_telemetry_path(&fixture.snapshot_dir());
    let first_telemetry: Value = load_json(&telemetry_path)?;
    assert_eq!(first_telemetry["state"], "deferred");
    assert_eq!(
        first_telemetry["staged_row_count_after_attempt"].as_u64(),
        Some(first_staged_rows)
    );
    assert_eq!(staged_artifact_count(&fixture.snapshot_dir())?, 2);

    let second_deferred = run_with_snapshot_policy_override(
        Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: None,
            scheduled: true,
            force: false,
            json: true,
            now: initial_now + Duration::minutes(20),
        },
        policy.clone(),
    )?;
    let second_output: Value = serde_json::from_str(&second_deferred.rendered_output)?;
    assert_eq!(second_output["state"], "deferred");
    assert_eq!(second_output["staged_progress_resumed"], true);
    assert_eq!(second_output["staged_seeded_from_latest_surface"], false);
    assert_eq!(second_output["staged_progress_preserved_for_retry"], true);
    assert_eq!(second_output["staged_progress_advanced"], true);
    assert_eq!(
        second_output["staged_row_count_before_attempt"].as_u64(),
        Some(first_staged_rows)
    );
    let second_staged_rows = second_output["staged_row_count_after_attempt"]
        .as_u64()
        .context("second deferred staged row count must be present")?;
    assert!(second_staged_rows > first_staged_rows);
    let second_telemetry: Value = load_json(&telemetry_path)?;
    assert_eq!(second_telemetry["state"], "deferred");
    assert_eq!(
        second_telemetry["staged_row_count_before_attempt"].as_u64(),
        Some(first_staged_rows)
    );
    assert_eq!(
        second_telemetry["staged_row_count_after_attempt"].as_u64(),
        Some(second_staged_rows)
    );
    assert_ne!(first_telemetry, second_telemetry);
    assert_eq!(staged_artifact_count(&fixture.snapshot_dir())?, 2);

    let completed = run_with_snapshot_policy_override(
        Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: None,
            scheduled: true,
            force: false,
            json: true,
            now: initial_now + Duration::minutes(25),
        },
        policy,
    )?;
    assert_eq!(completed.exit_code, 0);
    let completed_output: Value = serde_json::from_str(&completed.rendered_output)?;
    assert_eq!(completed_output["state"], "written");
    assert_eq!(completed_output["archive_promoted"], true);
    assert_eq!(completed_output["staged_progress_resumed"], true);
    assert_eq!(staged_artifact_count(&fixture.snapshot_dir())?, 0);

    let promoted_latest_manifest: RecentRawJournalSnapshotManifest =
        load_json(&latest_manifest_path)?;
    assert!(
        promoted_latest_manifest.row_count > stale_latest_manifest.row_count,
        "resumed completion must replace the stale latest surface with newer bounded coverage"
    );
    assert_ne!(
        promoted_latest_manifest.covered_through_cursor,
        stale_latest_manifest.covered_through_cursor
    );
    assert_eq!(
        promoted_latest_manifest.row_count,
        fixture.journal_store.recent_raw_journal_state()?.row_count
    );
    assert!(
        archive_count(&fixture.snapshot_dir())? <= 2,
        "archive retention must remain bounded after resumed completion"
    );
    Ok(())
}

#[test]
fn scheduled_run_seeds_stage_from_latest_surface_before_bounded_catchup() -> Result<()> {
    let fixture = make_fixture("recent-raw-snapshot-resume-latest-ahead")?;
    let initial_now = parse_ts("2026-03-23T12:00:00Z")?;
    seed_recent_raw_journal_range(
        &fixture.journal_store,
        initial_now - Duration::minutes(30),
        10,
        "sig-resume-latest-ahead-initial",
        1_024,
        initial_now,
    )?;
    run(Config {
        config_path: fixture.config_path.clone(),
        journal_db_path: Some(fixture.journal_db_path.clone()),
        output_path: None,
        scheduled: true,
        force: true,
        json: false,
        now: initial_now,
    })?;

    let latest_manifest: RecentRawJournalSnapshotManifest =
        load_json(&fixture.snapshot_dir().join("latest.json"))?;
    seed_recent_raw_journal_range(
        &fixture.journal_store,
        initial_now + Duration::minutes(1),
        20_000,
        "sig-resume-latest-ahead-extra",
        4_096,
        initial_now + Duration::minutes(1),
    )?;

    let _guard =
        install_resumable_snapshot_progress_hook(|completed_batches, _staged_row_count| {
            completed_batches >= 1
        });
    let policy = Some(copybot_storage_core::SqliteSnapshotPolicy {
        busy_timeout: StdDuration::from_millis(1),
        pages_per_step: 8,
        pause_between_steps: StdDuration::from_millis(0),
        retry_backoff_ms: vec![1, 1],
        max_attempt_duration: Some(StdDuration::from_secs(5)),
        pin_source_snapshot: true,
    });

    let first_deferred = run_with_snapshot_policy_override(
        Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: None,
            scheduled: true,
            force: false,
            json: true,
            now: initial_now + Duration::minutes(15),
        },
        policy.clone(),
    )?;
    assert_eq!(first_deferred.exit_code, 75);
    let first_output: Value = serde_json::from_str(&first_deferred.rendered_output)?;
    assert_eq!(first_output["state"], "deferred");
    assert_eq!(first_output["staged_progress_resumed"], false);
    assert_eq!(first_output["staged_seeded_from_latest_surface"], true);
    assert_eq!(first_output["staged_progress_preserved_for_retry"], true);
    assert_eq!(
        first_output["staged_row_count_before_attempt"].as_u64(),
        Some(latest_manifest.row_count as u64)
    );
    let first_staged_rows = first_output["staged_row_count_after_attempt"]
        .as_u64()
        .context("first deferred staged row count must be present")?;
    assert!(first_staged_rows > latest_manifest.row_count as u64);
    let staged_manifest: RecentRawJournalSnapshotManifest = load_json(
        &fixture
            .snapshot_dir()
            .join(".discovery_recent_raw_staged.sqlite.archive-staged.json"),
    )?;
    assert_eq!(staged_manifest.created_at, latest_manifest.created_at);

    let second_deferred = run_with_snapshot_policy_override(
        Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: None,
            scheduled: true,
            force: false,
            json: true,
            now: initial_now + Duration::minutes(20),
        },
        policy,
    )?;
    assert_eq!(second_deferred.exit_code, 75);
    let second_output: Value = serde_json::from_str(&second_deferred.rendered_output)?;
    assert_eq!(second_output["state"], "deferred");
    assert_eq!(second_output["staged_progress_resumed"], true);
    assert_eq!(second_output["staged_seeded_from_latest_surface"], false);
    assert_eq!(second_output["staged_progress_preserved_for_retry"], true);
    assert_eq!(
        second_output["staged_row_count_before_attempt"].as_u64(),
        Some(first_staged_rows)
    );
    let second_staged_rows = second_output["staged_row_count_after_attempt"]
        .as_u64()
        .context("second deferred staged row count must be present")?;
    assert!(second_staged_rows > first_staged_rows);
    Ok(())
}

#[test]
fn scheduled_run_fails_explicitly_when_preserved_staged_cached_state_is_missing() -> Result<()> {
    let fixture = make_fixture("recent-raw-snapshot-missing-staged-cached-state")?;
    let initial_now = parse_ts("2026-03-23T12:00:00Z")?;
    seed_recent_raw_journal_range(
        &fixture.journal_store,
        initial_now - Duration::minutes(5),
        10,
        "sig-missing-staged-cached",
        512,
        initial_now,
    )?;
    let _guard =
        install_resumable_snapshot_progress_hook(|completed_batches, _staged_row_count| {
            completed_batches >= 1
        });
    let policy = Some(copybot_storage_core::SqliteSnapshotPolicy {
        busy_timeout: StdDuration::from_millis(1),
        pages_per_step: 8,
        pause_between_steps: StdDuration::from_millis(0),
        retry_backoff_ms: vec![1, 1],
        max_attempt_duration: Some(StdDuration::from_secs(5)),
        pin_source_snapshot: true,
    });

    let deferred = run_with_snapshot_policy_override(
        Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: None,
            scheduled: true,
            force: true,
            json: true,
            now: initial_now + Duration::minutes(15),
        },
        policy.clone(),
    )?;
    assert_eq!(deferred.exit_code, 75);
    let deferred_output: Value = serde_json::from_str(&deferred.rendered_output)?;
    assert_eq!(deferred_output["state"], "deferred");
    assert_eq!(deferred_output["staged_progress_preserved_for_retry"], true);

    let staged_snapshot_path = fixture
        .snapshot_dir()
        .join(".discovery_recent_raw_staged.sqlite.archive-staged");
    let staged_conn = rusqlite::Connection::open(&staged_snapshot_path)?;
    staged_conn.execute("DELETE FROM recent_raw_journal_state WHERE id = 1", [])?;

    let failed = run_with_snapshot_policy_override(
        Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: None,
            scheduled: true,
            force: false,
            json: true,
            now: initial_now + Duration::minutes(20),
        },
        policy,
    )?;
    assert_eq!(failed.exit_code, 1);
    let failed_output: Value = serde_json::from_str(&failed.rendered_output)?;
    assert_eq!(failed_output["state"], "hard_failure");
    assert!(
        failed_output["hard_failure_reason"]
            .as_str()
            .unwrap_or_default()
            .contains("cached recent raw journal state row id=1 is missing"),
        "unexpected hard-failure output: {}",
        serde_json::to_string_pretty(&failed_output)?
    );
    Ok(())
}
