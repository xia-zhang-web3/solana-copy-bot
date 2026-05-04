#[test]
fn parse_args_from_accepts_scheduled_json_and_now() {
    let parsed = parse_args_from(vec![
        "--config".to_string(),
        "configs/live.toml".to_string(),
        "--journal-db-path".to_string(),
        "state/discovery_recent_raw.db".to_string(),
        "--scheduled".to_string(),
        "--json".to_string(),
        "--now".to_string(),
        "2026-03-23T12:00:00Z".to_string(),
    ])
    .expect("parse should succeed")
    .expect("config should be present");
    assert_eq!(parsed.config_path, PathBuf::from("configs/live.toml"));
    assert_eq!(
        parsed.journal_db_path,
        Some(PathBuf::from("state/discovery_recent_raw.db"))
    );
    assert!(parsed.scheduled);
    assert!(parsed.json);
}

#[test]
fn snapshot_state_exit_codes_keep_transient_contention_distinct_from_hard_failure() {
    assert_eq!(super::SnapshotState::Deferred.exit_code(), 75);
    assert_eq!(super::SnapshotState::RetryableBusy.exit_code(), 75);
    assert_eq!(super::SnapshotState::HardFailure.exit_code(), 1);
}

#[test]
fn staged_write_interrupted_error_is_classified_as_budget_exhaustion() {
    let interrupted = anyhow::anyhow!(
        "failed to run recent raw journal bulk batch write: interrupted by progress handler"
    );
    assert!(super::recent_raw_staged_write_error_is_budget_exhaustion(
        &interrupted,
        false
    ));

    let generic_after_deadline =
        anyhow::anyhow!("failed to run recent raw journal bulk batch write");
    assert!(super::recent_raw_staged_write_error_is_budget_exhaustion(
        &generic_after_deadline,
        true
    ));
    assert!(!super::recent_raw_staged_write_error_is_budget_exhaustion(
        &generic_after_deadline,
        false
    ));

    let real_error =
        anyhow::anyhow!("failed to run recent raw journal bulk batch write: disk I/O error");
    assert!(!super::recent_raw_staged_write_error_is_budget_exhaustion(
        &real_error,
        true
    ));

    let schema_error =
        anyhow::anyhow!("failed to run recent raw journal bulk batch write: no such table");
    assert!(!super::recent_raw_staged_write_error_is_budget_exhaustion(
        &schema_error,
        true
    ));
}

#[test]
fn adaptive_snapshot_policy_scales_for_large_sources() {
    let policy = adaptive_snapshot_policy(&SnapshotSourceStats {
        source_db_bytes: 1_200 * 1024 * 1024,
        source_wal_bytes: 459 * 1024 * 1024,
        source_page_size_bytes: 4096,
        source_page_count: 310_000,
    });
    assert!(
        policy.pages_per_step > 16,
        "large sources must use larger backup steps than the tiny default"
    );
    assert_eq!(
        policy.pause_between_steps,
        StdDuration::from_millis(super::SNAPSHOT_LARGE_PAUSE_BETWEEN_STEPS_MS)
    );
    assert_eq!(
        policy.max_attempt_duration,
        Some(StdDuration::from_millis(
            super::SNAPSHOT_HUGE_MAX_ATTEMPT_DURATION_MS,
        ))
    );
}

#[test]
fn snapshot_resume_batch_size_uses_explicit_staged_write_cap() {
    let small_policy = copybot_storage_core::SqliteSnapshotPolicy {
        pages_per_step: 8,
        ..copybot_storage_core::SqliteSnapshotPolicy::default()
    };
    assert_eq!(
        super::snapshot_resume_batch_size(&small_policy),
        8 * super::SNAPSHOT_RESUME_ROW_BATCH_MULTIPLIER
    );

    let live_capped_policy = copybot_storage_core::SqliteSnapshotPolicy {
        pages_per_step: super::SNAPSHOT_MAX_PAGES_PER_STEP as i32,
        ..copybot_storage_core::SqliteSnapshotPolicy::default()
    };
    assert_eq!(
        super::snapshot_resume_batch_size(&live_capped_policy),
        super::SNAPSHOT_RESUME_ROW_BATCH_MAX_ROWS
    );

    let oversized_policy = copybot_storage_core::SqliteSnapshotPolicy {
        pages_per_step: 16_384,
        ..copybot_storage_core::SqliteSnapshotPolicy::default()
    };
    assert_eq!(
        super::snapshot_resume_batch_size(&oversized_policy),
        super::SNAPSHOT_RESUME_ROW_BATCH_MAX_ROWS
    );
}

#[test]
fn source_window_outran_staged_progress_returns_true_when_source_cursor_is_newer() -> Result<()> {
    let source_state = RecentRawJournalStateRow {
        covered_since: Some(parse_ts("2026-03-27T11:43:56Z")?),
        covered_through_cursor: Some(super::DiscoveryRuntimeCursor {
            ts_utc: parse_ts("2026-03-29T12:44:48Z")?,
            slot: 33_206_523,
            signature: "sig-source-newer".to_string(),
        }),
        row_count: 33_206_523,
        ..RecentRawJournalStateRow::default()
    };
    let staged_manifest = RecentRawJournalSnapshotManifest {
        created_at: parse_ts("2026-03-27T12:00:00Z")?,
        source_db_path: "/tmp/source.db".to_string(),
        snapshot_path: "/tmp/staged.sqlite".to_string(),
        row_count: 22_938_251,
        covered_since: Some(parse_ts("2026-03-27T11:43:56Z")?),
        covered_through_cursor: Some(super::DiscoveryRuntimeCursor {
            ts_utc: parse_ts("2026-03-27T11:43:56Z")?,
            slot: 22_938_251,
            signature: "sig-staged-older".to_string(),
        }),
        last_batch_completed_at: Some(parse_ts("2026-03-27T11:45:00Z")?),
        updated_at: Some(parse_ts("2026-03-27T11:45:00Z")?),
        snapshot_bytes: 1,
    };

    assert!(source_window_outran_staged_progress(
        &source_state,
        &staged_manifest
    ));
    Ok(())
}

#[test]
fn scheduled_run_returns_bounded_deferred_outcome_when_attempt_budget_is_exhausted() -> Result<()> {
    let fixture = make_fixture("recent-raw-snapshot-budget")?;
    let now = parse_ts("2026-03-23T12:00:00Z")?;
    seed_recent_raw_journal_range(
        &fixture.journal_store,
        now - Duration::minutes(30),
        10,
        "sig-budget",
        1_024,
        now,
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
            force: true,
            json: true,
            now,
        },
        Some(copybot_storage_core::SqliteSnapshotPolicy {
            busy_timeout: StdDuration::from_millis(1),
            pages_per_step: 8,
            pause_between_steps: StdDuration::from_millis(0),
            retry_backoff_ms: vec![1, 1],
            // Keep a real bounded deadline, but make exhaustion deterministic via the
            // resumable-progress hook after one committed staged batch. A 1ms wall-clock
            // budget can expire before the first staged insert under full-bin runs, which
            // turns this into a flaky zero-progress timeout instead of the intended
            // preserved-progress deferred contract.
            max_attempt_duration: Some(StdDuration::from_secs(5)),
            pin_source_snapshot: true,
        }),
    )?;
    assert_eq!(deferred.exit_code, 75);
    let output: Value = serde_json::from_str(&deferred.rendered_output)?;
    assert_eq!(output["state"], "deferred");
    assert_eq!(
        output["terminal_reason"],
        "staged_write_attempt_duration_budget_exhausted"
    );
    assert_eq!(
        output["latest_surface_action"],
        "unchanged_due_to_attempt_budget"
    );
    assert_eq!(output["hard_failure_reason"], Value::Null);
    assert_eq!(output["archive_promoted"], false);
    assert_eq!(output["snapshot_pages_per_step"], 8);
    assert_eq!(output["staged_completed_batches"].as_u64(), Some(1));
    assert!(
        output["staged_source_rows_loaded"]
            .as_u64()
            .unwrap_or_default()
            > 0
    );
    assert!(output["staged_rows_processed"].as_u64().unwrap_or_default() > 0);
    assert!(output["staged_rows_inserted"].as_u64().unwrap_or_default() > 0);
    assert_eq!(
        output["staged_write_batch_count"],
        output["staged_completed_batches"]
    );
    assert_eq!(
        output["staged_write_batch_rows"].as_u64(),
        Some(
            (8 * super::SNAPSHOT_RESUME_ROW_BATCH_MULTIPLIER)
                .min(super::SNAPSHOT_RESUME_ROW_BATCH_MAX_ROWS) as u64
        )
    );
    for field in [
        "staged_write_sqlite_variable_limit",
        "staged_write_statement_params_per_row",
        "staged_write_statement_chunk_row_cap",
        "staged_write_effective_statement_chunk_rows",
        "staged_write_statement_count",
        "staged_write_rows_processed",
        "staged_write_rows_inserted",
        "staged_write_value_build_duration_ms",
        "staged_write_prepare_duration_ms",
        "staged_write_execute_duration_ms",
        "staged_write_state_refresh_duration_ms",
        "staged_write_state_upsert_duration_ms",
        "staged_write_transaction_duration_ms",
        "staged_write_deadline_exhausted_before_statement",
        "staged_write_deadline_exhausted_during_execute",
    ] {
        assert!(output.get(field).is_some(), "missing output field {field}");
    }
    assert_eq!(output["staged_write_statement_params_per_row"], 13);
    assert_eq!(output["staged_write_statement_chunk_row_cap"], 512);
    assert!(
        output["staged_write_sqlite_variable_limit"]
            .as_u64()
            .unwrap_or_default()
            >= 999
    );
    assert!(
        output["staged_write_effective_statement_chunk_rows"]
            .as_u64()
            .unwrap_or_default()
            > 0
    );
    assert!(
        output["staged_write_statement_count"]
            .as_u64()
            .unwrap_or_default()
            > 0
    );
    assert_eq!(
        output["staged_write_rows_processed"],
        output["staged_rows_processed"]
    );
    assert_eq!(
        output["staged_write_rows_inserted"],
        output["staged_rows_inserted"]
    );
    assert!(
        output["staged_write_rows_per_second"].as_f64().is_some(),
        "deferred progress must expose write-throughput telemetry"
    );
    assert_eq!(output["staged_terminal_phase"], "staged_write");
    assert_eq!(output["staged_progress_preserved_for_retry"], true);
    assert_eq!(output["staged_progress_advanced"], true);
    assert!(
        output["staged_row_count_after_attempt"]
            .as_u64()
            .unwrap_or_default()
            > output["staged_row_count_before_attempt"]
                .as_u64()
                .unwrap_or_default(),
        "bounded deferred outcome must expose preserved staged forward progress"
    );
    assert!(
        !fixture.snapshot_dir().join("latest.sqlite").exists(),
        "partial staged progress must not be published as latest"
    );
    assert!(
        !fixture.snapshot_dir().join("latest.json").exists(),
        "partial staged progress must not publish latest metadata"
    );
    let telemetry_path = super::latest_attempt_telemetry_path(&fixture.snapshot_dir());
    let telemetry: Value = load_json(&telemetry_path)?;
    assert_eq!(telemetry["event"], "discovery_recent_raw_snapshot");
    assert_eq!(telemetry["state"], "deferred");
    assert_eq!(telemetry["hard_failure_reason"], Value::Null);
    assert_eq!(telemetry["archive_promoted"], false);
    assert_eq!(telemetry["staged_progress_resumed"], false);
    assert_eq!(telemetry["staged_seeded_from_latest_surface"], false);
    assert_eq!(telemetry["staged_progress_preserved_for_retry"], true);
    assert_eq!(telemetry["staged_progress_advanced"], true);
    assert_eq!(
        telemetry["staged_row_count_before_attempt"],
        output["staged_row_count_before_attempt"]
    );
    assert_eq!(
        telemetry["staged_row_count_after_attempt"],
        output["staged_row_count_after_attempt"]
    );
    assert_eq!(
        telemetry["staged_covered_through_cursor_before_attempt"],
        output["staged_covered_through_cursor_before_attempt"]
    );
    assert_eq!(
        telemetry["staged_covered_through_cursor_after_attempt"],
        output["staged_covered_through_cursor_after_attempt"]
    );
    assert_eq!(
        telemetry["staged_write_rows_per_second"],
        output["staged_write_rows_per_second"]
    );
    assert_eq!(
        telemetry["staged_write_batch_count"],
        output["staged_write_batch_count"]
    );
    assert_eq!(
        telemetry["staged_write_batch_rows"],
        output["staged_write_batch_rows"]
    );
    for field in [
        "staged_write_sqlite_variable_limit",
        "staged_write_statement_params_per_row",
        "staged_write_statement_chunk_row_cap",
        "staged_write_effective_statement_chunk_rows",
        "staged_write_statement_count",
        "staged_write_rows_processed",
        "staged_write_rows_inserted",
        "staged_write_value_build_duration_ms",
        "staged_write_prepare_duration_ms",
        "staged_write_execute_duration_ms",
        "staged_write_state_refresh_duration_ms",
        "staged_write_state_upsert_duration_ms",
        "staged_write_transaction_duration_ms",
        "staged_write_deadline_exhausted_before_statement",
        "staged_write_deadline_exhausted_during_execute",
    ] {
        assert_eq!(
            telemetry[field], output[field],
            "attempt telemetry must mirror output field {field}"
        );
    }
    assert!(telemetry.get("created_at").is_some());
    assert!(telemetry.get("last_batch_completed_at").is_some());

    assert_eq!(
        telemetry_path,
        fixture
            .snapshot_dir()
            .join("discovery_recent_raw_snapshot_attempt_latest.json")
    );
    assert_eq!(staged_artifact_count(&fixture.snapshot_dir())?, 2);
    Ok(())
}

#[test]
fn scheduled_run_defers_generic_bulk_write_wrapper_after_committed_staged_batch() -> Result<()> {
    let fixture = make_fixture("recent-raw-snapshot-generic-bulk-timeout")?;
    let initial_now = parse_ts("2026-03-23T12:00:00Z")?;
    seed_recent_raw_journal_range(
        &fixture.journal_store,
        initial_now - Duration::minutes(5),
        10,
        "sig-generic-bulk-initial",
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
    let latest_before: RecentRawJournalSnapshotManifest = load_json(&latest_manifest_path)?;
    seed_recent_raw_journal_range(
        &fixture.journal_store,
        initial_now + Duration::minutes(1),
        2_048,
        "sig-generic-bulk-extra",
        2_048,
        initial_now + Duration::minutes(1),
    )?;

    let _guard = install_staged_write_failure_hook(|completed_batches| {
        if completed_batches >= 1 {
            Some(StagedWriteHookFailure {
                error: anyhow::anyhow!("failed to run recent raw journal bulk batch write"),
                force_budget_context: true,
            })
        } else {
            None
        }
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
    assert_eq!(
        output["terminal_reason"],
        "staged_write_attempt_duration_budget_exhausted"
    );
    assert_eq!(
        output["latest_surface_action"],
        "deferred_due_to_attempt_budget"
    );
    assert_eq!(output["hard_failure_reason"], Value::Null);
    assert_eq!(output["archive_promoted"], false);
    assert_eq!(output["staged_completed_batches"].as_u64(), Some(1));
    assert_eq!(output["staged_progress_preserved_for_retry"], true);
    assert_eq!(output["staged_progress_advanced"], true);
    assert_eq!(
        output["staged_row_count_before_attempt"].as_u64(),
        Some(latest_before.row_count as u64)
    );
    let staged_after = output["staged_row_count_after_attempt"]
        .as_u64()
        .context("staged row_count_after_attempt must be populated")?;
    assert!(staged_after > latest_before.row_count as u64);

    let latest_after: RecentRawJournalSnapshotManifest = load_json(&latest_manifest_path)?;
    assert_eq!(latest_after.row_count, latest_before.row_count);
    assert_eq!(
        latest_after.covered_through_cursor,
        latest_before.covered_through_cursor
    );
    assert_eq!(staged_artifact_count(&fixture.snapshot_dir())?, 2);

    let telemetry_path = super::latest_attempt_telemetry_path(&fixture.snapshot_dir());
    let telemetry: Value = load_json(&telemetry_path)?;
    assert_eq!(telemetry["state"], "deferred");
    assert_eq!(telemetry["hard_failure_reason"], Value::Null);
    assert_eq!(telemetry["archive_promoted"], false);
    assert_eq!(telemetry["staged_progress_preserved_for_retry"], true);
    assert_eq!(
        telemetry["staged_row_count_after_attempt"].as_u64(),
        Some(staged_after)
    );
    Ok(())
}

#[test]
fn scheduled_attempt_telemetry_write_failure_does_not_change_snapshot_result() -> Result<()> {
    let fixture = make_fixture("recent-raw-snapshot-telemetry-write-failure")?;
    let now = parse_ts("2026-03-23T12:00:00Z")?;
    seed_recent_raw_journal_range(
        &fixture.journal_store,
        now - Duration::minutes(30),
        10,
        "sig-telemetry-failure",
        512,
        now,
    )?;
    let telemetry_path = super::latest_attempt_telemetry_path(&fixture.snapshot_dir());
    std::fs::create_dir_all(&telemetry_path)?;
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
            force: true,
            json: true,
            now,
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
    assert_eq!(
        output["latest_surface_action"],
        "unchanged_due_to_attempt_budget"
    );
    assert_eq!(output["staged_progress_preserved_for_retry"], true);
    assert!(telemetry_path.is_dir());
    Ok(())
}
