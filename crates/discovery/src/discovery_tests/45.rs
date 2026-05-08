    #[test]
    fn recent_raw_replacement_attempt_telemetry_includes_explicit_fields_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-replacement-attempt-telemetry-fields",
            SourceStateSeed::Missing,
        )?;
        fixture.write_attempt_telemetry_json(
            "discovery_recent_raw_snapshot_attempt_latest.json",
            serde_json::json!({
                "event": "discovery_recent_raw_snapshot",
                "state": "deferred",
                "staged_progress_resumed": true,
                "staged_seeded_from_latest_surface": false,
                "staged_progress_preserved_for_retry": true,
                "staged_progress_advanced": true,
                "staged_row_count_before_attempt": 1,
                "staged_row_count_after_attempt": 2,
                "staged_covered_through_cursor_before_attempt": {
                    "ts_utc": "2026-04-14T07:56:00Z",
                    "slot": parse_ts("2026-04-14T07:56:00Z")?.timestamp() as u64,
                    "signature": "sig-source-a"
                },
                "staged_covered_through_cursor_after_attempt": {
                    "ts_utc": "2026-04-14T07:57:00Z",
                    "slot": parse_ts("2026-04-14T07:57:00Z")?.timestamp() as u64,
                    "signature": "sig-source-b"
                },
                "created_at": "2026-04-14T08:05:00Z",
                "last_batch_completed_at": "2026-04-14T08:10:00Z"
            }),
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_replacement_attempt_telemetry_read_only(
                &fixture.state_root,
            )?;
        assert!(diagnostic.recent_raw_replacement_attempt_telemetry_probe_bounded);
        assert_eq!(
            diagnostic.recent_raw_replacement_attempt_telemetry_probe_mode,
            RECENT_RAW_ATTEMPT_TELEMETRY_PROBE_MODE_EXPLICIT_PATHS
        );
        assert!(!diagnostic.recent_raw_replacement_attempt_telemetry_deep_scan_used);
        assert!(diagnostic
            .recent_raw_replacement_attempt_telemetry_explicit_paths_checked
            .iter()
            .any(|path| path.ends_with(
                "discovery_restore/recent_raw/discovery_recent_raw_snapshot_attempt_latest.json"
            )));
        assert!(diagnostic
            .recent_raw_replacement_attempt_telemetry_scanned_dirs
            .is_empty());
        assert_eq!(
            diagnostic.recent_raw_replacement_attempt_telemetry_scan_file_limit,
            0
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_attempt_telemetry_artifact_count,
            1
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_attempt_telemetry_parseable_count,
            1
        );
        assert!(diagnostic
            .recent_raw_replacement_attempt_telemetry_latest_path
            .is_some());
        assert!(diagnostic
            .recent_raw_replacement_attempt_telemetry_latest_timestamp
            .is_some());
        assert!(diagnostic
            .recent_raw_replacement_attempt_telemetry_last_covered_through_before
            .is_some());
        assert!(diagnostic
            .recent_raw_replacement_attempt_telemetry_last_covered_through_after
            .is_some());
        assert!(!diagnostic
            .recent_raw_replacement_attempt_telemetry_explanation
            .is_empty());
        Ok(())
    }

    #[test]
    fn recent_raw_replacement_attempt_telemetry_explain_remains_read_only_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-replacement-attempt-telemetry-read-only",
            SourceStateSeed::Missing,
        )?;
        let telemetry_path = fixture.write_attempt_telemetry_json(
            "discovery_recent_raw_snapshot_attempt_latest.json",
            serde_json::json!({
                "event": "discovery_recent_raw_snapshot",
                "state": "deferred",
                "staged_progress_resumed": true,
                "staged_seeded_from_latest_surface": false,
                "staged_progress_preserved_for_retry": true,
                "staged_progress_advanced": true,
                "staged_row_count_before_attempt": 1,
                "staged_row_count_after_attempt": 2,
                "created_at": "2026-04-14T08:05:00Z",
                "last_batch_completed_at": "2026-04-14T08:10:00Z"
            }),
        )?;

        let before = fs::read(&telemetry_path)?;
        let _ = DiscoveryService::explain_recent_raw_replacement_attempt_telemetry_read_only(
            &fixture.state_root,
        )?;
        let after = fs::read(&telemetry_path)?;
        assert_eq!(before, after);
        Ok(())
    }

    #[test]
    fn recent_raw_replacement_convergence_ready_to_promote_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-replacement-convergence-ready",
            SourceStateSeed::StagedCurrent,
        )?;
        fixture.write_promoted_surface(
            "latest.sqlite",
            1,
            parse_ts("2026-04-14T07:55:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;
        fixture.write_staged_surface(
            2,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-staged",
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;
        fixture.write_attempt_telemetry_json(
            RECENT_RAW_ATTEMPT_TELEMETRY_LATEST_FILE_NAME,
            serde_json::json!({
                "event": "discovery_recent_raw_snapshot",
                "state": "written",
                "staged_progress_resumed": true,
                "staged_seeded_from_latest_surface": false,
                "staged_progress_preserved_for_retry": false,
                "staged_progress_advanced": true,
                "staged_row_count_before_attempt": 1,
                "staged_row_count_after_attempt": 2,
                "staged_covered_through_cursor_before_attempt": {
                    "ts_utc": "2026-04-14T07:55:00Z",
                    "slot": parse_ts("2026-04-14T07:55:00Z")?.timestamp() as u64,
                    "signature": "sig-promoted"
                },
                "staged_covered_through_cursor_after_attempt": {
                    "ts_utc": "2026-04-14T07:56:00Z",
                    "slot": parse_ts("2026-04-14T07:56:00Z")?.timestamp() as u64,
                    "signature": "sig-staged"
                },
                "created_at": "2026-04-14T08:05:00Z",
                "last_batch_completed_at": "2026-04-14T08:10:00Z"
            }),
        )?;

        let diagnostic = DiscoveryService::explain_recent_raw_replacement_convergence_read_only(
            &fixture.state_root,
        )?;
        assert_eq!(
            diagnostic.recent_raw_replacement_convergence_reason_class,
            RecentRawReplacementConvergenceReasonClass::RecentRawReplacementConvergenceReadyToPromote
        );
        assert!(diagnostic.recent_raw_replacement_convergence_observed);
        assert_eq!(
            diagnostic.recent_raw_replacement_candidate_complete_against_current_source,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_candidate_promotable_now,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_rows_remaining_to_current_source,
            Some(0)
        );
        Ok(())
    }

    #[test]
    fn recent_raw_replacement_convergence_advancing_but_incomplete_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-replacement-convergence-advancing",
            SourceStateSeed::SourceAheadOfStaged,
        )?;
        fixture.write_promoted_surface(
            "latest.sqlite",
            1,
            parse_ts("2026-04-14T07:55:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;
        fixture.write_staged_surface(
            2,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-staged",
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;
        fixture.write_attempt_telemetry_json(
            RECENT_RAW_ATTEMPT_TELEMETRY_LATEST_FILE_NAME,
            serde_json::json!({
                "event": "discovery_recent_raw_snapshot",
                "state": "deferred",
                "staged_progress_resumed": true,
                "staged_seeded_from_latest_surface": false,
                "staged_progress_preserved_for_retry": true,
                "staged_progress_advanced": true,
                "staged_row_count_before_attempt": 1,
                "staged_row_count_after_attempt": 2,
                "staged_covered_through_cursor_before_attempt": {
                    "ts_utc": "2026-04-14T07:55:00Z",
                    "slot": parse_ts("2026-04-14T07:55:00Z")?.timestamp() as u64,
                    "signature": "sig-promoted"
                },
                "staged_covered_through_cursor_after_attempt": {
                    "ts_utc": "2026-04-14T07:56:00Z",
                    "slot": parse_ts("2026-04-14T07:56:00Z")?.timestamp() as u64,
                    "signature": "sig-staged"
                },
                "created_at": "2026-04-14T08:05:00Z",
                "last_batch_completed_at": "2026-04-14T08:10:00Z"
            }),
        )?;

        let diagnostic = DiscoveryService::explain_recent_raw_replacement_convergence_read_only(
            &fixture.state_root,
        )?;
        assert_eq!(
            diagnostic.recent_raw_replacement_convergence_reason_class,
            RecentRawReplacementConvergenceReasonClass::RecentRawReplacementConvergenceAdvancingButIncomplete
        );
        assert!(diagnostic.recent_raw_replacement_convergence_observed);
        assert_eq!(diagnostic.recent_raw_replacement_candidate_exists, true);
        assert_eq!(
            diagnostic.recent_raw_replacement_candidate_row_count,
            Some(2)
        );
        assert_eq!(diagnostic.recent_raw_source_row_count, Some(3));
        assert_eq!(
            diagnostic.recent_raw_replacement_rows_remaining_to_current_source,
            Some(1)
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_latest_attempt_row_count_delta,
            Some(1)
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_estimated_attempts_to_current_source,
            Some(1)
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_latest_attempt_advanced,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_latest_attempt_resumed,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_latest_attempt_preserved_for_retry,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_candidate_complete_against_current_source,
            Some(false)
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_candidate_promotable_now,
            Some(false)
        );
        assert!(diagnostic.recent_raw_replacement_attempt_telemetry_parseable);
        assert!(diagnostic.recent_raw_replacement_attempt_telemetry_probe_bounded);
        assert!(diagnostic
            .recent_raw_replacement_attempt_telemetry_path
            .ends_with(RECENT_RAW_ATTEMPT_TELEMETRY_LATEST_FILE_NAME));
        Ok(())
    }
