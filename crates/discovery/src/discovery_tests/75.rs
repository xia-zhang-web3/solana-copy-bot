#[test]
    fn recent_raw_replacement_attempt_telemetry_stalled_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-replacement-attempt-telemetry-stalled",
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
                "staged_progress_advanced": false,
                "staged_row_count_before_attempt": 2,
                "staged_row_count_after_attempt": 2,
                "staged_covered_through_cursor_before_attempt": {
                    "ts_utc": "2026-04-14T07:57:00Z",
                    "slot": parse_ts("2026-04-14T07:57:00Z")?.timestamp() as u64,
                    "signature": "sig-source-b"
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
        assert_eq!(
            diagnostic.recent_raw_replacement_attempt_telemetry_reason_class,
            RecentRawReplacementAttemptTelemetryReasonClass::RecentRawReplacementAttemptTelemetryStalled
        );
        assert!(diagnostic.recent_raw_replacement_attempt_telemetry_observed);
        assert!(!diagnostic.recent_raw_replacement_attempt_telemetry_proves_advancing);
        assert!(diagnostic.recent_raw_replacement_attempt_telemetry_proves_stalled);
        Ok(())
    }

    #[test]
    fn recent_raw_replacement_attempt_telemetry_reset_or_recreated_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-replacement-attempt-telemetry-reset",
            SourceStateSeed::Missing,
        )?;
        fixture.write_attempt_telemetry_json(
            "discovery_recent_raw_snapshot_attempt_latest.json",
            serde_json::json!({
                "event": "discovery_recent_raw_snapshot",
                "state": "deferred",
                "staged_progress_resumed": false,
                "staged_seeded_from_latest_surface": true,
                "staged_progress_preserved_for_retry": true,
                "staged_progress_advanced": false,
                "staged_row_count_before_attempt": 0,
                "staged_row_count_after_attempt": 0,
                "created_at": "2026-04-14T08:05:00Z",
                "last_batch_completed_at": "2026-04-14T08:10:00Z"
            }),
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_replacement_attempt_telemetry_read_only(
                &fixture.state_root,
            )?;
        assert_eq!(
            diagnostic.recent_raw_replacement_attempt_telemetry_reason_class,
            RecentRawReplacementAttemptTelemetryReasonClass::RecentRawReplacementAttemptTelemetryResetOrRecreated
        );
        assert!(diagnostic.recent_raw_replacement_attempt_telemetry_observed);
        assert!(diagnostic.recent_raw_replacement_attempt_telemetry_proves_reset_or_recreated);
        Ok(())
    }

    #[test]
    fn recent_raw_replacement_attempt_telemetry_missing_or_unparseable_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-replacement-attempt-telemetry-missing",
            SourceStateSeed::Missing,
        )?;
        fs::write(
            fixture
                .recent_raw_dir
                .join("discovery_recent_raw_snapshot_attempt_bad.json"),
            b"{not-json",
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_replacement_attempt_telemetry_read_only(
                &fixture.state_root,
            )?;
        assert_eq!(
            diagnostic.recent_raw_replacement_attempt_telemetry_reason_class,
            RecentRawReplacementAttemptTelemetryReasonClass::RecentRawReplacementAttemptTelemetryMissingOrUnparseable
        );
        assert!(!diagnostic.recent_raw_replacement_attempt_telemetry_observed);
        assert_eq!(
            diagnostic.recent_raw_replacement_attempt_telemetry_artifact_count,
            0
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_attempt_telemetry_parseable_count,
            0
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_attempt_telemetry_probe_mode,
            RECENT_RAW_ATTEMPT_TELEMETRY_PROBE_MODE_EXPLICIT_PATHS
        );
        assert!(diagnostic
            .recent_raw_replacement_attempt_telemetry_scanned_dirs
            .is_empty());
        assert!(!diagnostic.recent_raw_replacement_attempt_telemetry_proves_advancing);
        Ok(())
    }

    #[test]
    fn recent_raw_replacement_attempt_telemetry_default_does_not_scan_artifact_dirs_stage1(
    ) -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-replacement-attempt-telemetry-no-default-scan",
            SourceStateSeed::Missing,
        )?;
        let artifact_dir = fixture.state_root.join("discovery_restore/artifacts");
        fs::create_dir_all(&artifact_dir)?;
        runtime_artifacts::write_json_atomic(
            &artifact_dir.join("discovery_recent_raw_snapshot_attempt_deep_only.json"),
            &serde_json::json!({
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

        let diagnostic =
            DiscoveryService::explain_recent_raw_replacement_attempt_telemetry_read_only(
                &fixture.state_root,
            )?;
        assert_eq!(
            diagnostic.recent_raw_replacement_attempt_telemetry_reason_class,
            RecentRawReplacementAttemptTelemetryReasonClass::RecentRawReplacementAttemptTelemetryMissingOrUnparseable
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_attempt_telemetry_probe_mode,
            RECENT_RAW_ATTEMPT_TELEMETRY_PROBE_MODE_EXPLICIT_PATHS
        );
        assert!(diagnostic.recent_raw_replacement_attempt_telemetry_probe_bounded);
        assert!(!diagnostic.recent_raw_replacement_attempt_telemetry_deep_scan_used);
        assert!(diagnostic
            .recent_raw_replacement_attempt_telemetry_scanned_dirs
            .is_empty());
        assert_eq!(
            diagnostic.recent_raw_replacement_attempt_telemetry_artifact_count,
            0
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_attempt_telemetry_parseable_count,
            0
        );
        Ok(())
    }

    #[test]
    fn recent_raw_replacement_attempt_telemetry_deep_scan_is_opt_in_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-replacement-attempt-telemetry-deep-opt-in",
            SourceStateSeed::Missing,
        )?;
        let artifact_dir = fixture.state_root.join("discovery_restore/artifacts");
        fs::create_dir_all(&artifact_dir)?;
        runtime_artifacts::write_json_atomic(
            &artifact_dir.join("discovery_recent_raw_snapshot_attempt_deep_only.json"),
            &serde_json::json!({
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
            DiscoveryService::explain_recent_raw_replacement_attempt_telemetry_with_deep_scan_read_only(
                &fixture.state_root,
                true,
            )?;
        assert_eq!(
            diagnostic.recent_raw_replacement_attempt_telemetry_reason_class,
            RecentRawReplacementAttemptTelemetryReasonClass::RecentRawReplacementAttemptTelemetryAdvancingButIncomplete
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_attempt_telemetry_probe_mode,
            RECENT_RAW_ATTEMPT_TELEMETRY_PROBE_MODE_DEEP_SCAN
        );
        assert!(!diagnostic.recent_raw_replacement_attempt_telemetry_probe_bounded);
        assert!(diagnostic.recent_raw_replacement_attempt_telemetry_deep_scan_used);
        assert!(!diagnostic
            .recent_raw_replacement_attempt_telemetry_scanned_dirs
            .is_empty());
        assert_eq!(
            diagnostic.recent_raw_replacement_attempt_telemetry_scan_file_limit,
            RECENT_RAW_ATTEMPT_TELEMETRY_SCAN_FILE_LIMIT
        );
        assert!(diagnostic.recent_raw_replacement_attempt_telemetry_proves_advancing);
        Ok(())
    }
