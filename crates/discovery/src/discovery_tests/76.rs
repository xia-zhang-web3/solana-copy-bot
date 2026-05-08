#[test]
    fn recent_raw_replacement_convergence_stalled_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-replacement-convergence-stalled",
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
                "staged_progress_advanced": false,
                "staged_row_count_before_attempt": 2,
                "staged_row_count_after_attempt": 2,
                "staged_covered_through_cursor_before_attempt": {
                    "ts_utc": "2026-04-14T07:56:00Z",
                    "slot": parse_ts("2026-04-14T07:56:00Z")?.timestamp() as u64,
                    "signature": "sig-staged"
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
            RecentRawReplacementConvergenceReasonClass::RecentRawReplacementConvergenceStalledOnLatestAttempt
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_latest_attempt_row_count_delta,
            Some(0)
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_latest_attempt_advanced,
            Some(false)
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_estimated_attempts_to_current_source,
            None
        );
        Ok(())
    }

    #[test]
    fn recent_raw_replacement_convergence_reset_or_recreated_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-replacement-convergence-reset",
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
            1,
            parse_ts("2026-04-14T07:55:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;
        fixture.write_attempt_telemetry_json(
            RECENT_RAW_ATTEMPT_TELEMETRY_LATEST_FILE_NAME,
            serde_json::json!({
                "event": "discovery_recent_raw_snapshot",
                "state": "deferred",
                "staged_progress_resumed": false,
                "staged_seeded_from_latest_surface": true,
                "staged_progress_preserved_for_retry": true,
                "staged_progress_advanced": false,
                "staged_row_count_before_attempt": 0,
                "staged_row_count_after_attempt": 1,
                "created_at": "2026-04-14T08:05:00Z",
                "last_batch_completed_at": "2026-04-14T08:10:00Z"
            }),
        )?;

        let diagnostic = DiscoveryService::explain_recent_raw_replacement_convergence_read_only(
            &fixture.state_root,
        )?;
        assert_eq!(
            diagnostic.recent_raw_replacement_convergence_reason_class,
            RecentRawReplacementConvergenceReasonClass::RecentRawReplacementConvergenceResetOrRecreated
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_latest_attempt_advanced,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_candidate_complete_against_current_source,
            Some(false)
        );
        Ok(())
    }

    #[test]
    fn recent_raw_replacement_convergence_missing_attempt_telemetry_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-replacement-convergence-missing-telemetry",
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

        let diagnostic = DiscoveryService::explain_recent_raw_replacement_convergence_read_only(
            &fixture.state_root,
        )?;
        assert_eq!(
            diagnostic.recent_raw_replacement_convergence_reason_class,
            RecentRawReplacementConvergenceReasonClass::RecentRawReplacementConvergenceMissingOrUnparseableAttemptTelemetry
        );
        assert!(!diagnostic.recent_raw_replacement_convergence_observed);
        assert!(!diagnostic.recent_raw_replacement_attempt_telemetry_parseable);
        assert_eq!(
            diagnostic.recent_raw_replacement_latest_attempt_row_count_delta,
            None
        );
        Ok(())
    }

    #[test]
    fn recent_raw_replacement_convergence_unproven_missing_candidate_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-replacement-convergence-unproven",
            SourceStateSeed::Missing,
        )?;

        let diagnostic = DiscoveryService::explain_recent_raw_replacement_convergence_read_only(
            &fixture.state_root,
        )?;
        assert_eq!(
            diagnostic.recent_raw_replacement_convergence_reason_class,
            RecentRawReplacementConvergenceReasonClass::RecentRawReplacementConvergenceUnprovenDueToMissingEvidence
        );
        assert!(!diagnostic.recent_raw_replacement_candidate_exists);
        assert_eq!(diagnostic.recent_raw_source_row_count, None);
        Ok(())
    }

    #[test]
    fn recent_raw_replacement_convergence_explain_remains_read_only_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-replacement-convergence-read-only",
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
                "created_at": "2026-04-14T08:05:00Z",
                "last_batch_completed_at": "2026-04-14T08:10:00Z"
            }),
        )?;

        let before = fixture.capture_bytes()?;
        let _ = DiscoveryService::explain_recent_raw_replacement_convergence_read_only(
            &fixture.state_root,
        )?;
        let after = fixture.capture_bytes()?;
        assert_eq!(before, after);
        Ok(())
    }

    struct RecentRawPromotionFixture {
        _temp: tempfile::TempDir,
        state_root: PathBuf,
        recent_raw_dir: PathBuf,
        runtime_db_path: PathBuf,
    }

    #[derive(Debug, Clone, Copy)]
    enum SourceStateSeed {
        StagedCurrent,
        SourceAheadOfStaged,
        Missing,
    }
