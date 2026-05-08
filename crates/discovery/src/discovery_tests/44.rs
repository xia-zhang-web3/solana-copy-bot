    #[test]
    fn recent_raw_replacement_artifact_history_contract_fixed_path_overwrite_by_design_stage1(
    ) -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-replacement-artifact-history-contract-fixed",
            SourceStateSeed::Missing,
        )?;
        fixture.write_selected_staged_surface_sqlite_with_source_path_and_created_at(
            &fixture.runtime_db_path,
            &[swap(
                "wallet-raw",
                "sig-source-a",
                parse_ts("2026-04-14T07:56:00Z")?,
                SOL_MINT,
                "TokenRaw111111111111111111111111111111111",
                1.0,
                10.0,
            )],
            parse_ts("2026-04-14T08:10:00Z")?,
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_replacement_artifact_history_contract_read_only(
                &fixture.state_root,
            )?;
        assert_eq!(
            diagnostic.recent_raw_replacement_artifact_history_reason_class,
            RecentRawReplacementArtifactHistoryReasonClass::RecentRawReplacementArtifactHistoryFixedPathOverwriteByDesign
        );
        assert!(diagnostic.recent_raw_replacement_artifact_history_observed);
        assert!(diagnostic.recent_raw_replacement_fixed_path_overwrite_contract);
        assert!(diagnostic.recent_raw_replacement_current_fixed_candidate_exists);
        assert!(diagnostic.recent_raw_replacement_current_fixed_candidate_manifest_parseable);
        assert!(!diagnostic.recent_raw_replacement_previous_artifact_archive_path_present);
        assert_eq!(
            diagnostic.recent_raw_replacement_previous_artifact_archive_candidate_count,
            0
        );
        assert!(
            diagnostic
                .recent_raw_replacement_previous_artifact_history_expected_under_current_contract
        );
        assert!(
            !diagnostic
                .recent_raw_replacement_previous_artifact_history_missing_due_to_unproven_evidence
        );
        Ok(())
    }

    #[test]
    fn recent_raw_replacement_artifact_history_contract_archived_elsewhere_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-replacement-artifact-history-contract-archived",
            SourceStateSeed::Missing,
        )?;
        fixture.write_selected_staged_surface_sqlite_with_source_path_and_created_at(
            &fixture.runtime_db_path,
            &[swap(
                "wallet-raw",
                "sig-source-b",
                parse_ts("2026-04-14T07:57:00Z")?,
                SOL_MINT,
                "TokenRaw111111111111111111111111111111111",
                1.0,
                11.0,
            )],
            parse_ts("2026-04-14T08:10:00Z")?,
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;
        fixture.write_named_staged_candidate_surface_with_source_path_and_covered_since(
            ".discovery_recent_raw_staged.sqlite.archive-staged.prev",
            &fixture.runtime_db_path,
            parse_ts("2026-04-14T07:56:00Z")?,
            1,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-source-a",
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_replacement_artifact_history_contract_read_only(
                &fixture.state_root,
            )?;
        assert_eq!(
            diagnostic.recent_raw_replacement_artifact_history_reason_class,
            RecentRawReplacementArtifactHistoryReasonClass::RecentRawReplacementArtifactHistoryArchivedElsewhere
        );
        assert!(diagnostic.recent_raw_replacement_artifact_history_observed);
        assert!(diagnostic.recent_raw_replacement_previous_artifact_archive_path_present);
        assert_eq!(
            diagnostic.recent_raw_replacement_previous_artifact_archive_candidate_count,
            1
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_previous_artifact_archive_parseable_count,
            1
        );
        assert!(
            !diagnostic
                .recent_raw_replacement_previous_artifact_history_expected_under_current_contract
        );
        Ok(())
    }

    #[test]
    fn recent_raw_replacement_artifact_history_contract_unproven_case_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-replacement-artifact-history-contract-unproven",
            SourceStateSeed::Missing,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_replacement_artifact_history_contract_read_only(
                &fixture.state_root,
            )?;
        assert_eq!(
            diagnostic.recent_raw_replacement_artifact_history_reason_class,
            RecentRawReplacementArtifactHistoryReasonClass::RecentRawReplacementArtifactHistoryUnprovenDueToMissingEvidence
        );
        assert!(!diagnostic.recent_raw_replacement_artifact_history_observed);
        assert!(diagnostic.recent_raw_replacement_fixed_path_overwrite_contract);
        assert!(!diagnostic.recent_raw_replacement_current_fixed_candidate_exists);
        assert!(!diagnostic.recent_raw_replacement_current_fixed_candidate_manifest_parseable);
        assert!(
            diagnostic
                .recent_raw_replacement_previous_artifact_history_missing_due_to_unproven_evidence
        );
        Ok(())
    }

    #[test]
    fn recent_raw_replacement_artifact_history_contract_includes_explicit_fields_stage1(
    ) -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-replacement-artifact-history-contract-fields",
            SourceStateSeed::Missing,
        )?;
        fixture.write_selected_staged_surface_sqlite_with_source_path_and_created_at(
            &fixture.runtime_db_path,
            &[swap(
                "wallet-raw",
                "sig-source-a",
                parse_ts("2026-04-14T07:56:00Z")?,
                SOL_MINT,
                "TokenRaw111111111111111111111111111111111",
                1.0,
                10.0,
            )],
            parse_ts("2026-04-14T08:10:00Z")?,
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_replacement_artifact_history_contract_read_only(
                &fixture.state_root,
            )?;
        assert!(diagnostic
            .recent_raw_replacement_fixed_snapshot_path
            .ends_with(RECENT_RAW_STAGED_SNAPSHOT_FILE_NAME));
        assert!(diagnostic
            .recent_raw_replacement_fixed_metadata_path
            .ends_with(RECENT_RAW_STAGED_METADATA_FILE_NAME));
        assert!(diagnostic.recent_raw_replacement_staged_candidate_scan_succeeded);
        assert!(diagnostic
            .recent_raw_replacement_staged_candidate_scan_error
            .is_none());
        assert!(!diagnostic
            .recent_raw_replacement_artifact_history_explanation
            .is_empty());
        Ok(())
    }

    #[test]
    fn recent_raw_replacement_artifact_history_contract_explain_remains_read_only_stage1(
    ) -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-replacement-artifact-history-contract-read-only",
            SourceStateSeed::Missing,
        )?;
        fixture.write_selected_staged_surface_sqlite_with_source_path_and_created_at(
            &fixture.runtime_db_path,
            &[swap(
                "wallet-raw",
                "sig-source-a",
                parse_ts("2026-04-14T07:56:00Z")?,
                SOL_MINT,
                "TokenRaw111111111111111111111111111111111",
                1.0,
                10.0,
            )],
            parse_ts("2026-04-14T08:10:00Z")?,
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;

        let before = fixture.capture_bytes()?;
        let _ =
            DiscoveryService::explain_recent_raw_replacement_artifact_history_contract_read_only(
                &fixture.state_root,
            )?;
        let after = fixture.capture_bytes()?;
        assert_eq!(before, after);
        Ok(())
    }

    #[test]
    fn recent_raw_replacement_attempt_telemetry_advancing_but_incomplete_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-replacement-attempt-telemetry-advancing",
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
        assert_eq!(
            diagnostic.recent_raw_replacement_attempt_telemetry_reason_class,
            RecentRawReplacementAttemptTelemetryReasonClass::RecentRawReplacementAttemptTelemetryAdvancingButIncomplete
        );
        assert!(diagnostic.recent_raw_replacement_attempt_telemetry_observed);
        assert!(diagnostic.recent_raw_replacement_attempt_telemetry_proves_advancing);
        assert!(!diagnostic.recent_raw_replacement_attempt_telemetry_proves_stalled);
        assert_eq!(
            diagnostic.recent_raw_replacement_attempt_telemetry_last_row_count_before,
            Some(1)
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_attempt_telemetry_last_row_count_after,
            Some(2)
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_attempt_telemetry_staged_progress_advanced,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_attempt_telemetry_staged_progress_resumed,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_attempt_telemetry_staged_progress_preserved_for_retry,
            Some(true)
        );
        Ok(())
    }
