    #[test]
    fn recent_raw_promotion_staged_not_newer_than_promoted_reports_blocker_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-promotion-staged-not-newer",
            SourceStateSeed::StagedCurrent,
        )?;
        fixture.write_promoted_surface(
            "latest.sqlite",
            2,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-staged",
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;
        fixture.write_staged_surface(
            2,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-staged",
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_promotion_blocker_read_only(&fixture.state_root)?;

        assert_eq!(
            diagnostic.recent_raw_promotion_reason_class,
            RecentRawPromotionBlockerReasonClass::RecentRawPromotionBlockedByStagedNotNewerThanPromoted
        );
        assert_eq!(
            diagnostic.recent_raw_staged_newer_than_promoted,
            Some(false)
        );
        assert!(diagnostic.recent_raw_stage3_current_fresh_healthy_evidence_possible);
        Ok(())
    }

    #[test]
    fn recent_raw_promotion_includes_promoted_staged_and_source_facts_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-promotion-facts",
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

        let diagnostic =
            DiscoveryService::explain_recent_raw_promotion_blocker_read_only(&fixture.state_root)?;

        assert_eq!(diagnostic.recent_raw_promoted_exists, true);
        assert_eq!(diagnostic.recent_raw_staged_exists, true);
        assert_eq!(
            diagnostic.recent_raw_runtime_db_path.as_deref(),
            Some(fixture.runtime_db_path.to_string_lossy().as_ref())
        );
        assert!(diagnostic.recent_raw_runtime_db_size_bytes.is_some());
        assert!(diagnostic.recent_raw_runtime_db_mtime.is_some());
        assert!(diagnostic.recent_raw_source_state_available);
        assert_eq!(diagnostic.recent_raw_source_row_count, Some(2));
        assert!(diagnostic.recent_raw_promoted_created_at.is_some());
        assert!(diagnostic.recent_raw_staged_created_at.is_some());
        Ok(())
    }

    #[test]
    fn recent_raw_promotion_explain_remains_read_only_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-promotion-read-only",
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
        let before = fixture.capture_bytes()?;

        let _ =
            DiscoveryService::explain_recent_raw_promotion_blocker_read_only(&fixture.state_root)?;

        let after = fixture.capture_bytes()?;
        assert_eq!(before, after);
        Ok(())
    }

    #[test]
    fn recent_raw_catch_up_progressing_but_not_caught_up_case_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-catch-up-progressing",
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

        let diagnostic =
            DiscoveryService::explain_recent_raw_catch_up_status_read_only(&fixture.state_root)?;

        assert_eq!(
            diagnostic.recent_raw_catch_up_reason_class,
            RecentRawCatchUpReasonClass::RecentRawCatchUpProgressingButNotCaughtUp
        );
        assert!(diagnostic.recent_raw_catch_up_status_observed);
        assert!(diagnostic.recent_raw_catch_up_progressing);
        assert_eq!(diagnostic.recent_raw_staged_ahead_of_promoted, Some(true));
        assert_eq!(diagnostic.recent_raw_source_vs_staged_row_lag, Some(1));
        Ok(())
    }

    #[test]
    fn recent_raw_catch_up_stalled_case_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-catch-up-stalled",
            SourceStateSeed::SourceAheadOfStaged,
        )?;
        fixture.write_promoted_surface(
            "latest.sqlite",
            2,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-staged",
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;
        fixture.write_staged_surface(
            2,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-staged",
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_catch_up_status_read_only(&fixture.state_root)?;

        assert_eq!(
            diagnostic.recent_raw_catch_up_reason_class,
            RecentRawCatchUpReasonClass::RecentRawCatchUpStalled
        );
        assert!(diagnostic.recent_raw_catch_up_status_observed);
        assert_eq!(diagnostic.recent_raw_staged_advancing, Some(false));
        assert!(!diagnostic.recent_raw_catch_up_progressing);
        assert!(!diagnostic.recent_raw_catch_up_losing_to_source);
        Ok(())
    }

    #[test]
    fn recent_raw_catch_up_losing_to_source_case_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-catch-up-losing",
            SourceStateSeed::SourceAheadOfStaged,
        )?;
        fixture.write_promoted_surface(
            "latest.sqlite",
            2,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-staged",
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;
        fixture.write_staged_surface(
            1,
            parse_ts("2026-04-14T07:55:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_catch_up_status_read_only(&fixture.state_root)?;

        assert_eq!(
            diagnostic.recent_raw_catch_up_reason_class,
            RecentRawCatchUpReasonClass::RecentRawCatchUpLosingToSource
        );
        assert!(diagnostic.recent_raw_catch_up_losing_to_source);
        assert_eq!(diagnostic.recent_raw_staged_ahead_of_promoted, Some(false));
        assert_eq!(diagnostic.recent_raw_staged_vs_promoted_row_delta, Some(-1));
        Ok(())
    }

    #[test]
    fn recent_raw_catch_up_caught_up_case_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-catch-up-caught-up",
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

        let diagnostic =
            DiscoveryService::explain_recent_raw_catch_up_status_read_only(&fixture.state_root)?;

        assert_eq!(
            diagnostic.recent_raw_catch_up_reason_class,
            RecentRawCatchUpReasonClass::RecentRawCatchUpCaughtUp
        );
        assert!(diagnostic.recent_raw_catch_up_status_observed);
        assert_eq!(diagnostic.recent_raw_source_vs_staged_row_lag, Some(0));
        assert!(!diagnostic.recent_raw_catch_up_progressing);
        Ok(())
    }

    #[test]
    fn recent_raw_catch_up_unproven_due_to_missing_evidence_case_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-catch-up-unproven",
            SourceStateSeed::Missing,
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

        let diagnostic =
            DiscoveryService::explain_recent_raw_catch_up_status_read_only(&fixture.state_root)?;

        assert_eq!(
            diagnostic.recent_raw_catch_up_reason_class,
            RecentRawCatchUpReasonClass::RecentRawCatchUpUnprovenDueToMissingEvidence
        );
        assert!(!diagnostic.recent_raw_catch_up_status_observed);
        assert!(diagnostic.recent_raw_catch_up_indeterminate);
        Ok(())
    }

    #[test]
    fn recent_raw_catch_up_includes_lag_and_progress_fields_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-catch-up-facts",
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

        let diagnostic =
            DiscoveryService::explain_recent_raw_catch_up_status_read_only(&fixture.state_root)?;

        assert_eq!(diagnostic.recent_raw_promoted_exists, true);
        assert_eq!(diagnostic.recent_raw_staged_exists, true);
        assert!(diagnostic.recent_raw_runtime_db_path.is_some());
        assert_eq!(diagnostic.recent_raw_source_vs_staged_row_lag, Some(1));
        assert_eq!(diagnostic.recent_raw_source_vs_promoted_row_lag, Some(2));
        assert_eq!(diagnostic.recent_raw_staged_advancing, Some(true));
        assert_eq!(
            diagnostic.recent_raw_staged_last_batch_completed_at_newer_than_promoted,
            Some(true)
        );
        Ok(())
    }

    #[test]
    fn recent_raw_catch_up_explain_remains_read_only_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-catch-up-read-only",
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
        let before = fixture.capture_bytes()?;

        let _ =
            DiscoveryService::explain_recent_raw_catch_up_status_read_only(&fixture.state_root)?;

        let after = fixture.capture_bytes()?;
        assert_eq!(before, after);
        Ok(())
    }

    #[test]
    fn recent_raw_staged_lineage_monotonic_case_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-staged-lineage-monotonic",
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

        let diagnostic =
            DiscoveryService::explain_recent_raw_staged_lineage_read_only(&fixture.state_root)?;

        assert_eq!(
            diagnostic.recent_raw_staged_lineage_reason_class,
            RecentRawStagedLineageReasonClass::RecentRawStagedLineageMonotonicButIncomplete
        );
        assert_eq!(
            diagnostic.recent_raw_staged_same_source_db_as_promoted,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_staged_cursor_relation_to_promoted,
            RecentRawLineageRelation::Ahead
        );
        assert_eq!(
            diagnostic.recent_raw_staged_cursor_relation_basis,
            RecentRawCursorRelationBasis::DirectCoveredThroughCursorComparison
        );
        assert_eq!(
            diagnostic.recent_raw_staged_cursor_ts_relation_to_promoted,
            RecentRawLineageRelation::Ahead
        );
        assert_eq!(
            diagnostic.recent_raw_staged_cursor_slot_relation_to_promoted,
            RecentRawLineageRelation::Ahead
        );
        assert_eq!(
            diagnostic.recent_raw_staged_cursor_signature_equal_to_promoted,
            Some(false)
        );
        assert!(diagnostic
            .recent_raw_staged_cursor_relation_explanation
            .contains("direct covered-through cursor comparison"));
        assert_eq!(
            diagnostic.recent_raw_staged_covered_since_relation_to_promoted,
            RecentRawLineageRelation::Equal
        );
        assert_eq!(
            diagnostic.recent_raw_staged_monotonic_relative_to_promoted,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_staged_regressed_relative_to_promoted,
            Some(false)
        );
        Ok(())
    }

    #[test]
    fn recent_raw_staged_lineage_later_covered_since_is_regressed_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-staged-lineage-later-covered-since",
            SourceStateSeed::SourceAheadOfStaged,
        )?;
        fixture.write_promoted_surface(
            "latest.sqlite",
            1,
            parse_ts("2026-04-14T07:55:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;
        fixture.write_staged_surface_with_source_path_and_covered_since(
            &fixture.runtime_db_path,
            parse_ts("2026-04-14T07:56:00Z")?,
            2,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-staged",
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_staged_lineage_read_only(&fixture.state_root)?;

        assert_eq!(
            diagnostic.recent_raw_staged_lineage_reason_class,
            RecentRawStagedLineageReasonClass::RecentRawStagedLineageRegressedRelativeToPromoted
        );
        assert_eq!(
            diagnostic.recent_raw_staged_covered_since_relation_to_promoted,
            RecentRawLineageRelation::Behind
        );
        assert_eq!(
            diagnostic.recent_raw_staged_monotonic_relative_to_promoted,
            Some(false)
        );
        assert_eq!(
            diagnostic.recent_raw_staged_regressed_relative_to_promoted,
            Some(true)
        );
        Ok(())
    }

    #[test]
    fn recent_raw_staged_lineage_earlier_covered_since_is_monotonic_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-staged-lineage-earlier-covered-since",
            SourceStateSeed::SourceAheadOfStaged,
        )?;
        fixture.write_promoted_surface_with_covered_since(
            "latest.sqlite",
            parse_ts("2026-04-14T07:56:00Z")?,
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

        let diagnostic =
            DiscoveryService::explain_recent_raw_staged_lineage_read_only(&fixture.state_root)?;

        assert_eq!(
            diagnostic.recent_raw_staged_lineage_reason_class,
            RecentRawStagedLineageReasonClass::RecentRawStagedLineageMonotonicButIncomplete
        );
        assert_eq!(
            diagnostic.recent_raw_staged_covered_since_relation_to_promoted,
            RecentRawLineageRelation::Ahead
        );
        assert_eq!(
            diagnostic.recent_raw_staged_monotonic_relative_to_promoted,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_staged_regressed_relative_to_promoted,
            Some(false)
        );
        Ok(())
    }

    #[test]
    fn recent_raw_staged_lineage_regressed_case_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-staged-lineage-regressed",
            SourceStateSeed::SourceAheadOfStaged,
        )?;
        fixture.write_promoted_surface(
            "latest.sqlite",
            2,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-staged",
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;
        fixture.write_staged_surface(
            1,
            parse_ts("2026-04-14T07:55:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:10:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_staged_lineage_read_only(&fixture.state_root)?;

        assert_eq!(
            diagnostic.recent_raw_staged_lineage_reason_class,
            RecentRawStagedLineageReasonClass::RecentRawStagedLineageRegressedRelativeToPromoted
        );
        assert_eq!(
            diagnostic.recent_raw_staged_same_source_db_as_promoted,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_staged_cursor_relation_to_promoted,
            RecentRawLineageRelation::Behind
        );
        assert_eq!(
            diagnostic.recent_raw_staged_cursor_ts_relation_to_promoted,
            RecentRawLineageRelation::Behind
        );
        assert_eq!(
            diagnostic.recent_raw_staged_cursor_slot_relation_to_promoted,
            RecentRawLineageRelation::Behind
        );
        assert_eq!(
            diagnostic.recent_raw_staged_cursor_signature_equal_to_promoted,
            Some(false)
        );
        assert_eq!(
            diagnostic.recent_raw_staged_regressed_relative_to_promoted,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_staged_closer_to_source_than_promoted,
            Some(false)
        );
        Ok(())
    }
