    #[test]
    fn recent_raw_staged_lineage_identical_cursor_reports_equal_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-staged-lineage-identical-cursor",
            SourceStateSeed::StagedCurrent,
        )?;
        fixture.write_promoted_surface(
            "latest.sqlite",
            1,
            parse_ts("2026-04-14T07:55:00Z")?,
            "sig-same",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;
        fixture.write_staged_surface(
            2,
            parse_ts("2026-04-14T07:55:00Z")?,
            "sig-same",
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_staged_lineage_read_only(&fixture.state_root)?;

        assert_eq!(
            diagnostic.recent_raw_staged_cursor_relation_basis,
            RecentRawCursorRelationBasis::DirectCoveredThroughCursorComparison
        );
        assert_eq!(
            diagnostic.recent_raw_staged_cursor_relation_to_promoted,
            RecentRawLineageRelation::Equal
        );
        assert_eq!(
            diagnostic.recent_raw_staged_cursor_ts_relation_to_promoted,
            RecentRawLineageRelation::Equal
        );
        assert_eq!(
            diagnostic.recent_raw_staged_cursor_slot_relation_to_promoted,
            RecentRawLineageRelation::Equal
        );
        assert_eq!(
            diagnostic.recent_raw_staged_cursor_signature_equal_to_promoted,
            Some(true)
        );
        Ok(())
    }

    #[test]
    fn recent_raw_staged_lineage_signature_difference_does_not_report_equal_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-staged-lineage-signature-difference",
            SourceStateSeed::StagedCurrent,
        )?;
        fixture.write_promoted_surface(
            "latest.sqlite",
            1,
            parse_ts("2026-04-14T07:55:00Z")?,
            "sig-b",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;
        fixture.write_staged_surface(
            2,
            parse_ts("2026-04-14T07:55:00Z")?,
            "sig-a",
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_staged_lineage_read_only(&fixture.state_root)?;

        assert_eq!(
            diagnostic.recent_raw_staged_cursor_relation_to_promoted,
            RecentRawLineageRelation::Behind
        );
        assert_eq!(
            diagnostic.recent_raw_staged_cursor_ts_relation_to_promoted,
            RecentRawLineageRelation::Equal
        );
        assert_eq!(
            diagnostic.recent_raw_staged_cursor_slot_relation_to_promoted,
            RecentRawLineageRelation::Equal
        );
        assert_eq!(
            diagnostic.recent_raw_staged_cursor_signature_equal_to_promoted,
            Some(false)
        );
        Ok(())
    }

    #[test]
    fn recent_raw_staged_lineage_live_shape_contradiction_reports_behind_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-staged-lineage-live-shape-contradiction",
            SourceStateSeed::SourceAheadOfStaged,
        )?;
        fixture.write_promoted_surface(
            "latest.sqlite",
            52_615_967,
            parse_ts("2026-04-10T09:34:27.024499053Z")?,
            "3UNhL7QFL6tYg2XNTqiLRi75zDTbNRLbB65uLnJ8m5hz5DesK9FNSVZRaSkoLDqhUon4BVgkRen8pdYnCT7emGuu",
            parse_ts("2026-04-10T09:43:22.332587209Z")?,
        )?;
        fixture.write_staged_surface(
            21_824_742,
            parse_ts("2026-03-28T03:15:38.311254197Z")?,
            "3eBHyga7C7qCgDvRtReEDwbKiGnhFEjeYcfLiMMtBBGtGFjXbAJSB5SrmnMNvqYUssWbRR7y72HdK2Vyd4ChDTni",
            parse_ts("2026-04-14T11:28:53.343352735Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_staged_lineage_read_only(&fixture.state_root)?;

        assert_eq!(
            diagnostic.recent_raw_staged_cursor_relation_to_promoted,
            RecentRawLineageRelation::Behind
        );
        assert_ne!(
            diagnostic.recent_raw_staged_cursor_relation_to_promoted,
            RecentRawLineageRelation::Equal
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
        Ok(())
    }

    #[test]
    fn recent_raw_staged_lineage_different_source_case_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-staged-lineage-different-source",
            SourceStateSeed::SourceAheadOfStaged,
        )?;
        fixture.write_promoted_surface(
            "latest.sqlite",
            1,
            parse_ts("2026-04-14T07:55:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;
        fixture.write_staged_surface_with_source_path(
            &fixture.state_root.join("foreign-source.db"),
            2,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-staged",
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_staged_lineage_read_only(&fixture.state_root)?;

        assert_eq!(
            diagnostic.recent_raw_staged_lineage_reason_class,
            RecentRawStagedLineageReasonClass::RecentRawStagedLineagePointsToDifferentSource
        );
        assert_eq!(
            diagnostic.recent_raw_staged_same_source_db_as_promoted,
            Some(false)
        );
        assert_eq!(
            diagnostic.recent_raw_staged_cursor_relation_to_promoted,
            RecentRawLineageRelation::DifferentSource
        );
        assert_eq!(
            diagnostic.recent_raw_staged_cursor_relation_basis,
            RecentRawCursorRelationBasis::DirectCoveredThroughCursorComparison
        );
        assert_eq!(
            diagnostic.recent_raw_staged_cursor_ts_relation_to_promoted,
            RecentRawLineageRelation::DifferentSource
        );
        assert_eq!(
            diagnostic.recent_raw_staged_cursor_slot_relation_to_promoted,
            RecentRawLineageRelation::DifferentSource
        );
        assert_eq!(
            diagnostic.recent_raw_staged_cursor_signature_equal_to_promoted,
            None
        );
        assert_eq!(
            diagnostic.recent_raw_staged_monotonic_relative_to_promoted,
            None
        );
        Ok(())
    }

    #[test]
    fn recent_raw_staged_lineage_unproven_case_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-staged-lineage-unproven",
            SourceStateSeed::Missing,
        )?;
        fixture.write_promoted_surface(
            "latest.sqlite",
            1,
            parse_ts("2026-04-14T07:55:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_staged_lineage_read_only(&fixture.state_root)?;

        assert_eq!(
            diagnostic.recent_raw_staged_lineage_reason_class,
            RecentRawStagedLineageReasonClass::RecentRawStagedLineageUnprovenDueToMissingEvidence
        );
        assert!(!diagnostic.recent_raw_staged_lineage_observed);
        assert_eq!(
            diagnostic.recent_raw_staged_cursor_relation_to_promoted,
            RecentRawLineageRelation::Unproven
        );
        assert_eq!(
            diagnostic.recent_raw_staged_cursor_relation_basis,
            RecentRawCursorRelationBasis::DirectCoveredThroughCursorComparison
        );
        assert_eq!(
            diagnostic.recent_raw_staged_cursor_ts_relation_to_promoted,
            RecentRawLineageRelation::Unproven
        );
        assert_eq!(
            diagnostic.recent_raw_staged_cursor_slot_relation_to_promoted,
            RecentRawLineageRelation::Unproven
        );
        assert_eq!(
            diagnostic.recent_raw_staged_cursor_signature_equal_to_promoted,
            None
        );
        Ok(())
    }

    #[test]
    fn recent_raw_staged_lineage_includes_comparison_fields_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-staged-lineage-fields",
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

        assert!(diagnostic.recent_raw_promoted_source_db_path.is_some());
        assert!(diagnostic.recent_raw_staged_source_db_path.is_some());
        assert_eq!(diagnostic.recent_raw_source_outruns_both, Some(true));
        assert_eq!(
            diagnostic.recent_raw_staged_row_count_relation_to_promoted,
            RecentRawLineageRelation::Behind
        );
        assert_eq!(
            diagnostic.recent_raw_staged_covered_since_relation_to_promoted,
            RecentRawLineageRelation::Equal
        );
        assert_eq!(
            diagnostic.recent_raw_staged_cursor_relation_basis,
            RecentRawCursorRelationBasis::DirectCoveredThroughCursorComparison
        );
        assert!(!diagnostic
            .recent_raw_staged_cursor_relation_explanation
            .is_empty());
        Ok(())
    }

    #[test]
    fn recent_raw_staged_lineage_explain_remains_read_only_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-staged-lineage-read-only",
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

        let _ = DiscoveryService::explain_recent_raw_staged_lineage_read_only(&fixture.state_root)?;

        let after = fixture.capture_bytes()?;
        assert_eq!(before, after);
        Ok(())
    }

    #[test]
    fn recent_raw_staged_regression_selected_artifact_already_behind_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-staged-regression-selected-behind",
            SourceStateSeed::SourceAheadOfStaged,
        )?;
        fixture.write_promoted_surface(
            "latest.sqlite",
            2,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;
        fixture.write_staged_surface(
            1,
            parse_ts("2026-04-14T07:55:00Z")?,
            "sig-staged",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_staged_regression_read_only(&fixture.state_root)?;

        assert_eq!(
            diagnostic.recent_raw_staged_regression_reason_class,
            RecentRawStagedRegressionReasonClass::RecentRawStagedRegressionSelectedOlderStagedArtifact
        );
        assert_eq!(
            diagnostic.recent_raw_selected_staged_frontier_behind_promoted_before_comparison,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_selected_staged_created_after_promoted,
            Some(false)
        );
        assert_eq!(
            diagnostic.recent_raw_selected_staged_completed_after_creation,
            Some(false)
        );
        Ok(())
    }

    #[test]
    fn recent_raw_staged_regression_multiple_candidates_proves_wrong_selection_stage1() -> Result<()>
    {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-staged-regression-multiple-candidates",
            SourceStateSeed::SourceAheadOfStaged,
        )?;
        fixture.write_promoted_surface(
            "latest.sqlite",
            2,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;
        fixture.write_staged_surface(
            1,
            parse_ts("2026-04-14T07:55:00Z")?,
            "sig-staged-fixed",
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;
        fixture.write_named_staged_candidate_surface(
            ".discovery_recent_raw_staged.sqlite.archive-staged.alt",
            3,
            parse_ts("2026-04-14T07:57:00Z")?,
            "sig-staged-alt",
            parse_ts("2026-04-14T08:10:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_staged_regression_read_only(&fixture.state_root)?;

        assert_eq!(
            diagnostic.recent_raw_staged_regression_reason_class,
            RecentRawStagedRegressionReasonClass::RecentRawStagedRegressionMultipleCandidatesWrongSelection
        );
        assert_eq!(diagnostic.recent_raw_staged_candidate_count, 2);
        assert!(diagnostic.recent_raw_multiple_staged_candidates_present);
        assert_eq!(
            diagnostic.recent_raw_selected_staged_is_latest_candidate,
            Some(false)
        );
        assert!(diagnostic
            .recent_raw_staged_candidate_snapshot_paths
            .iter()
            .any(|path| path.ends_with(".discovery_recent_raw_staged.sqlite.archive-staged.alt")));
        Ok(())
    }

    #[test]
    fn recent_raw_staged_regression_created_after_promoted_but_behind_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-staged-regression-created-after-promoted",
            SourceStateSeed::SourceAheadOfStaged,
        )?;
        fixture.write_promoted_surface(
            "latest.sqlite",
            2,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;
        fixture.write_staged_surface(
            1,
            parse_ts("2026-04-14T07:55:00Z")?,
            "sig-staged",
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_staged_regression_read_only(&fixture.state_root)?;

        assert_eq!(
            diagnostic.recent_raw_staged_regression_reason_class,
            RecentRawStagedRegressionReasonClass::RecentRawStagedRegressionArtifactItselfAlreadyBehind
        );
        assert_eq!(
            diagnostic.recent_raw_selected_staged_created_after_promoted,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_selected_staged_frontier_behind_promoted_before_comparison,
            Some(true)
        );
        Ok(())
    }

    #[test]
    fn recent_raw_staged_regression_unproven_case_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-staged-regression-unproven",
            SourceStateSeed::Missing,
        )?;
        fixture.write_promoted_surface(
            "latest.sqlite",
            1,
            parse_ts("2026-04-14T07:55:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_staged_regression_read_only(&fixture.state_root)?;

        assert_eq!(
            diagnostic.recent_raw_staged_regression_reason_class,
            RecentRawStagedRegressionReasonClass::RecentRawStagedRegressionUnprovenDueToMissingEvidence
        );
        assert!(!diagnostic.recent_raw_staged_regression_observed);
        assert_eq!(diagnostic.recent_raw_staged_candidate_count, 0);
        Ok(())
    }

    #[test]
    fn recent_raw_staged_regression_includes_selection_fields_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-staged-regression-fields",
            SourceStateSeed::SourceAheadOfStaged,
        )?;
        fixture.write_promoted_surface(
            "latest.sqlite",
            2,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;
        fixture.write_staged_surface_with_source_path_and_covered_since(
            &fixture.runtime_db_path,
            parse_ts("2026-04-14T07:55:00Z")?,
            1,
            parse_ts("2026-04-14T07:55:00Z")?,
            "sig-staged",
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;
        fixture
            .rewrite_selected_staged_last_batch_completed_at(parse_ts("2026-04-14T08:10:00Z")?)?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_staged_regression_read_only(&fixture.state_root)?;

        assert!(diagnostic
            .recent_raw_promoted_snapshot_path
            .ends_with("latest.sqlite"));
        assert!(diagnostic
            .recent_raw_staged_snapshot_path
            .ends_with(".discovery_recent_raw_staged.sqlite.archive-staged"));
        assert_eq!(
            diagnostic.recent_raw_selected_staged_completed_after_creation,
            Some(true)
        );
        assert!(!diagnostic.recent_raw_staged_selection_reason.is_empty());
        Ok(())
    }

    #[test]
    fn recent_raw_staged_regression_explain_remains_read_only_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-staged-regression-read-only",
            SourceStateSeed::SourceAheadOfStaged,
        )?;
        fixture.write_promoted_surface(
            "latest.sqlite",
            2,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;
        fixture.write_staged_surface(
            1,
            parse_ts("2026-04-14T07:55:00Z")?,
            "sig-staged",
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;
        let before = fixture.capture_bytes()?;

        let _ =
            DiscoveryService::explain_recent_raw_staged_regression_read_only(&fixture.state_root)?;

        let after = fixture.capture_bytes()?;
        assert_eq!(before, after);
        Ok(())
    }
