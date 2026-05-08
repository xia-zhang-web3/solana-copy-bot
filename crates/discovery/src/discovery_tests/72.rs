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
