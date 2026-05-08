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
