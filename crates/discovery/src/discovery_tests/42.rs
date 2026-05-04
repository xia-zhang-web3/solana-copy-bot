    #[test]
    fn recent_raw_promoted_retention_contract_unproven_case_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-promoted-retention-contract-unproven",
            SourceStateSeed::Missing,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_promoted_retention_contract_read_only(
                &fixture.state_root,
            )?;
        assert_eq!(
            diagnostic.recent_raw_promoted_retention_reason_class,
            RecentRawPromotedRetentionReasonClass::RecentRawPromotedRetentionContractUnprovenDueToMissingEvidence
        );
        assert!(!diagnostic.recent_raw_promoted_retention_observed);
        assert_eq!(
            diagnostic.recent_raw_promoted_retention_basis,
            RecentRawPromotedRetentionBasis::Unproven
        );
        assert_eq!(
            diagnostic.recent_raw_promoted_currently_retained_as_truth,
            Some(false)
        );
        Ok(())
    }

    #[test]
    fn recent_raw_promoted_retention_contract_includes_explicit_fields_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-promoted-retention-contract-fields",
            SourceStateSeed::Missing,
        )?;
        fixture.rewrite_source_state(
            &[
                swap(
                    "wallet-raw",
                    "sig-source-old",
                    parse_ts("2026-04-14T07:54:00Z")?,
                    SOL_MINT,
                    "TokenRaw111111111111111111111111111111111",
                    1.0,
                    9.0,
                ),
                swap(
                    "wallet-raw",
                    "sig-source-a",
                    parse_ts("2026-04-14T07:56:00Z")?,
                    SOL_MINT,
                    "TokenRaw111111111111111111111111111111111",
                    1.0,
                    10.0,
                ),
                swap(
                    "wallet-raw",
                    "sig-source-b",
                    parse_ts("2026-04-14T07:57:00Z")?,
                    SOL_MINT,
                    "TokenRaw111111111111111111111111111111111",
                    1.0,
                    11.0,
                ),
            ],
            parse_ts("2026-04-14T08:06:00Z")?,
        )?;
        fixture.prune_source_state_before(
            parse_ts("2026-04-14T07:56:00Z")?,
            32,
            parse_ts("2026-04-14T08:07:00Z")?,
        )?;
        fixture.write_promoted_surface_with_covered_since(
            "latest.sqlite",
            parse_ts("2026-04-14T07:55:00Z")?,
            2,
            parse_ts("2026-04-14T07:57:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_promoted_retention_contract_read_only(
                &fixture.state_root,
            )?;
        assert!(diagnostic
            .recent_raw_promoted_snapshot_path
            .ends_with("latest.sqlite"));
        assert!(diagnostic
            .recent_raw_promoted_metadata_path
            .ends_with("latest.json"));
        assert_eq!(
            diagnostic.recent_raw_promoted_same_source_db_as_current_source,
            Some(true)
        );
        assert_eq!(diagnostic.recent_raw_source_window_contract_observed, true);
        assert!(!diagnostic
            .recent_raw_promoted_retention_basis_explanation
            .is_empty());
        Ok(())
    }

    #[test]
    fn recent_raw_promoted_retention_contract_explain_remains_read_only_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-promoted-retention-contract-read-only",
            SourceStateSeed::Missing,
        )?;
        fixture.rewrite_source_state(
            &[
                swap(
                    "wallet-raw",
                    "sig-source-old",
                    parse_ts("2026-04-14T07:54:00Z")?,
                    SOL_MINT,
                    "TokenRaw111111111111111111111111111111111",
                    1.0,
                    9.0,
                ),
                swap(
                    "wallet-raw",
                    "sig-source-a",
                    parse_ts("2026-04-14T07:56:00Z")?,
                    SOL_MINT,
                    "TokenRaw111111111111111111111111111111111",
                    1.0,
                    10.0,
                ),
                swap(
                    "wallet-raw",
                    "sig-source-b",
                    parse_ts("2026-04-14T07:57:00Z")?,
                    SOL_MINT,
                    "TokenRaw111111111111111111111111111111111",
                    1.0,
                    11.0,
                ),
            ],
            parse_ts("2026-04-14T08:06:00Z")?,
        )?;
        fixture.prune_source_state_before(
            parse_ts("2026-04-14T07:56:00Z")?,
            32,
            parse_ts("2026-04-14T08:07:00Z")?,
        )?;
        fixture.write_promoted_surface_with_covered_since(
            "latest.sqlite",
            parse_ts("2026-04-14T07:55:00Z")?,
            2,
            parse_ts("2026-04-14T07:57:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;

        let before = fixture.capture_bytes()?;
        let _ = DiscoveryService::explain_recent_raw_promoted_retention_contract_read_only(
            &fixture.state_root,
        )?;
        let after = fixture.capture_bytes()?;
        assert_eq!(before, after);
        Ok(())
    }

    #[test]
    fn recent_raw_replacement_promotion_contract_ready_case_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-replacement-promotion-contract-ready",
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
            DiscoveryService::explain_recent_raw_replacement_promotion_contract_read_only(
                &fixture.state_root,
            )?;
        assert_eq!(
            diagnostic.recent_raw_replacement_promotion_reason_class,
            RecentRawReplacementPromotionReasonClass::RecentRawReplacementCandidateReadyToPromote
        );
        assert!(diagnostic.recent_raw_replacement_promotion_observed);
        assert!(diagnostic.recent_raw_replacement_candidate_exists);
        assert_eq!(
            diagnostic.recent_raw_replacement_candidate_source_db_matches_promoted,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_candidate_start_matches_current_source,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_candidate_complete_against_current_source,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_candidate_promotable_now,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_promotion_would_retire_current_promoted_truth,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_stage3_blocked_on_replacement_candidate,
            Some(false)
        );
        Ok(())
    }

    #[test]
    fn recent_raw_replacement_promotion_contract_incomplete_against_current_source_stage1(
    ) -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-replacement-promotion-contract-incomplete",
            SourceStateSeed::Missing,
        )?;
        fixture.rewrite_source_state(
            &[
                swap(
                    "wallet-raw",
                    "sig-source-old",
                    parse_ts("2026-04-14T07:55:00Z")?,
                    SOL_MINT,
                    "TokenRaw111111111111111111111111111111111",
                    1.0,
                    10.0,
                ),
                swap(
                    "wallet-raw",
                    "sig-promoted",
                    parse_ts("2026-04-14T07:56:00Z")?,
                    SOL_MINT,
                    "TokenRaw111111111111111111111111111111111",
                    1.0,
                    11.0,
                ),
                swap(
                    "wallet-raw",
                    "sig-source",
                    parse_ts("2026-04-14T07:57:00Z")?,
                    SOL_MINT,
                    "TokenRaw111111111111111111111111111111111",
                    1.0,
                    12.0,
                ),
            ],
            parse_ts("2026-04-14T08:06:00Z")?,
        )?;
        fixture.prune_source_state_before(
            parse_ts("2026-04-14T07:56:00Z")?,
            32,
            parse_ts("2026-04-14T08:07:00Z")?,
        )?;
        fixture.write_promoted_surface_with_covered_since(
            "latest.sqlite",
            parse_ts("2026-04-14T07:55:00Z")?,
            2,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;
        fixture.write_staged_surface_with_source_path_and_covered_since(
            &fixture.runtime_db_path,
            parse_ts("2026-04-14T07:56:00Z")?,
            1,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_replacement_promotion_contract_read_only(
                &fixture.state_root,
            )?;
        assert_eq!(
            diagnostic.recent_raw_replacement_promotion_reason_class,
            RecentRawReplacementPromotionReasonClass::RecentRawReplacementCandidateIncompleteAgainstCurrentSource
        );
        assert!(diagnostic.recent_raw_replacement_promotion_observed);
        assert!(diagnostic.recent_raw_replacement_candidate_exists);
        assert_eq!(
            diagnostic.recent_raw_replacement_candidate_start_matches_current_source,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_candidate_covered_through_relation_to_promoted,
            RecentRawLineageRelation::Equal
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_candidate_row_count_relation_to_promoted,
            RecentRawLineageRelation::Behind
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_candidate_complete_against_current_source,
            Some(false)
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_candidate_promotable_now,
            Some(false)
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_promotion_would_retire_current_promoted_truth,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_stage3_blocked_on_replacement_candidate,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_promotion_reason_class,
            RecentRawPromotionBlockerReasonClass::RecentRawPromotionBlockedByIncompleteStagedCoverage
        );
        Ok(())
    }

    #[test]
    fn recent_raw_replacement_promotion_contract_not_newer_than_promoted_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-replacement-promotion-contract-not-newer",
            SourceStateSeed::StagedCurrent,
        )?;
        fixture.write_promoted_surface(
            "latest.sqlite",
            2,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-staged",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;
        fixture.write_staged_surface(
            2,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-staged",
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_replacement_promotion_contract_read_only(
                &fixture.state_root,
            )?;
        assert_eq!(
            diagnostic.recent_raw_replacement_promotion_reason_class,
            RecentRawReplacementPromotionReasonClass::RecentRawReplacementCandidateNotNewerThanPromoted
        );
        assert!(diagnostic.recent_raw_replacement_promotion_observed);
        assert_eq!(
            diagnostic.recent_raw_replacement_candidate_complete_against_current_source,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_candidate_promotable_now,
            Some(false)
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_candidate_covered_through_relation_to_promoted,
            RecentRawLineageRelation::Equal
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_candidate_row_count_relation_to_promoted,
            RecentRawLineageRelation::Equal
        );
        assert_eq!(
            diagnostic.recent_raw_stage3_blocked_on_replacement_candidate,
            Some(false)
        );
        Ok(())
    }

    #[test]
    fn recent_raw_replacement_promotion_contract_unproven_case_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-replacement-promotion-contract-unproven",
            SourceStateSeed::Missing,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_replacement_promotion_contract_read_only(
                &fixture.state_root,
            )?;
        assert_eq!(
            diagnostic.recent_raw_replacement_promotion_reason_class,
            RecentRawReplacementPromotionReasonClass::RecentRawReplacementPromotionContractUnprovenDueToMissingEvidence
        );
        assert!(!diagnostic.recent_raw_replacement_promotion_observed);
        assert!(!diagnostic.recent_raw_replacement_candidate_exists);
        assert_eq!(
            diagnostic.recent_raw_replacement_candidate_complete_against_current_source,
            None
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_candidate_promotable_now,
            None
        );
        Ok(())
    }

    #[test]
    fn recent_raw_replacement_promotion_contract_includes_explicit_fields_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-replacement-promotion-contract-fields",
            SourceStateSeed::SourceAheadOfStaged,
        )?;
        fixture.write_promoted_surface(
            "latest.sqlite",
            2,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-staged",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;
        fixture.write_staged_surface(
            2,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-staged",
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_replacement_promotion_contract_read_only(
                &fixture.state_root,
            )?;
        assert!(!diagnostic
            .recent_raw_replacement_promotion_explanation
            .is_empty());
        assert_eq!(
            diagnostic.recent_raw_replacement_candidate_source_db_matches_promoted,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_candidate_start_matches_current_source,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_replacement_promotion_would_retire_current_promoted_truth,
            Some(true)
        );
        Ok(())
    }

    #[test]
    fn recent_raw_replacement_promotion_contract_explain_remains_read_only_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-replacement-promotion-contract-read-only",
            SourceStateSeed::SourceAheadOfStaged,
        )?;
        fixture.write_promoted_surface(
            "latest.sqlite",
            2,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-staged",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;
        fixture.write_staged_surface(
            2,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-staged",
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;

        let before = fixture.capture_bytes()?;
        let _ = DiscoveryService::explain_recent_raw_replacement_promotion_contract_read_only(
            &fixture.state_root,
        )?;
        let after = fixture.capture_bytes()?;
        assert_eq!(before, after);
        Ok(())
    }
