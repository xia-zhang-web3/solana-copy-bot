    #[test]
    fn recent_raw_source_window_contract_promoted_reflects_older_window_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-source-window-contract-promoted-older-window",
            SourceStateSeed::Missing,
        )?;
        fixture.rewrite_source_state(
            &[
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
        fixture.write_promoted_surface_with_covered_since(
            "latest.sqlite",
            parse_ts("2026-04-14T07:55:00Z")?,
            2,
            parse_ts("2026-04-14T07:57:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;

        let diagnostic = DiscoveryService::explain_recent_raw_source_window_contract_read_only(
            &fixture.state_root,
        )?;
        assert_eq!(
            diagnostic.recent_raw_source_window_contract_reason_class,
            RecentRawSourceWindowContractReasonClass::RecentRawSourceWindowPromotedSurfaceReflectsOlderWindow
        );
        assert!(diagnostic.recent_raw_source_window_contract_observed);
        assert_eq!(
            diagnostic.recent_raw_source_start_later_than_promoted,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_promoted_reflects_older_still_promoted_window,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_source_window_matches_current_bounded_contract,
            Some(true)
        );
        assert!(diagnostic.recent_raw_source_window_probe_bounded);
        assert_eq!(
            diagnostic.recent_raw_source_window_probe_mode,
            RecentRawSourceWindowProbeMode::BoundedIndexEdges
        );
        assert_eq!(
            diagnostic.recent_raw_source_cached_state_matches_bounded_probe,
            Some(true)
        );
        assert!(diagnostic
            .recent_raw_source_bounded_probe_covered_since
            .is_some());
        assert!(diagnostic
            .recent_raw_source_bounded_probe_covered_through
            .is_some());
        assert_eq!(diagnostic.recent_raw_source_scanned_covered_since, None);
        assert_eq!(diagnostic.recent_raw_source_scanned_covered_through, None);
        assert_eq!(diagnostic.recent_raw_source_scanned_row_count, None);
        assert_eq!(
            diagnostic.recent_raw_source_cached_state_matches_scanned_rows,
            None
        );
        assert_eq!(
            diagnostic.recent_raw_source_prune_activity_recorded,
            Some(false)
        );
        assert_eq!(
            diagnostic.recent_raw_source_window_contract_basis,
            RecentRawSourceWindowContractBasis::PromotedStillReflectsOlderWindowWhileCurrentSourceStartsLater
        );
        Ok(())
    }

    #[test]
    fn recent_raw_source_window_contract_unproven_case_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-source-window-contract-unproven",
            SourceStateSeed::Missing,
        )?;
        fixture.write_promoted_surface_with_covered_since(
            "latest.sqlite",
            parse_ts("2026-04-14T07:55:00Z")?,
            1,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;

        let diagnostic = DiscoveryService::explain_recent_raw_source_window_contract_read_only(
            &fixture.state_root,
        )?;
        assert_eq!(
            diagnostic.recent_raw_source_window_contract_reason_class,
            RecentRawSourceWindowContractReasonClass::RecentRawSourceWindowCurrentAndPromotedContractRelationUnproven
        );
        assert!(!diagnostic.recent_raw_source_window_contract_observed);
        assert_eq!(
            diagnostic.recent_raw_source_window_contract_basis,
            RecentRawSourceWindowContractBasis::Unproven
        );
        assert_eq!(
            diagnostic.recent_raw_source_window_matches_current_bounded_contract,
            None
        );
        assert!(diagnostic.recent_raw_source_window_probe_bounded);
        assert_eq!(
            diagnostic.recent_raw_source_window_probe_mode,
            RecentRawSourceWindowProbeMode::BoundedIndexEdges
        );
        assert_eq!(
            diagnostic.recent_raw_source_cached_state_matches_bounded_probe,
            None
        );
        assert_eq!(
            diagnostic.recent_raw_source_bounded_probe_covered_since,
            None
        );
        assert_eq!(
            diagnostic.recent_raw_source_bounded_probe_covered_through,
            None
        );
        assert_eq!(diagnostic.recent_raw_source_scanned_covered_since, None);
        assert_eq!(diagnostic.recent_raw_source_scanned_covered_through, None);
        assert_eq!(diagnostic.recent_raw_source_scanned_row_count, None);
        assert_eq!(
            diagnostic.recent_raw_source_cached_state_matches_scanned_rows,
            None
        );
        Ok(())
    }

    #[test]
    fn recent_raw_source_window_contract_includes_explicit_fields_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-source-window-contract-fields",
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

        let diagnostic = DiscoveryService::explain_recent_raw_source_window_contract_read_only(
            &fixture.state_root,
        )?;
        assert!(diagnostic.recent_raw_promoted_covered_since.is_some());
        assert!(diagnostic.recent_raw_source_covered_since.is_some());
        assert!(diagnostic.recent_raw_source_window_probe_bounded);
        assert_eq!(
            diagnostic.recent_raw_source_window_probe_mode,
            RecentRawSourceWindowProbeMode::BoundedIndexEdges
        );
        assert!(diagnostic
            .recent_raw_source_window_probe_explanation
            .contains("does not perform a full observed_swaps row-count scan"));
        assert!(diagnostic
            .recent_raw_source_bounded_probe_covered_since
            .is_some());
        assert!(diagnostic
            .recent_raw_source_bounded_probe_covered_through
            .is_some());
        assert_eq!(
            diagnostic.recent_raw_source_cached_state_matches_bounded_probe,
            Some(true)
        );
        assert_eq!(diagnostic.recent_raw_source_scanned_covered_since, None);
        assert_eq!(diagnostic.recent_raw_source_scanned_covered_through, None);
        assert_eq!(diagnostic.recent_raw_source_scanned_row_count, None);
        assert_eq!(
            diagnostic.recent_raw_source_cached_state_matches_scanned_rows,
            None
        );
        assert!(!diagnostic
            .recent_raw_source_window_contract_basis_explanation
            .is_empty());
        Ok(())
    }

    #[test]
    fn recent_raw_source_window_contract_explain_remains_read_only_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-source-window-contract-read-only",
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
        let _ = DiscoveryService::explain_recent_raw_source_window_contract_read_only(
            &fixture.state_root,
        )?;
        let after = fixture.capture_bytes()?;
        assert_eq!(before, after);
        Ok(())
    }
