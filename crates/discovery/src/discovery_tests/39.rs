    #[test]
    fn recent_raw_staged_birth_current_artifact_agree_but_behind_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-staged-birth-agree-behind",
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
        fixture.write_selected_staged_snapshot_sqlite_content(
            &[swap(
                "wallet-raw",
                "sig-staged",
                parse_ts("2026-04-14T07:55:00Z")?,
                SOL_MINT,
                "TokenRaw111111111111111111111111111111111",
                1.0,
                10.0,
            )],
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_staged_birth_read_only(&fixture.state_root)?;

        assert_eq!(
            diagnostic.recent_raw_staged_birth_reason_class,
            RecentRawStagedBirthReasonClass::RecentRawStagedCurrentArtifactManifestAndSqliteContentAgreeButBehind
        );
        assert!(!diagnostic.recent_raw_staged_birth_proven_from_current_artifacts);
        assert_eq!(
            diagnostic.recent_raw_staged_birth_manifest_matches_sqlite_content,
            Some(true)
        );
        assert!(!diagnostic.recent_raw_staged_birth_manifest_sqlite_match_unproven);
        assert_eq!(
            diagnostic.recent_raw_staged_birth_covered_through_relation_to_promoted,
            RecentRawLineageRelation::Behind
        );
        Ok(())
    }

    #[test]
    fn recent_raw_staged_birth_current_artifact_later_start_window_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-staged-birth-reseeded-window",
            SourceStateSeed::SourceAheadOfStaged,
        )?;
        fixture.write_promoted_surface_with_covered_since(
            "latest.sqlite",
            parse_ts("2026-04-14T07:55:00Z")?,
            2,
            parse_ts("2026-04-14T07:57:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;
        fixture.write_staged_surface_with_source_path_and_covered_since(
            &fixture.runtime_db_path,
            parse_ts("2026-04-14T07:56:00Z")?,
            1,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-staged",
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;
        fixture.write_selected_staged_snapshot_sqlite_content(
            &[swap(
                "wallet-raw",
                "sig-staged",
                parse_ts("2026-04-14T07:56:00Z")?,
                SOL_MINT,
                "TokenRaw111111111111111111111111111111111",
                1.0,
                10.0,
            )],
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_staged_birth_read_only(&fixture.state_root)?;

        assert_eq!(
            diagnostic.recent_raw_staged_birth_reason_class,
            RecentRawStagedBirthReasonClass::RecentRawStagedCurrentArtifactLaterStartWindowObserved
        );
        assert!(!diagnostic.recent_raw_staged_birth_proven_from_current_artifacts);
        assert_eq!(
            diagnostic.recent_raw_staged_birth_window_later_start_than_promoted,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_staged_birth_manifest_matches_sqlite_content,
            Some(true)
        );
        Ok(())
    }

    #[test]
    fn recent_raw_staged_birth_manifest_sqlite_mismatch_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-staged-birth-manifest-sqlite-mismatch",
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
            "sig-staged-manifest",
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;
        fixture.write_selected_staged_snapshot_sqlite_content(
            &[
                swap(
                    "wallet-raw",
                    "sig-staged-a",
                    parse_ts("2026-04-14T07:55:00Z")?,
                    SOL_MINT,
                    "TokenRaw111111111111111111111111111111111",
                    1.0,
                    10.0,
                ),
                swap(
                    "wallet-raw",
                    "sig-staged-b",
                    parse_ts("2026-04-14T07:56:00Z")?,
                    SOL_MINT,
                    "TokenRaw111111111111111111111111111111111",
                    1.0,
                    11.0,
                ),
            ],
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_staged_birth_read_only(&fixture.state_root)?;

        assert_eq!(
            diagnostic.recent_raw_staged_birth_reason_class,
            RecentRawStagedBirthReasonClass::RecentRawStagedCurrentArtifactManifestSqliteMismatch
        );
        assert!(!diagnostic.recent_raw_staged_birth_proven_from_current_artifacts);
        assert_eq!(
            diagnostic.recent_raw_staged_birth_manifest_matches_sqlite_content,
            Some(false)
        );
        Ok(())
    }

    #[test]
    fn recent_raw_staged_birth_unproven_case_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-staged-birth-unproven",
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
            DiscoveryService::explain_recent_raw_staged_birth_read_only(&fixture.state_root)?;

        assert_eq!(
            diagnostic.recent_raw_staged_birth_reason_class,
            RecentRawStagedBirthReasonClass::RecentRawStagedCurrentArtifactUnprovenDueToMissingEvidence
        );
        assert!(!diagnostic.recent_raw_staged_birth_observed);
        assert!(!diagnostic.recent_raw_staged_birth_proven_from_current_artifacts);
        assert!(diagnostic.recent_raw_staged_birth_manifest_sqlite_match_unproven);
        Ok(())
    }

    #[test]
    fn recent_raw_staged_birth_includes_birth_identity_fields_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-staged-birth-fields",
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
        fixture.write_selected_staged_snapshot_sqlite_content(
            &[swap(
                "wallet-raw",
                "sig-staged",
                parse_ts("2026-04-14T07:55:00Z")?,
                SOL_MINT,
                "TokenRaw111111111111111111111111111111111",
                1.0,
                10.0,
            )],
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_staged_birth_read_only(&fixture.state_root)?;

        assert!(diagnostic
            .recent_raw_staged_birth_snapshot_path
            .ends_with(".discovery_recent_raw_staged.sqlite.archive-staged"));
        assert!(diagnostic
            .recent_raw_staged_birth_metadata_path
            .ends_with(".discovery_recent_raw_staged.sqlite.archive-staged.json"));
        assert_eq!(
            diagnostic.recent_raw_staged_birth_same_source_db_as_promoted,
            Some(true)
        );
        assert_eq!(diagnostic.recent_raw_staged_birth_sqlite_row_count, Some(1));
        assert!(!diagnostic.recent_raw_staged_birth_proven_from_current_artifacts);
        Ok(())
    }

    #[test]
    fn recent_raw_staged_birth_explain_remains_read_only_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-staged-birth-read-only",
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
        fixture.write_selected_staged_snapshot_sqlite_content(
            &[swap(
                "wallet-raw",
                "sig-staged",
                parse_ts("2026-04-14T07:55:00Z")?,
                SOL_MINT,
                "TokenRaw111111111111111111111111111111111",
                1.0,
                10.0,
            )],
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;
        let before = fixture.capture_bytes()?;

        let _ = DiscoveryService::explain_recent_raw_staged_birth_read_only(&fixture.state_root)?;

        let after = fixture.capture_bytes()?;
        assert_eq!(before, after);
        Ok(())
    }

    #[test]
    fn recent_raw_staged_window_seeding_current_start_matches_promoted_start_stage1() -> Result<()>
    {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-staged-window-seeding-promoted-start",
            SourceStateSeed::Missing,
        )?;
        fixture.rewrite_source_state(
            &[
                swap(
                    "wallet-raw",
                    "sig-source-early",
                    parse_ts("2026-04-14T07:54:00Z")?,
                    SOL_MINT,
                    "TokenRaw111111111111111111111111111111111",
                    1.0,
                    9.0,
                ),
                swap(
                    "wallet-raw",
                    "sig-source-late",
                    parse_ts("2026-04-14T07:56:00Z")?,
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
            1,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;
        fixture.write_selected_staged_surface_sqlite_with_source_path_and_created_at(
            &fixture.runtime_db_path,
            &[swap(
                "wallet-raw",
                "sig-staged",
                parse_ts("2026-04-14T07:55:00Z")?,
                SOL_MINT,
                "TokenRaw111111111111111111111111111111111",
                1.0,
                10.0,
            )],
            parse_ts("2026-04-14T08:01:00Z")?,
            parse_ts("2026-04-14T08:02:00Z")?,
        )?;

        let diagnostic = DiscoveryService::explain_recent_raw_staged_window_seeding_read_only(
            &fixture.state_root,
        )?;
        assert_eq!(
            diagnostic.recent_raw_staged_window_seeding_reason_class,
            RecentRawStagedWindowSeedingReasonClass::RecentRawStagedWindowCurrentStartMatchesPromotedStart
        );
        assert!(diagnostic.recent_raw_staged_window_seeding_observed);
        assert!(
            !diagnostic
                .recent_raw_staged_window_historical_seeding_basis_proven_from_current_artifacts
        );
        assert_eq!(
            diagnostic.recent_raw_staged_start_current_evidence_basis,
            RecentRawStagedWindowSeedingBasis::MatchesPromotedStart
        );
        assert_eq!(
            diagnostic.recent_raw_staged_start_matches_promoted_start,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_staged_start_matches_current_window_cutoff,
            Some(false)
        );
        assert_eq!(
            diagnostic.recent_raw_staged_start_matches_neither_promoted_nor_source,
            Some(false)
        );
        assert_eq!(
            diagnostic.recent_raw_promoted_can_seed_staged_progress_under_current_code,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_staged_manifest_matches_sqlite_content,
            Some(true)
        );
        Ok(())
    }

    #[test]
    fn recent_raw_staged_window_seeding_current_start_matches_source_start_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-staged-window-seeding-later-cutoff",
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
                    12.0,
                ),
            ],
            parse_ts("2026-04-14T08:07:00Z")?,
        )?;
        fixture.write_promoted_surface_with_covered_since(
            "latest.sqlite",
            parse_ts("2026-04-14T07:55:00Z")?,
            1,
            parse_ts("2026-04-14T07:56:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;
        fixture.write_selected_staged_surface_sqlite_with_source_path_and_created_at(
            &fixture.runtime_db_path,
            &[swap(
                "wallet-raw",
                "sig-staged",
                parse_ts("2026-04-14T07:56:00Z")?,
                SOL_MINT,
                "TokenRaw111111111111111111111111111111111",
                1.0,
                10.0,
            )],
            parse_ts("2026-04-14T08:01:00Z")?,
            parse_ts("2026-04-14T08:03:00Z")?,
        )?;

        let diagnostic = DiscoveryService::explain_recent_raw_staged_window_seeding_read_only(
            &fixture.state_root,
        )?;
        assert_eq!(
            diagnostic.recent_raw_staged_window_seeding_reason_class,
            RecentRawStagedWindowSeedingReasonClass::RecentRawStagedWindowCurrentStartMatchesSourceStart
        );
        assert!(diagnostic.recent_raw_staged_window_seeding_observed);
        assert!(
            !diagnostic
                .recent_raw_staged_window_historical_seeding_basis_proven_from_current_artifacts
        );
        assert_eq!(
            diagnostic.recent_raw_staged_start_current_evidence_basis,
            RecentRawStagedWindowSeedingBasis::MatchesCurrentSourceStart
        );
        assert_eq!(
            diagnostic.recent_raw_staged_start_matches_promoted_start,
            Some(false)
        );
        assert_eq!(
            diagnostic.recent_raw_staged_start_matches_current_window_cutoff,
            Some(true)
        );
        assert_eq!(
            diagnostic.recent_raw_staged_start_matches_neither_promoted_nor_source,
            Some(false)
        );
        assert_eq!(
            diagnostic.recent_raw_promoted_can_seed_staged_progress_under_current_code,
            Some(false)
        );
        assert_eq!(
            diagnostic.recent_raw_promoted_seed_blocked_by_source_contract_mismatch,
            Some(true)
        );
        Ok(())
    }
