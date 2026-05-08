#[test]
    fn persisted_stream_catch_up_request_targets_collect_buy_mints_and_replay_phases() {
        assert!(should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::CollectBuyMints,
                CollectBuyMintsMode::ReconcileExpiredHead,
                false,
                Some(PersistedStreamBudgetExhaustedReason::PageBudget),
            )
        ));
        assert!(should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::CollectBuyMints,
                CollectBuyMintsMode::ReconcileNewTail,
                false,
                Some(PersistedStreamBudgetExhaustedReason::PageBudget),
            )
        ));
        assert!(should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::CollectBuyMints,
                CollectBuyMintsMode::FreshScan,
                false,
                Some(PersistedStreamBudgetExhaustedReason::TimeBudget),
            )
        ));
        assert!(!should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::CollectBuyMints,
                CollectBuyMintsMode::ReconcileExpiredHead,
                false,
                Some(PersistedStreamBudgetExhaustedReason::TimeBudget),
            )
        ));
        assert!(!should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::CollectBuyMints,
                CollectBuyMintsMode::ReconcileNewTail,
                false,
                Some(PersistedStreamBudgetExhaustedReason::TimeBudget),
            )
        ));
        assert!(should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::Replay,
                CollectBuyMintsMode::FreshScan,
                false,
                Some(PersistedStreamBudgetExhaustedReason::TimeBudget),
            )
        ));
        assert!(should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::Replay,
                CollectBuyMintsMode::FreshScan,
                false,
                Some(PersistedStreamBudgetExhaustedReason::PageBudget),
            )
        ));
        assert!(!should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::Replay,
                CollectBuyMintsMode::FreshScan,
                true,
                None,
            )
        ));
        assert!(should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::Replay,
                CollectBuyMintsMode::FreshScan,
                true,
                Some(PersistedStreamBudgetExhaustedReason::PageBudget),
            )
        ));
        assert!(!should_request_persisted_stream_catch_up(
            &catch_up_test_telemetry(
                DiscoveryPersistedRebuildPhase::Replay,
                CollectBuyMintsMode::FreshScan,
                false,
                None,
            )
        ));
    }

    #[test]
    fn persisted_stream_catch_up_pressure_override_targets_priority_recovery_phases() {
        let mut replay_sol_leg = catch_up_test_telemetry(
            DiscoveryPersistedRebuildPhase::Replay,
            CollectBuyMintsMode::FreshScan,
            true,
            Some(PersistedStreamBudgetExhaustedReason::PageBudget),
        );
        replay_sol_leg.replay_subphase = Some("sol_leg");
        assert!(should_request_persisted_stream_catch_up_pressure_override(
            &replay_sol_leg
        ));

        let mut replay_wallet_stats = catch_up_test_telemetry(
            DiscoveryPersistedRebuildPhase::Replay,
            CollectBuyMintsMode::FreshScan,
            false,
            Some(PersistedStreamBudgetExhaustedReason::PageBudget),
        );
        replay_wallet_stats.replay_subphase = Some("wallet_stats");
        assert!(!should_request_persisted_stream_catch_up_pressure_override(
            &replay_wallet_stats
        ));

        replay_wallet_stats.budget_exhausted_reason =
            Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
        replay_wallet_stats.replay_wallet_stats_wallet_cursor =
            Some("wallet-stats-cursor".to_string());
        replay_wallet_stats.cycle_pages_processed = 54;
        replay_wallet_stats.cycle_rows_processed = 1_094_540;
        replay_wallet_stats.wallets_buffered = 500_400;
        assert!(should_request_persisted_stream_catch_up_pressure_override(
            &replay_wallet_stats
        ));

        replay_wallet_stats.replay_wallet_stats_complete = true;
        assert!(!should_request_persisted_stream_catch_up_pressure_override(
            &replay_wallet_stats
        ));

        let mut collect_buy_mints = catch_up_test_telemetry(
            DiscoveryPersistedRebuildPhase::CollectBuyMints,
            CollectBuyMintsMode::FreshScan,
            false,
            Some(PersistedStreamBudgetExhaustedReason::TimeBudget),
        );
        collect_buy_mints.collect_buy_mints_cursor_token = Some("fresh-scan-cursor".to_string());
        collect_buy_mints.cycle_pages_processed = 2;
        collect_buy_mints.cycle_rows_processed = 518;
        collect_buy_mints.cycle_unique_buy_mints_discovered = 518;
        assert!(should_request_persisted_stream_catch_up_pressure_override(
            &collect_buy_mints
        ));

        collect_buy_mints.cycle_unique_buy_mints_discovered = 0;
        assert!(!should_request_persisted_stream_catch_up_pressure_override(
            &collect_buy_mints
        ));

        let collect_buy_mints_page_budget = catch_up_test_telemetry(
            DiscoveryPersistedRebuildPhase::CollectBuyMints,
            CollectBuyMintsMode::FreshScan,
            false,
            Some(PersistedStreamBudgetExhaustedReason::PageBudget),
        );
        assert!(!should_request_persisted_stream_catch_up_pressure_override(
            &collect_buy_mints_page_budget
        ));

        let collect_buy_mints_reconcile = catch_up_test_telemetry(
            DiscoveryPersistedRebuildPhase::CollectBuyMints,
            CollectBuyMintsMode::ReconcileExpiredHead,
            false,
            Some(PersistedStreamBudgetExhaustedReason::TimeBudget),
        );
        assert!(!should_request_persisted_stream_catch_up_pressure_override(
            &collect_buy_mints_reconcile
        ));
    }

    fn aggregate_write_config(config: &DiscoveryConfig) -> DiscoveryAggregateWriteConfig {
        DiscoveryAggregateWriteConfig {
            max_tx_per_minute: config.max_tx_per_minute,
            rug_lookahead_seconds: config.rug_lookahead_seconds as u32,
            helius_http_url: None,
            min_token_age_hint_seconds: None,
        }
    }

    fn insert_shadow_recorded_signal(
        store: &SqliteStore,
        wallet_id: &str,
        token: &str,
        ts: DateTime<Utc>,
        signal_id: &str,
    ) -> Result<()> {
        store.insert_copy_signal(&CopySignalRow {
            signal_id: signal_id.to_string(),
            wallet_id: wallet_id.to_string(),
            side: "buy".to_string(),
            token: token.to_string(),
            notional_sol: 0.2,
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
            ts,
            status: "shadow_recorded".to_string(),
        })?;
        Ok(())
    }

    fn insert_recent_tail_noise_swaps(
        store: &SqliteStore,
        start: DateTime<Utc>,
        count: usize,
        signature_prefix: &str,
    ) -> Result<()> {
        for idx in 0..count {
            let ts = start + Duration::minutes(idx as i64);
            store.insert_observed_swap(&swap(
                "wallet_tail_noise",
                &format!("{signature_prefix}-{idx}"),
                ts,
                SOL_MINT,
                "TokenStage3TailNoise1111111111111111111111",
                0.2,
                20.0,
            ))?;
        }
        Ok(())
    }

    #[test]
    fn recent_raw_promotion_ready_case_reports_ready_now_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-promotion-ready",
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

        assert_eq!(
            diagnostic.recent_raw_promotion_reason_class,
            RecentRawPromotionBlockerReasonClass::RecentRawPromotionReadyNow
        );
        assert!(diagnostic.recent_raw_promotion_ready_now);
        assert_eq!(diagnostic.recent_raw_staged_newer_than_promoted, Some(true));
        assert_eq!(diagnostic.recent_raw_source_outruns_staged, Some(false));
        assert!(diagnostic.recent_raw_stage3_truth_blocked_by_promotion);
        assert!(!diagnostic.recent_raw_stage3_current_fresh_healthy_evidence_possible);
        Ok(())
    }

    #[test]
    fn recent_raw_promotion_missing_staged_snapshot_reports_blocker_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-promotion-missing-staged",
            SourceStateSeed::StagedCurrent,
        )?;
        fixture.write_promoted_surface(
            "latest.sqlite",
            1,
            parse_ts("2026-04-14T07:55:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:00:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_promotion_blocker_read_only(&fixture.state_root)?;

        assert_eq!(
            diagnostic.recent_raw_promotion_reason_class,
            RecentRawPromotionBlockerReasonClass::RecentRawPromotionBlockedByMissingStagedSnapshot
        );
        assert!(diagnostic.recent_raw_promotion_blocker_observed);
        assert!(!diagnostic.recent_raw_staged_exists);
        assert_eq!(diagnostic.recent_raw_source_outruns_promoted, Some(true));
        Ok(())
    }

    #[test]
    fn recent_raw_promotion_incomplete_staged_coverage_reports_blocker_stage1() -> Result<()> {
        let fixture = make_recent_raw_promotion_fixture(
            "recent-raw-promotion-incomplete-staged",
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
            1,
            parse_ts("2026-04-14T07:55:00Z")?,
            "sig-promoted",
            parse_ts("2026-04-14T08:01:00Z")?,
        )?;

        let diagnostic =
            DiscoveryService::explain_recent_raw_promotion_blocker_read_only(&fixture.state_root)?;

        assert_eq!(
            diagnostic.recent_raw_promotion_reason_class,
            RecentRawPromotionBlockerReasonClass::RecentRawPromotionBlockedByIncompleteStagedCoverage
        );
        assert_eq!(diagnostic.recent_raw_source_outruns_staged, Some(true));
        assert!(!diagnostic.recent_raw_promotion_ready_now);
        Ok(())
    }
