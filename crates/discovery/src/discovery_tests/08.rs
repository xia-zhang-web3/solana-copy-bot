    #[test]
    fn cold_start_truncated_in_memory_with_complete_persisted_observed_swaps_publishes_healthy_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-cold-start-persisted-stream-healthy.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = stage1_runtime_config();
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = {
            let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
            let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
            let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
            bucketed_now - Duration::days(config.scoring_window_days.max(1) as i64)
        };

        for idx in 0..6 {
            let offset = Duration::minutes((idx * 20) as i64);
            store.insert_observed_swap(&swap(
                "wallet_top",
                &format!("stage1-persisted-top-buy-{idx}"),
                window_start + offset,
                SOL_MINT,
                "TokenStage1PersistedTop111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_top",
                &format!("stage1-persisted-top-sell-{idx}"),
                window_start + offset + Duration::minutes(5),
                "TokenStage1PersistedTop111111111111111111111",
                SOL_MINT,
                100.0,
                1.3,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_noise",
                &format!("stage1-persisted-noise-early-buy-{idx}"),
                window_start + offset,
                SOL_MINT,
                "TokenStage1PersistedTop111111111111111111111",
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_noise",
                &format!("stage1-persisted-noise-early-sell-{idx}"),
                window_start + offset + Duration::minutes(5),
                "TokenStage1PersistedTop111111111111111111111",
                SOL_MINT,
                100.0,
                0.7,
            ))?;
        }

        let mut latest_cursor: Option<DiscoveryRuntimeCursor> = None;
        for idx in 0..9 {
            let ts = now - Duration::minutes(9) + Duration::minutes(idx as i64);
            let swap = swap(
                "wallet_tail_noise",
                &format!("stage1-persisted-tail-noise-{idx}"),
                ts,
                SOL_MINT,
                "TokenStage1PersistedNoise1111111111111111",
                0.2,
                20.0,
            );
            latest_cursor = Some(DiscoveryRuntimeCursor {
                ts_utc: swap.ts_utc,
                slot: swap.slot,
                signature: swap.signature.clone(),
            });
            store.insert_observed_swap(&swap)?;
        }
        store.upsert_discovery_runtime_cursor(
            &latest_cursor.expect("latest cursor should be present"),
        )?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let summary = discovery.run_cycle(&store, now)?;

        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::Healthy);
        assert_eq!(summary.scoring_source, "raw_window_persisted_stream");
        assert!(!summary.trusted_selection_fail_closed);
        assert!(
            summary.raw_window_cap_truncated,
            "persisted-stream runtime path should still expose that the in-memory raw cache was truncated"
        );
        assert!(summary.eligible_wallets >= 1);
        assert!(summary.follow_promoted >= 1);
        assert!(
            summary.metrics_written > 0,
            "healthy persisted-stream recompute should persist the snapshot bucket"
        );
        assert_eq!(
            store.latest_wallet_metrics_window_start()?,
            Some(metrics_window_start)
        );
        let active_wallets = store.list_active_follow_wallets()?;
        assert_eq!(active_wallets, HashSet::from([String::from("wallet_top")]));
        Ok(())
    }

    #[test]
    fn persisted_stream_rebuild_is_bounded_and_observable_without_recent_published_universe_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-bounded-persisted-stream-observable.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        let (window_start, metrics_window_start) =
            seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 6, 9)?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let summary = discovery.run_cycle(&store, now)?;

        assert_eq!(summary.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        assert_eq!(
            summary.scoring_source,
            "raw_window_incomplete_no_recent_published_universe"
        );
        assert_eq!(summary.active_follow_wallets, 0);

        let rebuild = load_persisted_stream_rebuild_state_for_test(&store)?;
        assert_eq!(
            rebuild.phase,
            DiscoveryPersistedRebuildPhase::CollectBuyMints
        );
        assert_eq!(rebuild.window_start, window_start);
        assert_eq!(rebuild.metrics_window_start, metrics_window_start);
        assert_eq!(rebuild.horizon_end, now);
        assert_eq!(rebuild.prepass_rows_processed, 5);
        assert_eq!(rebuild.prepass_pages_processed, 1);
        assert_eq!(rebuild.replay_rows_processed, 0);
        assert_eq!(rebuild.replay_pages_processed, 0);
        assert_eq!(rebuild.chunks_completed, 1);
        assert!(
            rebuild.payload.collect_buy_mints_cursor_token.is_some(),
            "bounded collect_buy_mints must persist its own resumable mint cursor"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_collect_buy_mints_reduces_total_work_on_large_noise_fixture_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-collect-buy-mints-large-noise-throughput.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let window_start =
            seed_stage1_collect_buy_mints_noise_fixture(&store, &config, now, 7, 500)?;
        let metrics_window_start = metrics_window_start_for_test(&config, now);
        let mut state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);

        let phase_advance = discovery.advance_persisted_stream_prepass(
            &store,
            &mut state,
            20,
            1,
            None,
            Instant::now() + StdDuration::from_secs(1),
        )?;

        assert!(phase_advance.source_exhausted);
        assert_eq!(phase_advance.rows_processed, 7);
        assert_eq!(phase_advance.pages_processed, 1);
        assert_eq!(phase_advance.unique_buy_mints_discovered, 7);
        assert_eq!(state.payload.unique_buy_mints.len(), 7);
        assert!(
            phase_advance.collect_buy_mints_cursor_token.is_none(),
            "completed direct distinct-mint prepass should clear the mint cursor"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_collect_buy_mints_fresh_scan_caps_grouped_query_width_under_live_high_limit_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-collect-buy-mints-fresh-scan-batch-cap.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-02T09:30:03Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = bounded_stage1_runtime_config();
        config.max_fetch_swaps_per_cycle = 20_000;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 15_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = metrics_window_start_for_test(&config, now);

        for idx in 0..6_000usize {
            store.insert_observed_swap(&swap(
                "wallet_collect_buy_mints_cap",
                &format!("stage1-collect-buy-mints-cap-{idx:05}"),
                window_start + Duration::seconds(idx as i64),
                SOL_MINT,
                &format!("TokenStage1CollectBuyMintsCap{idx:05}"),
                0.2,
                20.0,
            ))?;
        }

        let mut state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        arm_test_force_collect_buy_mints_fresh_scan_row_limit(
            COLLECT_BUY_MINTS_FRESH_SCAN_BATCH_CAP,
        );
        let phase_advance = discovery.advance_persisted_stream_prepass(
            &store,
            &mut state,
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            None,
            Instant::now() + StdDuration::from_secs(5),
        )?;

        assert!(!phase_advance.source_exhausted);
        assert_eq!(
            phase_advance.pages_processed,
            config.max_fetch_pages_per_cycle,
            "fresh-scan recovery should consume its bounded cycle on multiple capped grouped-mint pages instead of one giant interrupted query"
        );
        assert_eq!(
            phase_advance.rows_processed,
            COLLECT_BUY_MINTS_FRESH_SCAN_BATCH_CAP * config.max_fetch_pages_per_cycle,
            "the effective grouped-mint batch width should be capped independently from the much larger raw swap fetch limit"
        );
        assert_eq!(
            phase_advance.unique_buy_mints_discovered, phase_advance.rows_processed,
            "one-shot distinct mint pages should discover one exact buy mint per grouped row"
        );
        assert!(matches!(
            phase_advance.budget_exhausted_reason,
            Some(PersistedStreamBudgetExhaustedReason::PageBudget)
        ));
        assert_eq!(
            state.payload.unique_buy_mints.len(),
            phase_advance.rows_processed,
            "bounded fresh-scan progress must persist every capped grouped-mint page instead of re-reading from zero"
        );
        assert!(
            phase_advance.collect_buy_mints_cursor_token.is_some(),
            "partial capped fresh-scan progress must persist the exact next mint cursor"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_collect_buy_mints_repair_phase_page_limit_completes_large_fresh_scan_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-collect-buy-mints-repair-page-limit.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-02T09:30:03Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let mut config = bounded_stage1_runtime_config();
        config.max_fetch_swaps_per_cycle = 20_000;
        config.max_fetch_pages_per_cycle = 5;
        config.fetch_time_budget_ms = 15_000;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = metrics_window_start_for_test(&config, now);

        for idx in 0..6_000usize {
            store.insert_observed_swap(&swap(
                "wallet_collect_buy_mints_repair",
                &format!("stage1-collect-buy-mints-repair-{idx:05}"),
                window_start + Duration::seconds(idx as i64),
                SOL_MINT,
                &format!("TokenStage1CollectBuyMintsRepair{idx:05}"),
                0.2,
                20.0,
            ))?;
        }

        let mut baseline_state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        let baseline = discovery.advance_persisted_stream_prepass(
            &store,
            &mut baseline_state,
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            None,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        assert!(!baseline.source_exhausted);
        assert_eq!(
            baseline.pages_processed,
            config.max_fetch_pages_per_cycle,
            "without a widened repair contract, the large live-like fresh scan should still stop on the normal page ceiling"
        );

        let mut repair_state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        let repair_page_limit = discovery.collect_buy_mints_repair_phase_page_limit(
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            StdDuration::from_secs(60),
        );
        let repaired = discovery.advance_persisted_stream_prepass(
            &store,
            &mut repair_state,
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            Some(repair_page_limit),
            Instant::now() + StdDuration::from_secs(5),
        )?;

        assert!(repaired.source_exhausted);
        assert_eq!(
            repaired.rows_processed, 6_000,
            "runtime-window-complete repair should be able to drain the bounded collect_buy_mints backlog instead of leaving it stuck on the first few grouped pages"
        );
        assert!(
            repaired.pages_processed > config.max_fetch_pages_per_cycle,
            "the widened repair phase budget must allow more collect_buy_mints pages than the normal cycle ceiling"
        );
        assert!(
            repaired.collect_buy_mints_cursor_token.is_none(),
            "once the widened repair contract drains fresh_scan, the grouped mint cursor should clear instead of persisting a partial prefix"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_collect_buy_mints_carry_forward_reconcile_reduces_work_on_large_noise_fixture_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-collect-buy-mints-carry-forward-noise-throughput.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = bounded_stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 60;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let checkpoint_now = DateTime::parse_from_rfc3339("2026-03-17T12:00:50Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let rollover_now = checkpoint_now + Duration::seconds(20);
        let window_start =
            checkpoint_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = metrics_window_start_for_test(&config, checkpoint_now);
        let next_window_start =
            rollover_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let next_metrics_window_start = metrics_window_start_for_test(&config, rollover_now);

        let token_old_only = "TokenStage1CarryThroughputOldOnly111111111".to_string();
        let token_survives = "TokenStage1CarryThroughputSurvives11111".to_string();
        let token_new_tail_a = "TokenStage1CarryThroughputNewTailA1111".to_string();
        let token_new_tail_b = "TokenStage1CarryThroughputNewTailB1111".to_string();

        for (idx, (token, ts)) in [
            (token_old_only.as_str(), window_start + Duration::seconds(5)),
            (
                token_survives.as_str(),
                next_window_start + Duration::seconds(5),
            ),
            (
                token_new_tail_a.as_str(),
                checkpoint_now + Duration::seconds(5),
            ),
            (
                token_new_tail_b.as_str(),
                checkpoint_now + Duration::seconds(6),
            ),
        ]
        .into_iter()
        .enumerate()
        {
            store.insert_observed_swap(&swap(
                "wallet_carry_throughput",
                &format!("stage1-carry-throughput-buy-{idx}"),
                ts,
                SOL_MINT,
                token,
                0.5,
                50.0,
            ))?;
        }

        let expired_head_noise_rows = 2_000usize;
        for idx in 0..expired_head_noise_rows {
            let ts = window_start + Duration::milliseconds((idx % 20_000) as i64);
            store.insert_observed_swap(&swap(
                "wallet_expired_head_noise",
                &format!("stage1-expired-head-noise-{idx}"),
                ts,
                &format!("NoiseExpiredHeadToken{idx:05}111111111111"),
                SOL_MINT,
                100.0,
                0.01,
            ))?;
        }
        let new_tail_noise_rows = 2_000usize;
        for idx in 0..new_tail_noise_rows {
            let ts = checkpoint_now + Duration::milliseconds((idx % 19_000) as i64 + 1);
            store.insert_observed_swap(&swap(
                "wallet_new_tail_noise",
                &format!("stage1-new-tail-noise-{idx}"),
                ts,
                &format!("NoiseNewTailToken{idx:05}11111111111111"),
                SOL_MINT,
                100.0,
                0.02,
            ))?;
        }

        let mut stale_state = discovery.start_persisted_stream_rebuild_state(
            window_start,
            metrics_window_start,
            checkpoint_now,
        );
        stale_state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        stale_state.prepass_rows_processed = 2;
        stale_state.prepass_pages_processed = 1;
        stale_state.payload.unique_buy_mints = vec![token_old_only.clone(), token_survives.clone()];
        stale_state.payload.buy_mint_counts =
            BTreeMap::from([(token_old_only.clone(), 1), (token_survives.clone(), 1)]);
        stale_state.payload.collect_buy_mints_prepass_complete = true;
        stale_state.payload.collect_buy_mints_mode = CollectBuyMintsMode::FreshScan;
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&stale_state, stale_state.horizon_end)?,
        )?;

        let (mut carried, restore_outcome) = discovery
            .load_or_start_persisted_stream_rebuild_state(
                &store,
                next_window_start,
                next_metrics_window_start,
                rollover_now,
            )?;
        assert_eq!(
            restore_outcome,
            PersistedStreamRebuildRestoreOutcome::CarriedForwardMetricsWindow
        );
        assert_eq!(
            carried.payload.collect_buy_mints_mode,
            CollectBuyMintsMode::ReconcileExpiredHead
        );

        let phase_advance = discovery.advance_persisted_stream_prepass(
            &store,
            &mut carried,
            10,
            10,
            None,
            Instant::now() + StdDuration::from_secs(1),
        )?;

        assert!(
            phase_advance.source_exhausted,
            "grouped delta reconcile should finish the carry-forward collect_buy_mints phase in bounded pages on a large noisy fixture"
        );
        assert_eq!(phase_advance.pages_processed, 2);
        assert_eq!(phase_advance.rows_processed, 3);
        assert_eq!(phase_advance.unique_buy_mints_discovered, 2);
        assert!(
            phase_advance.rows_processed
                < expired_head_noise_rows.saturating_add(new_tail_noise_rows),
            "carry-forward grouped reconcile should process buy-mint delta groups, not every raw swap in the expired/new tail windows"
        );
        assert_eq!(
            carried.payload.unique_buy_mints,
            vec![token_new_tail_a, token_new_tail_b, token_survives]
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_rebuild_resumes_across_cycles_stage1() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-bounded-persisted-stream-resume.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-17T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let config = bounded_stage1_runtime_config();
        seed_stage1_persisted_stream_runtime_fixture(&store, &config, now, 6, 9)?;

        let discovery = DiscoveryService::new(config, permissive_shadow_quality());
        let first_summary = discovery.run_cycle(&store, now)?;
        assert_eq!(first_summary.runtime_mode, DiscoveryRuntimeMode::FailClosed);
        let first_progress = store
            .load_discovery_persisted_rebuild_state()?
            .expect("first cycle should persist bounded rebuild progress");

        let second_summary = discovery.run_cycle(&store, now + Duration::minutes(1))?;
        assert_eq!(
            second_summary.runtime_mode,
            DiscoveryRuntimeMode::FailClosed
        );
        let second_progress = store
            .load_discovery_persisted_rebuild_state()?
            .expect("second cycle should keep persisted rebuild progress");

        assert_eq!(second_progress.window_start, first_progress.window_start);
        assert_eq!(second_progress.horizon_end, first_progress.horizon_end);
        assert_eq!(
            second_progress.metrics_window_start,
            first_progress.metrics_window_start
        );
        assert!(
            second_progress.prepass_rows_processed > first_progress.prepass_rows_processed,
            "next cycle must advance the bounded prepass instead of restarting from zero"
        );
        assert_eq!(
            second_progress.chunks_completed,
            first_progress.chunks_completed + 1
        );
        Ok(())
    }
