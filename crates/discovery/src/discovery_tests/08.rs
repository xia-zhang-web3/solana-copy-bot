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
