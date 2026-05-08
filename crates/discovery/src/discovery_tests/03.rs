    fn seed_stage1_replay_noise_fixture(
        store: &SqliteStore,
        config: &DiscoveryConfig,
        now: DateTime<Utc>,
        profitable_pairs: usize,
        non_sol_noise_rows: usize,
    ) -> Result<(DateTime<Utc>, DateTime<Utc>, usize, usize)> {
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = metrics_window_start_for_test(config, now);
        let mut total_rows = 0usize;
        let mut sol_leg_rows = 0usize;

        // Keep the runtime window provably complete for repair/run_cycle tests that resume
        // from a partial replay checkpoint rather than exercising the journal-gap path.
        store.insert_observed_swap(&swap(
            "wallet_replay_window_floor",
            "stage1-replay-window-floor",
            window_start,
            "TokenReplayWindowFloorIn11111111111111111",
            "TokenReplayWindowFloorOut1111111111111111",
            1.0,
            1.0,
        ))?;
        total_rows = total_rows.saturating_add(1);

        for idx in 0..profitable_pairs {
            let ts = window_start + Duration::minutes((idx * 12) as i64 + 1);
            let token = format!("TokenStage1ReplayOpt{idx:04}1111111111111111");
            store.insert_observed_swap(&swap(
                "wallet_replay_top",
                &format!("stage1-replay-opt-buy-{idx}"),
                ts,
                SOL_MINT,
                token.as_str(),
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_replay_top",
                &format!("stage1-replay-opt-sell-{idx}"),
                ts + Duration::minutes(5),
                token.as_str(),
                SOL_MINT,
                100.0,
                1.2,
            ))?;
            total_rows = total_rows.saturating_add(2);
            sol_leg_rows = sol_leg_rows.saturating_add(2);
        }

        for idx in 0..non_sol_noise_rows {
            let ts = window_start
                + Duration::minutes((idx % 240) as i64)
                + Duration::seconds((idx / 240) as i64);
            let token_in = format!("TokenReplayNoiseIn{:03}111111111111111111", idx % 17);
            let token_out = format!("TokenReplayNoiseOut{:03}1111111111111111", idx % 19);
            store.insert_observed_swap(&swap(
                &format!("wallet_replay_noise_{:02}", idx % 11),
                &format!("stage1-replay-opt-noise-{idx}"),
                ts,
                token_in.as_str(),
                token_out.as_str(),
                2.0,
                3.0,
            ))?;
            total_rows = total_rows.saturating_add(1);
        }

        Ok((window_start, metrics_window_start, total_rows, sol_leg_rows))
    }

    fn seed_stage1_partial_sol_leg_replay_checkpoint_fixture(
        store: &SqliteStore,
        discovery: &DiscoveryService,
        config: &DiscoveryConfig,
        now: DateTime<Utc>,
        profitable_pairs: usize,
        seeded_sol_leg_pages: usize,
    ) -> Result<(PersistedStreamRebuildState, DateTime<Utc>, usize)> {
        let (window_start, metrics_window_start) =
            seed_stage1_persisted_stream_runtime_fixture(store, config, now, profitable_pairs, 1)?;
        let sol_leg_rows = profitable_pairs.saturating_mul(4).saturating_add(1);
        let unique_buy_mints = store.load_observed_buy_mints_in_window(window_start, now)?;

        let mut state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        state.phase = DiscoveryPersistedRebuildPhase::Replay;
        state.horizon_end = now;
        state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        state.payload.collect_buy_mints_prepass_complete = true;
        state.payload.unique_buy_mints = unique_buy_mints;
        state.payload.buy_mint_counts = state
            .payload
            .unique_buy_mints
            .iter()
            .cloned()
            .map(|mint| (mint, 1u32))
            .collect();
        state.payload.token_quality_cache = state
            .payload
            .unique_buy_mints
            .iter()
            .cloned()
            .map(|mint| {
                (
                    mint.clone(),
                    quality_cache::TokenQualityResolution::Fresh(
                        copybot_core_types::TokenQualityCacheRow {
                            mint,
                            holders: Some(42),
                            liquidity_sol: Some(3.0),
                            token_age_seconds: Some(3_600),
                            fetched_at: now - Duration::minutes(1),
                        },
                    ),
                )
            })
            .collect();
        state.payload.token_quality_progress.next_mint_index = state.payload.unique_buy_mints.len();

        let stats_advance = discovery.advance_persisted_stream_replay_wallet_stats(
            store,
            &mut state,
            config.max_fetch_swaps_per_cycle,
            10,
            Instant::now() + StdDuration::from_secs(30),
        )?;
        assert!(
            stats_advance.source_exhausted,
            "fixture should fully drain wallet_stats before seeding a partial sol-leg checkpoint"
        );
        state.payload.replay_wallet_stats_rows_processed = state
            .payload
            .replay_wallet_stats_rows_processed
            .saturating_add(stats_advance.replay_wallet_stats_rows_processed);
        state.payload.replay_wallet_stats_pages_processed = state
            .payload
            .replay_wallet_stats_pages_processed
            .saturating_add(stats_advance.replay_wallet_stats_pages_processed);
        state
            .payload
            .replay_wallet_stats_day_count_source_progress
            .merge(stats_advance.replay_wallet_stats_day_count_source_progress);
        state.payload.replay_wallet_stats_complete = true;
        state.payload.replay_wallet_stats_wallet_cursor = None;
        state.phase_cursor = None;
        for acc in state.payload.by_wallet.values_mut() {
            acc.tx_per_minute.clear();
        }

        let replay_advance = discovery
            .advance_persisted_stream_replay_optimized_with_wallet_stats_phase_page_limit(
                store,
                &mut state,
                config.max_fetch_swaps_per_cycle,
                config.max_fetch_pages_per_cycle,
                None,
                Some(seeded_sol_leg_pages.max(1)),
                false,
                Instant::now() + StdDuration::from_secs(30),
            )?;
        assert!(
            !replay_advance.source_exhausted,
            "fixture should leave a live sol-leg cursor behind for the resumed checkpoint"
        );
        assert!(
            matches!(
                replay_advance.budget_exhausted_reason,
                Some(PersistedStreamBudgetExhaustedReason::PageBudget)
            ),
            "fixture should stop on page budget so the resumed checkpoint mirrors the live sol-leg blocker"
        );
        state.replay_rows_processed = state
            .replay_rows_processed
            .saturating_add(replay_advance.rows_processed);
        state.replay_pages_processed = state
            .replay_pages_processed
            .saturating_add(replay_advance.pages_processed);
        state.phase_cursor = replay_advance.phase_cursor;

        Ok((state, metrics_window_start, sol_leg_rows))
    }

    fn seed_stage1_dense_partial_sol_leg_replay_checkpoint_fixture(
        store: &SqliteStore,
        discovery: &DiscoveryService,
        config: &DiscoveryConfig,
        now: DateTime<Utc>,
        profitable_pairs: usize,
        seeded_sol_leg_pages: usize,
    ) -> Result<(PersistedStreamRebuildState, DateTime<Utc>)> {
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = metrics_window_start_for_test(config, now);
        let mut latest_cursor: Option<DiscoveryRuntimeCursor> = None;

        for idx in 0..profitable_pairs {
            let base_ts = window_start + Duration::seconds((idx * 4) as i64);
            let token = format!("TokenStage1DensePersistedTop{idx:04}11111111111111111111");
            let top_buy = swap(
                "wallet_top_dense",
                &format!("stage1-dense-persisted-top-buy-{idx}"),
                base_ts,
                SOL_MINT,
                token.as_str(),
                1.0,
                100.0,
            );
            store.insert_observed_swap(&top_buy)?;
            let top_sell = swap(
                "wallet_top_dense",
                &format!("stage1-dense-persisted-top-sell-{idx}"),
                base_ts + Duration::seconds(1),
                token.as_str(),
                SOL_MINT,
                100.0,
                1.3,
            );
            store.insert_observed_swap(&top_sell)?;
            let noise_buy = swap(
                "wallet_noise_dense",
                &format!("stage1-dense-persisted-noise-buy-{idx}"),
                base_ts + Duration::seconds(2),
                SOL_MINT,
                token.as_str(),
                1.0,
                100.0,
            );
            store.insert_observed_swap(&noise_buy)?;
            let noise_sell = swap(
                "wallet_noise_dense",
                &format!("stage1-dense-persisted-noise-sell-{idx}"),
                base_ts + Duration::seconds(3),
                token.as_str(),
                SOL_MINT,
                100.0,
                0.7,
            );
            latest_cursor = Some(DiscoveryRuntimeCursor {
                ts_utc: noise_sell.ts_utc,
                slot: noise_sell.slot,
                signature: noise_sell.signature.clone(),
            });
            store.insert_observed_swap(&noise_sell)?;
        }
        store.upsert_discovery_runtime_cursor(
            &latest_cursor.expect("latest cursor should be present for dense sol_leg fixture"),
        )?;

        let unique_buy_mints = store.load_observed_buy_mints_in_window(window_start, now)?;
        let mut state =
            discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
        state.phase = DiscoveryPersistedRebuildPhase::Replay;
        state.horizon_end = now;
        state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
        state.payload.collect_buy_mints_prepass_complete = true;
        state.payload.unique_buy_mints = unique_buy_mints;
        state.payload.buy_mint_counts = state
            .payload
            .unique_buy_mints
            .iter()
            .cloned()
            .map(|mint| (mint, 1u32))
            .collect();
        state.payload.token_quality_cache = state
            .payload
            .unique_buy_mints
            .iter()
            .cloned()
            .map(|mint| {
                (
                    mint.clone(),
                    quality_cache::TokenQualityResolution::Fresh(
                        copybot_core_types::TokenQualityCacheRow {
                            mint,
                            holders: Some(42),
                            liquidity_sol: Some(3.0),
                            token_age_seconds: Some(3_600),
                            fetched_at: now - Duration::minutes(1),
                        },
                    ),
                )
            })
            .collect();
        state.payload.token_quality_progress.next_mint_index = state.payload.unique_buy_mints.len();

        let stats_advance = discovery.advance_persisted_stream_replay_wallet_stats(
            store,
            &mut state,
            config.max_fetch_swaps_per_cycle,
            10,
            Instant::now() + StdDuration::from_secs(30),
        )?;
        assert!(
            stats_advance.source_exhausted,
            "dense sol_leg fixture should fully drain wallet_stats before seeding a partial sol-leg checkpoint"
        );
        state.payload.replay_wallet_stats_rows_processed = state
            .payload
            .replay_wallet_stats_rows_processed
            .saturating_add(stats_advance.replay_wallet_stats_rows_processed);
        state.payload.replay_wallet_stats_pages_processed = state
            .payload
            .replay_wallet_stats_pages_processed
            .saturating_add(stats_advance.replay_wallet_stats_pages_processed);
        state
            .payload
            .replay_wallet_stats_day_count_source_progress
            .merge(stats_advance.replay_wallet_stats_day_count_source_progress);
        state.payload.replay_wallet_stats_complete = true;
        state.payload.replay_wallet_stats_wallet_cursor = None;
        state.phase_cursor = None;
        for acc in state.payload.by_wallet.values_mut() {
            acc.tx_per_minute.clear();
        }

        let replay_advance = discovery
            .advance_persisted_stream_replay_optimized_with_wallet_stats_phase_page_limit(
                store,
                &mut state,
                config.max_fetch_swaps_per_cycle,
                config.max_fetch_pages_per_cycle,
                None,
                Some(seeded_sol_leg_pages.max(1)),
                false,
                Instant::now() + StdDuration::from_secs(60),
            )?;
        assert!(
            !replay_advance.source_exhausted,
            "dense sol_leg fixture should leave a live sol-leg cursor behind for the resumed checkpoint"
        );
        assert!(
            matches!(
                replay_advance.budget_exhausted_reason,
                Some(PersistedStreamBudgetExhaustedReason::PageBudget)
            ),
            "dense sol-leg fixture should stop on page budget so the resumed checkpoint mirrors the live downstream blocker"
        );
        state.replay_rows_processed = state
            .replay_rows_processed
            .saturating_add(replay_advance.rows_processed);
        state.replay_pages_processed = state
            .replay_pages_processed
            .saturating_add(replay_advance.pages_processed);
        state.phase_cursor = replay_advance.phase_cursor;

        Ok((state, metrics_window_start))
    }
