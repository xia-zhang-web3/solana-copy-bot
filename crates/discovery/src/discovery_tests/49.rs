fn wallet_entries_and_token_history_from_swaps(
        config: &DiscoveryConfig,
        swaps: &[SwapEvent],
    ) -> (
        Vec<(String, WalletAccumulator)>,
        HashMap<String, Vec<SolLegTrade>>,
    ) {
        let mut by_wallet: HashMap<String, WalletAccumulator> = HashMap::new();
        let mut token_sol_history: HashMap<String, Vec<SolLegTrade>> = HashMap::new();

        for swap in swaps {
            by_wallet
                .entry(swap.wallet.clone())
                .or_default()
                .observe_swap(
                    swap,
                    config.max_tx_per_minute,
                    is_sol_buy(swap).then_some(BuyTradability::Tradable),
                    false,
                );
            if let Some(token) = sol_leg_token(swap) {
                token_sol_history
                    .entry(token.to_string())
                    .or_default()
                    .push(SolLegTrade {
                        ts: swap.ts_utc,
                        wallet_id: swap.wallet.clone(),
                        sol_notional: if is_sol_buy(swap) {
                            swap.amount_in
                        } else {
                            swap.amount_out
                        },
                    });
            }
        }

        for trades in token_sol_history.values_mut() {
            trades.sort_by(|a, b| a.ts.cmp(&b.ts).then_with(|| a.wallet_id.cmp(&b.wallet_id)));
        }

        let mut entries: Vec<(String, WalletAccumulator)> = by_wallet.into_iter().collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        (entries, token_sol_history)
    }

    fn desired_wallets_from_clustered_thin_market_fixture(
        config: DiscoveryConfig,
        swaps: &[SwapEvent],
        now: DateTime<Utc>,
    ) -> (
        Vec<WalletSnapshot>,
        Vec<String>,
        Vec<(String, WalletAccumulator)>,
        HashMap<String, Vec<SolLegTrade>>,
    ) {
        let (entries, token_sol_history) =
            wallet_entries_and_token_history_from_swaps(&config, swaps);
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let snapshots = entries
            .iter()
            .map(|(wallet_id, acc)| {
                discovery.snapshot_from_accumulator(
                    wallet_id.clone(),
                    acc.clone(),
                    now,
                    &token_sol_history,
                )
            })
            .collect::<Vec<_>>();
        let ranked = rank_follow_candidates(&snapshots, config.min_score);
        let desired = desired_wallets(&ranked, config.follow_top_n);
        (snapshots, desired, entries, token_sol_history)
    }

    fn seed_stage1_persisted_stream_runtime_fixture(
        store: &SqliteStore,
        config: &DiscoveryConfig,
        now: DateTime<Utc>,
        profitable_pairs: usize,
        tail_noise_rows: usize,
    ) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = metrics_window_start_for_test(config, now);

        for idx in 0..profitable_pairs {
            let offset = Duration::minutes((idx * 20) as i64);
            let token = format!("TokenStage1PersistedTop{idx:02}111111111111111111111");
            store.insert_observed_swap(&swap(
                "wallet_top",
                &format!("stage1-persisted-top-buy-{idx}"),
                window_start + offset,
                SOL_MINT,
                token.as_str(),
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_top",
                &format!("stage1-persisted-top-sell-{idx}"),
                window_start + offset + Duration::minutes(5),
                token.as_str(),
                SOL_MINT,
                100.0,
                1.3,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_noise",
                &format!("stage1-persisted-noise-early-buy-{idx}"),
                window_start + offset,
                SOL_MINT,
                token.as_str(),
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_noise",
                &format!("stage1-persisted-noise-early-sell-{idx}"),
                window_start + offset + Duration::minutes(5),
                token.as_str(),
                SOL_MINT,
                100.0,
                0.7,
            ))?;
        }

        let mut latest_cursor: Option<DiscoveryRuntimeCursor> = None;
        for idx in 0..tail_noise_rows {
            let ts =
                now - Duration::minutes(tail_noise_rows as i64) + Duration::minutes(idx as i64);
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

        Ok((window_start, metrics_window_start))
    }

    fn load_persisted_stream_rebuild_state_for_test(
        store: &SqliteStore,
    ) -> Result<PersistedStreamRebuildState> {
        let row = store
            .load_discovery_persisted_rebuild_state()?
            .expect("persisted rebuild state should exist");
        DiscoveryService::persisted_stream_rebuild_state_from_row(row)
    }

    struct ReplayResumeExactTargetSurfaceWalletPageLimitOverrideGuard {
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl Drop for ReplayResumeExactTargetSurfaceWalletPageLimitOverrideGuard {
        fn drop(&mut self) {
            REPLAY_RESUME_EXACT_TARGET_SURFACE_TEST_WALLET_PAGE_LIMIT
                .store(0, AtomicOrdering::Relaxed);
        }
    }

    fn force_replay_resume_exact_target_surface_wallet_page_limit_for_test(
        limit: usize,
    ) -> ReplayResumeExactTargetSurfaceWalletPageLimitOverrideGuard {
        let lock = REPLAY_RESUME_EXACT_TARGET_SURFACE_TEST_WALLET_PAGE_LIMIT_LOCK
            .lock()
            .expect("resume exact target-surface test override lock should not be poisoned");
        REPLAY_RESUME_EXACT_TARGET_SURFACE_TEST_WALLET_PAGE_LIMIT
            .store(limit, AtomicOrdering::Relaxed);
        ReplayResumeExactTargetSurfaceWalletPageLimitOverrideGuard { _lock: lock }
    }

    fn fresh_token_quality_cache_for_mints_for_test(
        mints: &[String],
        fetched_at: DateTime<Utc>,
    ) -> HashMap<String, quality_cache::TokenQualityResolution> {
        mints
            .iter()
            .cloned()
            .map(|mint| {
                let row = copybot_core_types::TokenQualityCacheRow {
                    mint: mint.clone(),
                    holders: Some(42),
                    liquidity_sol: Some(3.0),
                    token_age_seconds: Some(3_600),
                    fetched_at,
                };
                (mint, quality_cache::TokenQualityResolution::Fresh(row))
            })
            .collect()
    }

    fn seed_fresh_token_quality_cache_rows_for_window_for_test(
        store: &SqliteStore,
        window_start: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<Vec<String>> {
        let mut unique_buy_mints = store.load_observed_buy_mints_in_window(window_start, now)?;
        DiscoveryService::canonicalize_unique_buy_mints(&mut unique_buy_mints);
        for mint in &unique_buy_mints {
            store.upsert_token_quality_cache(mint, Some(42), Some(3.0), Some(3_600), now)?;
        }
        Ok(unique_buy_mints)
    }

    fn seed_fresh_token_quality_cache_rows_for_mints_for_test(
        store: &SqliteStore,
        mints: &[String],
        now: DateTime<Utc>,
    ) -> Result<()> {
        for mint in mints {
            store.upsert_token_quality_cache(mint, Some(42), Some(3.0), Some(3_600), now)?;
        }
        Ok(())
    }

    fn advance_carried_target_until_replay_or_upstream_block_for_test(
        discovery: &DiscoveryService,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        prepass_fetch_limit: usize,
        prepass_page_limit: usize,
        quality_fetch_limit: usize,
        quality_page_limit: usize,
    ) -> Result<()> {
        for _ in 0..8 {
            match state.phase {
                DiscoveryPersistedRebuildPhase::CollectBuyMints => {
                    let advance = discovery.advance_persisted_stream_prepass(
                        store,
                        state,
                        prepass_fetch_limit,
                        prepass_page_limit,
                        Some(prepass_page_limit),
                        Instant::now() + StdDuration::from_secs(5),
                    )?;
                    if !advance.source_exhausted {
                        break;
                    }
                    if DiscoveryService::payload_has_exact_buy_mint_membership(&state.payload) {
                        DiscoveryService::sync_unique_buy_mints_from_counts(&mut state.payload);
                    }
                    DiscoveryService::canonicalize_unique_buy_mints(
                        &mut state.payload.unique_buy_mints,
                    );
                    let transition_now = state.horizon_end;
                    DiscoveryService::transition_persisted_stream_from_collect_buy_mints_to_token_quality(
                        state,
                        transition_now,
                    );
                }
                DiscoveryPersistedRebuildPhase::ResolveTokenQuality => {
                    let advance = discovery.advance_persisted_stream_token_quality(
                        store,
                        state,
                        quality_fetch_limit,
                        quality_page_limit,
                        Instant::now() + StdDuration::from_secs(5),
                    )?;
                    if !advance.source_exhausted {
                        break;
                    }
                    DiscoveryService::transition_persisted_stream_from_token_quality_to_replay(
                        state,
                        discovery.config.min_buy_count > 0,
                    );
                }
                DiscoveryPersistedRebuildPhase::Replay
                | DiscoveryPersistedRebuildPhase::PublishPending => break,
            }
        }
        Ok(())
    }
