    fn clustered_thin_market_fixture_swaps(
        now: DateTime<Utc>,
        include_independent_wallets: bool,
    ) -> (Vec<SwapEvent>, Vec<String>, Vec<String>) {
        let clustered_wallet_ids: Vec<String> = (0..9)
            .map(|idx| format!("wallet_clustered_{idx:02}"))
            .collect();
        let independent_wallet_ids: Vec<String> = if include_independent_wallets {
            (0..3)
                .map(|idx| format!("wallet_independent_{idx:02}"))
                .collect()
        } else {
            Vec::new()
        };
        let clustered_token = "TokenClusteredVdor111111111111111111111111";
        let phase_bases = [
            now - Duration::days(2),
            now - Duration::days(1),
            now - Duration::hours(2),
        ];
        let rounds_per_phase = 4;
        let mut swaps = Vec::new();

        for (phase_idx, phase_base) in phase_bases.into_iter().enumerate() {
            for round in 0..rounds_per_phase {
                let round_offset_minutes = (round * 20) as i64;
                for (wallet_idx, wallet_id) in clustered_wallet_ids.iter().enumerate() {
                    let buy_ts =
                        phase_base + Duration::minutes(round_offset_minutes + wallet_idx as i64);
                    let sell_ts = buy_ts + Duration::minutes(5);
                    swaps.push(swap(
                        wallet_id,
                        &format!("clustered-{phase_idx}-{round}-{wallet_idx}-buy"),
                        buy_ts,
                        SOL_MINT,
                        clustered_token,
                        1.0,
                        100.0,
                    ));
                    swaps.push(swap(
                        wallet_id,
                        &format!("clustered-{phase_idx}-{round}-{wallet_idx}-sell"),
                        sell_ts,
                        clustered_token,
                        SOL_MINT,
                        100.0,
                        1.3,
                    ));
                }

                for (wallet_idx, wallet_id) in independent_wallet_ids.iter().enumerate() {
                    let token = format!("TokenIndependent{wallet_idx:02}11111111111111111111");
                    let buy_ts = phase_base
                        + Duration::minutes(round_offset_minutes + 12 + wallet_idx as i64);
                    let sell_ts = buy_ts + Duration::minutes(5);
                    swaps.push(swap(
                        wallet_id,
                        &format!("independent-{phase_idx}-{round}-{wallet_idx}-buy"),
                        buy_ts,
                        SOL_MINT,
                        token.as_str(),
                        1.0,
                        100.0,
                    ));
                    swaps.push(swap(
                        wallet_id,
                        &format!("independent-{phase_idx}-{round}-{wallet_idx}-sell"),
                        sell_ts,
                        token.as_str(),
                        SOL_MINT,
                        100.0,
                        1.25,
                    ));
                    for noise_idx in 0..10 {
                        swaps.push(swap(
                            &format!(
                                "wallet_independent_noise_{phase_idx:02}_{round:02}_{wallet_idx:02}_{noise_idx:02}"
                            ),
                            &format!(
                                "independent-noise-{phase_idx}-{round}-{wallet_idx}-{noise_idx}"
                            ),
                            buy_ts + Duration::seconds(30 + noise_idx as i64),
                            SOL_MINT,
                            token.as_str(),
                            0.4,
                            40.0,
                        ));
                    }
                }
            }
        }

        swaps.sort_by(|a, b| {
            a.ts_utc
                .cmp(&b.ts_utc)
                .then_with(|| a.signature.cmp(&b.signature))
        });
        (swaps, clustered_wallet_ids, independent_wallet_ids)
    }

    fn clustered_partial_survival_fixture_swaps(
        now: DateTime<Utc>,
        include_independent_wallets_with_open_positions: bool,
    ) -> (Vec<SwapEvent>, Vec<String>, Vec<String>, Vec<String>) {
        let removed_wallet_ids: Vec<String> = (0..3)
            .map(|idx| format!("wallet_clustered_removed_{idx:02}"))
            .collect();
        let surviving_wallet_ids: Vec<String> = (0..6)
            .map(|idx| format!("wallet_clustered_surviving_{idx:02}"))
            .collect();
        let independent_wallet_ids: Vec<String> = if include_independent_wallets_with_open_positions
        {
            (0..3)
                .map(|idx| format!("wallet_independent_open_{idx:02}"))
                .collect()
        } else {
            Vec::new()
        };
        let removed_token = "TokenClusteredThinEdge111111111111111111111";
        let surviving_token = "TokenClusteredThickEdge11111111111111111111";
        let phase_bases = [
            now - Duration::days(2),
            now - Duration::days(1),
            now - Duration::hours(2),
        ];
        let rounds_per_phase = 4;
        let mut swaps = Vec::new();

        for (phase_idx, phase_base) in phase_bases.into_iter().enumerate() {
            for round in 0..rounds_per_phase {
                let round_offset_minutes = (round * 20) as i64;

                for (wallet_idx, wallet_id) in removed_wallet_ids.iter().enumerate() {
                    let buy_ts =
                        phase_base + Duration::minutes(round_offset_minutes + wallet_idx as i64);
                    let sell_ts = buy_ts + Duration::minutes(5);
                    swaps.push(swap(
                        wallet_id,
                        &format!("removed-{phase_idx}-{round}-{wallet_idx}-buy"),
                        buy_ts,
                        SOL_MINT,
                        removed_token,
                        1.0,
                        100.0,
                    ));
                    swaps.push(swap(
                        wallet_id,
                        &format!("removed-{phase_idx}-{round}-{wallet_idx}-sell"),
                        sell_ts,
                        removed_token,
                        SOL_MINT,
                        100.0,
                        1.3,
                    ));
                }

                for (wallet_idx, wallet_id) in surviving_wallet_ids.iter().enumerate() {
                    let buy_ts = phase_base
                        + Duration::minutes(round_offset_minutes + 6 + wallet_idx as i64);
                    let sell_ts = buy_ts + Duration::minutes(5);
                    swaps.push(swap(
                        wallet_id,
                        &format!("surviving-{phase_idx}-{round}-{wallet_idx}-buy"),
                        buy_ts,
                        SOL_MINT,
                        surviving_token,
                        1.0,
                        100.0,
                    ));
                    swaps.push(swap(
                        wallet_id,
                        &format!("surviving-{phase_idx}-{round}-{wallet_idx}-sell"),
                        sell_ts,
                        surviving_token,
                        SOL_MINT,
                        100.0,
                        1.3,
                    ));
                }

                for noise_idx in 0..4 {
                    let noise_buy_ts = phase_base
                        + Duration::minutes(round_offset_minutes + 14 + noise_idx as i64);
                    swaps.push(swap(
                        &format!(
                            "wallet_clustered_surviving_noise_{phase_idx:02}_{round:02}_{noise_idx:02}"
                        ),
                        &format!("surviving-noise-{phase_idx}-{round}-{noise_idx}"),
                        noise_buy_ts,
                        SOL_MINT,
                        surviving_token,
                        0.4,
                        40.0,
                    ));
                }

                for (wallet_idx, wallet_id) in independent_wallet_ids.iter().enumerate() {
                    let token = format!("TokenIndependentOpen{wallet_idx:02}111111111111111111");
                    let buy_ts = phase_base
                        + Duration::minutes(round_offset_minutes + 22 + wallet_idx as i64);
                    let final_round =
                        phase_idx == phase_bases.len() - 1 && round == rounds_per_phase - 1;
                    swaps.push(swap(
                        wallet_id,
                        &format!("independent-open-{phase_idx}-{round}-{wallet_idx}-buy"),
                        buy_ts,
                        SOL_MINT,
                        token.as_str(),
                        1.0,
                        100.0,
                    ));
                    if !final_round {
                        swaps.push(swap(
                            wallet_id,
                            &format!("independent-open-{phase_idx}-{round}-{wallet_idx}-sell"),
                            buy_ts + Duration::minutes(5),
                            token.as_str(),
                            SOL_MINT,
                            100.0,
                            1.25,
                        ));
                    }
                    for noise_idx in 0..10 {
                        swaps.push(swap(
                            &format!(
                                "wallet_independent_open_noise_{phase_idx:02}_{round:02}_{wallet_idx:02}_{noise_idx:02}"
                            ),
                            &format!(
                                "independent-open-noise-{phase_idx}-{round}-{wallet_idx}-{noise_idx}"
                            ),
                            buy_ts + Duration::seconds(30 + noise_idx as i64),
                            SOL_MINT,
                            token.as_str(),
                            0.4,
                            40.0,
                        ));
                    }
                }
            }
        }

        swaps.sort_by(|a, b| {
            a.ts_utc
                .cmp(&b.ts_utc)
                .then_with(|| a.signature.cmp(&b.signature))
        });
        (
            swaps,
            removed_wallet_ids,
            surviving_wallet_ids,
            independent_wallet_ids,
        )
    }

    fn sol_leg_window_volume_and_unique_traders(
        token_sol_history: &HashMap<String, Vec<SolLegTrade>>,
        token: &str,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> (f64, usize) {
        let Some(trades) = token_sol_history.get(token) else {
            return (0.0, 0);
        };
        let mut volume_sol = 0.0;
        let mut unique_traders = HashSet::new();
        for trade in trades {
            if trade.ts < window_start || trade.ts > window_end {
                continue;
            }
            volume_sol += trade.sol_notional;
            unique_traders.insert(trade.wallet_id.as_str());
        }
        (volume_sol, unique_traders.len())
    }

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
