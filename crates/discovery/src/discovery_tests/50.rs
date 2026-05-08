fn seed_stage1_collect_buy_mints_noise_fixture(
        store: &SqliteStore,
        config: &DiscoveryConfig,
        now: DateTime<Utc>,
        unique_buy_mints: usize,
        sell_noise_rows: usize,
    ) -> Result<DateTime<Utc>> {
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);

        for idx in 0..unique_buy_mints {
            let ts = window_start + Duration::minutes(idx as i64);
            let token = format!("TokenStage1CollectBuyMint{idx:04}1111111111111111");
            store.insert_observed_swap(&swap(
                "wallet_collect_buy_mints",
                &format!("stage1-collect-buy-mints-buy-{idx}"),
                ts,
                SOL_MINT,
                token.as_str(),
                0.5,
                50.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_collect_buy_mints",
                &format!("stage1-collect-buy-mints-sell-{idx}"),
                ts + Duration::minutes(1),
                token.as_str(),
                SOL_MINT,
                50.0,
                0.6,
            ))?;
        }

        for idx in 0..sell_noise_rows {
            let ts = window_start + Duration::hours(4) + Duration::seconds(idx as i64);
            let token = format!("TokenStage1SellNoise{idx:04}11111111111111111111");
            store.insert_observed_swap(&swap(
                "wallet_sell_noise",
                &format!("stage1-collect-buy-mints-sell-noise-{idx}"),
                ts,
                token.as_str(),
                SOL_MINT,
                100.0,
                0.05,
            ))?;
        }

        Ok(window_start)
    }

    fn seed_stage1_live_like_wallet_stats_backlog_fixture(
        store: &SqliteStore,
        config: &DiscoveryConfig,
        now: DateTime<Utc>,
        noise_wallets: usize,
        signature_prefix: &str,
    ) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let metrics_window_start = metrics_window_start_for_test(config, now);
        let token = "TokenStage1RepairReplayLive11111111111111111";

        for idx in 0..2 {
            let buy_ts = window_start + Duration::minutes((idx * 10) as i64);
            store.insert_observed_swap(&swap(
                "wallet_live_like_top",
                &format!("{signature_prefix}-top-buy-{idx}"),
                buy_ts,
                SOL_MINT,
                token,
                1.0,
                100.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_live_like_top",
                &format!("{signature_prefix}-top-sell-{idx}"),
                buy_ts + Duration::minutes(5),
                token,
                SOL_MINT,
                100.0,
                1.3,
            ))?;
        }

        let mut latest_cursor: Option<DiscoveryRuntimeCursor> = None;
        for idx in 0..noise_wallets {
            let ts = now - Duration::seconds((noise_wallets.saturating_sub(idx)) as i64);
            let swap = swap(
                &format!("wallet_live_like_noise_{idx:05}"),
                &format!("{signature_prefix}-noise-{idx:05}"),
                ts,
                SOL_MINT,
                token,
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

    fn seed_stage1_collect_buy_mints_legacy_migration_fixture(
        store: &SqliteStore,
        config: &DiscoveryConfig,
        now: DateTime<Utc>,
    ) -> Result<(DateTime<Utc>, Vec<String>)> {
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let ordered = vec![
            "TokenStage1CollectBuyMintA1111111111111111".to_string(),
            "TokenStage1CollectBuyMintB1111111111111111".to_string(),
            "TokenStage1CollectBuyMintC1111111111111111".to_string(),
            "TokenStage1CollectBuyMintD1111111111111111".to_string(),
        ];
        let chronological = [
            ordered[3].as_str(),
            ordered[0].as_str(),
            ordered[1].as_str(),
            ordered[2].as_str(),
        ];
        for (idx, token) in chronological.iter().enumerate() {
            let ts = window_start + Duration::minutes(idx as i64);
            store.insert_observed_swap(&swap(
                "wallet_collect_buy_migration",
                &format!("stage1-collect-buy-migration-buy-{idx}"),
                ts,
                SOL_MINT,
                token,
                0.5,
                50.0,
            ))?;
            store.insert_observed_swap(&swap(
                "wallet_collect_buy_migration",
                &format!("stage1-collect-buy-migration-sell-{idx}"),
                ts + Duration::minutes(1),
                token,
                SOL_MINT,
                50.0,
                0.6,
            ))?;
        }

        Ok((window_start, ordered))
    }

    fn seed_runtime_aggregate_ready_wallet(
        store: &SqliteStore,
        config: &DiscoveryConfig,
        wallet_id: &str,
        token: &str,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
        let mut swaps = Vec::new();
        for idx in 0..4 {
            let buy_ts = now - Duration::days(3) + Duration::minutes((idx * 20) as i64);
            let sell_ts = buy_ts + Duration::minutes(6);
            let buy = swap(
                wallet_id,
                &format!("{wallet_id}-aggregate-buy-{idx}"),
                buy_ts,
                SOL_MINT,
                token,
                1.0,
                100.0,
            );
            let sell = swap(
                wallet_id,
                &format!("{wallet_id}-aggregate-sell-{idx}"),
                sell_ts,
                token,
                SOL_MINT,
                100.0,
                1.2,
            );
            store.insert_observed_swap(&buy)?;
            store.insert_observed_swap(&sell)?;
            swaps.push(buy);
            swaps.push(sell);
        }

        store.reset_discovery_scoring_tables()?;
        store.apply_discovery_scoring_batch(&swaps, &aggregate_write_config(config))?;
        store.finalize_discovery_scoring_rug_facts(now)?;
        store.set_discovery_scoring_covered_since(window_start)?;
        store.set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
            ts_utc: now,
            slot: 999,
            signature: format!("{wallet_id}-aggregate-covered-through"),
        })?;
        Ok(())
    }
