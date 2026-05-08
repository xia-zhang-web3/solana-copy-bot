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
