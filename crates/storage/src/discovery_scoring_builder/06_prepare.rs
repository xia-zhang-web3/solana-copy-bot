use super::*;

pub(super) fn prepare_discovery_scoring_builder_batch(
    store: &SqliteStore,
    builder: &mut DiscoveryScoringReplayBuilder,
    swaps: &[SwapEvent],
    config: &DiscoveryAggregateWriteConfig,
) -> Result<(PreparedDiscoveryScoringBuilderBatch, u64)> {
    if swaps.is_empty() {
        return Ok((PreparedDiscoveryScoringBuilderBatch::default(), 0));
    }

    let prepare_started_at = Instant::now();
    let mut ordered = swaps.to_vec();
    ordered.sort_by(|left, right| {
        left.ts_utc
            .cmp(&right.ts_utc)
            .then_with(|| left.slot.cmp(&right.slot))
            .then_with(|| left.signature.cmp(&right.signature))
    });

    let mut prepared = PreparedDiscoveryScoringBuilderBatch::default();
    let mut quality_budget = QualityFetchBudget::default();

    let mut offset = 0usize;
    while offset < ordered.len() {
        let ts = ordered[offset].ts_utc;
        let group_end = ordered[offset..]
            .iter()
            .take_while(|swap| swap.ts_utc == ts)
            .count()
            + offset;

        for state in builder.market_windows.values_mut() {
            state.evict_before(ts - Duration::minutes(5));
        }

        let mut group_market_stats = HashMap::<String, TokenMarketSnapshot>::new();
        for swap in &ordered[offset..group_end] {
            let day_key = (swap.wallet.clone(), swap.ts_utc.date_naive());
            let delta = prepared.day_deltas.entry(day_key).or_default();
            delta.first_seen = Some(
                delta
                    .first_seen
                    .map(|current| current.min(swap.ts_utc))
                    .unwrap_or(swap.ts_utc),
            );
            delta.last_seen = Some(
                delta
                    .last_seen
                    .map(|current| current.max(swap.ts_utc))
                    .unwrap_or(swap.ts_utc),
            );
            delta.trades = delta.trades.saturating_add(1);
            if is_sol_buy(swap) {
                delta.spent_sol += swap.amount_in.max(0.0);
                delta.max_buy_notional_sol =
                    delta.max_buy_notional_sol.max(swap.amount_in.max(0.0));
            }
            *prepared
                .tx_minute_deltas
                .entry((swap.wallet.clone(), swap.ts_utc.timestamp().div_euclid(60)))
                .or_insert(0) += 1;

            if let Some((token, sol_notional)) = sol_pair_market_event(swap) {
                let state = builder.market_windows.entry(token.to_string()).or_default();
                state.push(swap, sol_notional);
                group_market_stats.insert(token.to_string(), state.snapshot());
            }
        }

        for swap in &ordered[offset..group_end] {
            if is_sol_buy(swap) {
                let token = swap.token_out.clone();
                ensure_builder_open_lots_loaded(store, builder, &swap.wallet, &token)?;
                let market_stats =
                    group_market_stats
                        .get(&token)
                        .copied()
                        .unwrap_or(TokenMarketSnapshot {
                            volume_5m_sol: 0.0,
                            unique_traders_5m: 0,
                            liquidity_sol_proxy: 0.0,
                        });
                let (quality, quality_upsert) = resolve_quality_snapshot(
                    store,
                    &mut builder.quality_cache,
                    swap.ts_utc,
                    &token,
                    config,
                    &mut quality_budget,
                )?;
                if let Some(quality_upsert) = quality_upsert {
                    prepared
                        .quality_upserts
                        .insert(quality_upsert.mint.clone(), quality_upsert);
                }
                prepared.buy_rows.push(PendingBuyFactRow {
                    buy_signature: swap.signature.clone(),
                    wallet_id: swap.wallet.clone(),
                    token: token.clone(),
                    ts: swap.ts_utc,
                    activity_day: swap.ts_utc.date_naive(),
                    notional_sol: swap.amount_in.max(0.0),
                    market_volume_5m_sol: market_stats.volume_5m_sol,
                    market_unique_traders_5m: market_stats.unique_traders_5m,
                    market_liquidity_proxy_sol: market_stats.liquidity_sol_proxy,
                    quality_source: quality.source,
                    quality_token_age_seconds: quality.token_age_seconds,
                    quality_holders: quality.holders,
                    quality_liquidity_sol: quality.liquidity_sol,
                    rug_check_after_ts: swap.ts_utc
                        + Duration::seconds(config.rug_lookahead_seconds.max(1) as i64),
                });
                for mutation in apply_swap_lot_accounting(&mut builder.open_lots, swap).mutations {
                    match mutation {
                        LotMutationAction::Upsert(lot) => {
                            prepared
                                .lot_mutations
                                .insert(lot.buy_signature.clone(), Some(lot));
                        }
                        LotMutationAction::Delete(buy_signature) => {
                            prepared.lot_mutations.insert(buy_signature, None);
                        }
                    }
                }
            } else if is_sol_sell(swap) {
                ensure_builder_open_lots_loaded(store, builder, &swap.wallet, &swap.token_in)?;
                let accounting_step = apply_swap_lot_accounting(&mut builder.open_lots, swap);
                prepared.close_rows.extend(accounting_step.close_rows);
                for mutation in accounting_step.mutations {
                    match mutation {
                        LotMutationAction::Upsert(lot) => {
                            prepared
                                .lot_mutations
                                .insert(lot.buy_signature.clone(), Some(lot));
                        }
                        LotMutationAction::Delete(buy_signature) => {
                            prepared.lot_mutations.insert(buy_signature, None);
                        }
                    }
                }
            }
        }

        offset = group_end;
    }

    Ok((prepared, prepare_started_at.elapsed().as_millis() as u64))
}

pub(super) fn prepare_discovery_scoring_boundary_lot_batch(
    builder: &mut DiscoveryScoringBoundaryLotBuilder,
    swaps: &[SwapEvent],
) -> Result<u64> {
    if swaps.is_empty() {
        return Ok(0);
    }

    let prepare_started_at = Instant::now();
    let mut ordered = swaps.to_vec();
    ordered.sort_by(|left, right| {
        left.ts_utc
            .cmp(&right.ts_utc)
            .then_with(|| left.slot.cmp(&right.slot))
            .then_with(|| left.signature.cmp(&right.signature))
    });

    for swap in &ordered {
        let _ = apply_swap_lot_accounting(&mut builder.open_lots, swap);
    }

    Ok(prepare_started_at.elapsed().as_millis() as u64)
}
