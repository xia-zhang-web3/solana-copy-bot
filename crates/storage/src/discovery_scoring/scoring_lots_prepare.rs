fn prepare_discovery_scoring_swaps(
    conn: &Connection,
    swaps: &[SwapEvent],
    config: &DiscoveryAggregateWriteConfig,
) -> Result<Vec<PreparedScoringSwap>> {
    prepare_discovery_scoring_swaps_with_diagnostics(conn, swaps, config, &mut |_| {}, None)
}

fn prepare_discovery_scoring_swaps_with_diagnostics(
    conn: &Connection,
    swaps: &[SwapEvent],
    config: &DiscoveryAggregateWriteConfig,
    emit: &mut impl FnMut(String),
    deadline: Option<Instant>,
) -> Result<Vec<PreparedScoringSwap>> {
    if swaps.is_empty() {
        return Ok(Vec::new());
    }

    let _progress_guard =
        deadline.map(|deadline| DiscoveryScoringPrepareProgressGuard::install(conn, deadline));

    emit_prepare_stage_start(emit, "prepare_sort", None, None);
    let sort_started_at = Instant::now();
    check_prepare_runtime_budget(deadline, emit, "prepare_sort", None, None)?;
    let mut ordered = swaps.to_vec();
    ordered.sort_by(cmp_swap_order);
    emit_prepare_stage_end(
        emit,
        "prepare_sort",
        None,
        None,
        sort_started_at.elapsed().as_millis() as u64,
        "completed",
    );
    emit_prepare_batch_counts(emit, &ordered);

    let mut budget = QualityFetchBudget::default();
    let mut prepared = Vec::with_capacity(ordered.len());
    for swap in ordered {
        let buy_fact = if is_sol_buy(&swap) {
            let token = swap.token_out.as_str();
            emit_prepare_stage_start(
                emit,
                "token_market_stats",
                Some(token),
                Some(&swap.signature),
            );
            let market_stats_started_at = Instant::now();
            check_prepare_runtime_budget(
                deadline,
                emit,
                "token_market_stats",
                Some(token),
                Some(&swap.signature),
            )?;
            let market_stats = match token_market_stats_on_conn(conn, token, swap.ts_utc) {
                Ok(market_stats) => {
                    emit_prepare_stage_end(
                        emit,
                        "token_market_stats",
                        Some(token),
                        Some(&swap.signature),
                        market_stats_started_at.elapsed().as_millis() as u64,
                        "completed",
                    );
                    market_stats
                }
                Err(error) => {
                    let error = prepare_stage_error(
                        error,
                        deadline,
                        "token_market_stats",
                        Some(token),
                        Some(&swap.signature),
                    );
                    emit_prepare_stage_end(
                        emit,
                        "token_market_stats",
                        Some(token),
                        Some(&swap.signature),
                        market_stats_started_at.elapsed().as_millis() as u64,
                        if prepare_error_is_runtime_budget(&error) {
                            "runtime_budget_exhausted"
                        } else {
                            "failed"
                        },
                    );
                    return Err(error);
                }
            };
            let (quality, quality_cache_upsert) =
                resolve_quality_snapshot_on_conn_with_diagnostics(
                    conn,
                    token,
                    swap.ts_utc,
                    config,
                    &mut budget,
                    emit,
                    deadline,
                    Some(&swap.signature),
                )?;
            Some(PreparedBuyFact {
                market_stats,
                quality,
                quality_cache_upsert,
                rug_check_after_ts: swap.ts_utc
                    + Duration::seconds(config.rug_lookahead_seconds.max(1) as i64),
            })
        } else {
            None
        };
        prepared.push(PreparedScoringSwap { swap, buy_fact });
    }
    Ok(prepared)
}
