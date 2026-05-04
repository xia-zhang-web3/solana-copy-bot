fn apply_discovery_scoring_swaps_on_conn(
    conn: &Connection,
    prepared_swaps: &[PreparedScoringSwap],
) -> Result<()> {
    for prepared in prepared_swaps {
        let swap = &prepared.swap;
        upsert_wallet_scoring_day_on_conn(conn, swap)?;
        upsert_wallet_scoring_tx_minute_on_conn(
            conn,
            &swap.wallet,
            swap.ts_utc.timestamp().div_euclid(60),
        )?;
        if let Some(buy_fact) = prepared.buy_fact.as_ref() {
            if let Some(cache_upsert) = buy_fact.quality_cache_upsert.as_ref() {
                upsert_token_quality_cache_on_conn(
                    conn,
                    &cache_upsert.mint,
                    cache_upsert.holders,
                    cache_upsert.liquidity_sol,
                    cache_upsert.token_age_seconds,
                    cache_upsert.fetched_at,
                )?;
            }
            insert_wallet_scoring_buy_fact_on_conn(conn, swap, buy_fact)?;
        } else if is_sol_sell(swap) {
            apply_wallet_scoring_sell_on_conn(conn, swap)?;
        }
    }
    Ok(())
}

fn apply_discovery_scoring_boundary_lot_swaps_on_conn(
    conn: &Connection,
    swaps: &[SwapEvent],
) -> Result<()> {
    for swap in swaps {
        if is_sol_buy(swap) {
            insert_wallet_scoring_open_lot_on_conn(conn, swap)?;
        } else if is_sol_sell(swap) {
            apply_wallet_scoring_sell_lot_only_on_conn(conn, swap)?;
        }
    }
    Ok(())
}

fn apply_discovery_scoring_swaps_and_checkpoint_on_conn(
    conn: &Connection,
    prepared_swaps: &[PreparedScoringSwap],
    progress_start_ts: DateTime<Utc>,
    progress_cursor: &DiscoveryRuntimeCursor,
    stage_start: &mut impl FnMut(&str),
    stage_end: &mut impl FnMut(&str, usize, u64, &str),
) -> Result<(u64, u64)> {
    stage_start("aggregate_batch_apply");
    let apply_started_at = Instant::now();
    if let Err(error) = apply_discovery_scoring_swaps_on_conn(conn, prepared_swaps) {
        stage_end(
            "aggregate_batch_apply",
            prepared_swaps.len(),
            apply_started_at.elapsed().as_millis() as u64,
            "failed",
        );
        return Err(error);
    }
    let apply_ms = apply_started_at.elapsed().as_millis() as u64;
    stage_end(
        "aggregate_batch_apply",
        prepared_swaps.len(),
        apply_ms,
        "completed",
    );

    maybe_fail_after_materialization_before_checkpoint()?;

    stage_start("progress_checkpoint");
    let progress_started_at = Instant::now();
    let updated_at = Utc::now().to_rfc3339();
    if let Err(error) = upsert_discovery_scoring_backfill_progress_on_conn(
        conn,
        progress_start_ts,
        progress_cursor,
        &updated_at,
    ) {
        stage_end(
            "progress_checkpoint",
            prepared_swaps.len(),
            progress_started_at.elapsed().as_millis() as u64,
            "failed",
        );
        return Err(error);
    }
    let progress_update_ms = progress_started_at.elapsed().as_millis() as u64;
    stage_end(
        "progress_checkpoint",
        prepared_swaps.len(),
        progress_update_ms,
        "completed",
    );

    Ok((apply_ms, progress_update_ms))
}

fn apply_discovery_scoring_boundary_lot_swaps_and_checkpoint_on_conn(
    conn: &Connection,
    swaps: &[SwapEvent],
    progress_start_ts: DateTime<Utc>,
    progress_cursor: &DiscoveryRuntimeCursor,
    stage_start: &mut impl FnMut(&str),
    stage_end: &mut impl FnMut(&str, usize, u64, &str),
) -> Result<(u64, u64)> {
    stage_start("aggregate_batch_apply");
    let apply_started_at = Instant::now();
    if let Err(error) = apply_discovery_scoring_boundary_lot_swaps_on_conn(conn, swaps) {
        stage_end(
            "aggregate_batch_apply",
            swaps.len(),
            apply_started_at.elapsed().as_millis() as u64,
            "failed",
        );
        return Err(error);
    }
    let apply_ms = apply_started_at.elapsed().as_millis() as u64;
    stage_end("aggregate_batch_apply", swaps.len(), apply_ms, "completed");

    maybe_fail_after_materialization_before_checkpoint()?;

    stage_start("progress_checkpoint");
    let progress_started_at = Instant::now();
    let updated_at = Utc::now().to_rfc3339();
    if let Err(error) = upsert_discovery_scoring_backfill_progress_on_conn(
        conn,
        progress_start_ts,
        progress_cursor,
        &updated_at,
    ) {
        stage_end(
            "progress_checkpoint",
            swaps.len(),
            progress_started_at.elapsed().as_millis() as u64,
            "failed",
        );
        return Err(error);
    }
    let progress_update_ms = progress_started_at.elapsed().as_millis() as u64;
    stage_end(
        "progress_checkpoint",
        swaps.len(),
        progress_update_ms,
        "completed",
    );

    Ok((apply_ms, progress_update_ms))
}
