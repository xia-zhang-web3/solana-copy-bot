use super::*;

pub(super) fn upsert_wallet_scoring_day_on_conn(conn: &Connection, swap: &SwapEvent) -> Result<()> {
    let buy_notional = if is_sol_buy(swap) {
        swap.amount_in.max(0.0)
    } else {
        0.0
    };
    conn.execute(
        "INSERT INTO wallet_scoring_days(
            wallet_id,
            activity_day,
            first_seen,
            last_seen,
            trades,
            spent_sol,
            max_buy_notional_sol
         ) VALUES (?1, ?2, ?3, ?4, 1, ?5, ?6)
         ON CONFLICT(wallet_id, activity_day) DO UPDATE SET
            first_seen = CASE
                WHEN excluded.first_seen < wallet_scoring_days.first_seen
                    THEN excluded.first_seen
                ELSE wallet_scoring_days.first_seen
            END,
            last_seen = CASE
                WHEN excluded.last_seen > wallet_scoring_days.last_seen
                    THEN excluded.last_seen
                ELSE wallet_scoring_days.last_seen
            END,
            trades = wallet_scoring_days.trades + 1,
            spent_sol = wallet_scoring_days.spent_sol + excluded.spent_sol,
            max_buy_notional_sol = MAX(
                wallet_scoring_days.max_buy_notional_sol,
                excluded.max_buy_notional_sol
            )",
        params![
            &swap.wallet,
            swap.ts_utc.date_naive().format("%Y-%m-%d").to_string(),
            swap.ts_utc.to_rfc3339(),
            swap.ts_utc.to_rfc3339(),
            buy_notional,
            buy_notional,
        ],
    )
    .context("failed upserting wallet_scoring_days row")?;
    Ok(())
}

pub(super) fn upsert_wallet_scoring_tx_minute_on_conn(
    conn: &Connection,
    wallet_id: &str,
    minute_bucket: i64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO wallet_scoring_tx_minutes(wallet_id, minute_bucket, tx_count)
         VALUES (?1, ?2, 1)
         ON CONFLICT(wallet_id, minute_bucket) DO UPDATE SET
            tx_count = wallet_scoring_tx_minutes.tx_count + 1",
        params![wallet_id, minute_bucket],
    )
    .context("failed upserting wallet_scoring_tx_minutes row")?;
    Ok(())
}

pub(super) fn insert_wallet_scoring_buy_fact_on_conn(
    conn: &Connection,
    swap: &SwapEvent,
    prepared: &PreparedBuyFact,
) -> Result<()> {
    let token = swap.token_out.as_str();

    conn.execute(
        "INSERT OR IGNORE INTO wallet_scoring_buy_facts(
            buy_signature,
            wallet_id,
            token,
            ts,
            activity_day,
            notional_sol,
            market_volume_5m_sol,
            market_unique_traders_5m,
            market_liquidity_proxy_sol,
            quality_source,
            quality_token_age_seconds,
            quality_holders,
            quality_liquidity_sol,
            rug_check_after_ts,
            rug_volume_lookahead_sol,
            rug_unique_traders_lookahead
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, NULL, NULL)",
        params![
            &swap.signature,
            &swap.wallet,
            token,
            swap.ts_utc.to_rfc3339(),
            swap.ts_utc.date_naive().format("%Y-%m-%d").to_string(),
            swap.amount_in.max(0.0),
            prepared.market_stats.volume_5m_sol.max(0.0),
            prepared.market_stats.unique_traders_5m as i64,
            prepared.market_stats.liquidity_sol_proxy.max(0.0),
            match prepared.quality.source {
                WalletScoringQualitySource::Fresh => "fresh",
                WalletScoringQualitySource::Stale => "stale",
                WalletScoringQualitySource::Deferred => "deferred",
                WalletScoringQualitySource::Missing => "missing",
            },
            prepared.quality.token_age_seconds.map(|value| value as i64),
            prepared.quality.holders.map(|value| value as i64),
            prepared.quality.liquidity_sol,
            prepared.rug_check_after_ts.to_rfc3339(),
        ],
    )
    .context("failed inserting wallet_scoring_buy_facts row")?;

    insert_wallet_scoring_open_lot_on_conn(conn, swap)?;
    Ok(())
}

pub(super) fn insert_wallet_scoring_open_lot_on_conn(
    conn: &Connection,
    swap: &SwapEvent,
) -> Result<()> {
    let token = swap.token_out.as_str();
    conn.execute(
        "INSERT OR IGNORE INTO wallet_scoring_open_lots(
            buy_signature,
            wallet_id,
            token,
            qty,
            cost_sol,
            opened_ts
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            &swap.signature,
            &swap.wallet,
            token,
            swap.amount_out.max(0.0),
            swap.amount_in.max(0.0),
            swap.ts_utc.to_rfc3339(),
        ],
    )
    .context("failed inserting wallet_scoring_open_lots row")?;
    Ok(())
}
