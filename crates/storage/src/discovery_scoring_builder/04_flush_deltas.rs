fn upsert_wallet_scoring_day_delta_on_conn(
    conn: &Connection,
    wallet_id: &str,
    activity_day: NaiveDate,
    delta: &DayDelta,
) -> Result<()> {
    if delta.trades == 0 {
        return Ok(());
    }
    conn.execute(
        "INSERT INTO wallet_scoring_days(
            wallet_id,
            activity_day,
            first_seen,
            last_seen,
            trades,
            spent_sol,
            max_buy_notional_sol
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
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
            trades = wallet_scoring_days.trades + excluded.trades,
            spent_sol = wallet_scoring_days.spent_sol + excluded.spent_sol,
            max_buy_notional_sol = MAX(
                wallet_scoring_days.max_buy_notional_sol,
                excluded.max_buy_notional_sol
            )",
        params![
            wallet_id,
            activity_day.format("%Y-%m-%d").to_string(),
            delta.first_seen.unwrap_or_else(Utc::now).to_rfc3339(),
            delta.last_seen.unwrap_or_else(Utc::now).to_rfc3339(),
            delta.trades as i64,
            delta.spent_sol,
            delta.max_buy_notional_sol,
        ],
    )
    .context("failed builder upserting wallet_scoring_days row")?;
    Ok(())
}

fn upsert_wallet_scoring_tx_minute_delta_on_conn(
    conn: &Connection,
    wallet_id: &str,
    minute_bucket: i64,
    tx_count: u32,
) -> Result<()> {
    if tx_count == 0 {
        return Ok(());
    }
    conn.execute(
        "INSERT INTO wallet_scoring_tx_minutes(wallet_id, minute_bucket, tx_count)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(wallet_id, minute_bucket) DO UPDATE SET
            tx_count = wallet_scoring_tx_minutes.tx_count + excluded.tx_count",
        params![wallet_id, minute_bucket, tx_count as i64],
    )
    .context("failed builder upserting wallet_scoring_tx_minutes row")?;
    Ok(())
}

fn upsert_token_quality_cache_on_conn(conn: &Connection, row: &QualityCacheUpsert) -> Result<()> {
    conn.execute(
        "INSERT INTO token_quality_cache(
            mint,
            holders,
            liquidity_sol,
            token_age_seconds,
            fetched_at
         ) VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(mint) DO UPDATE SET
            holders = excluded.holders,
            liquidity_sol = excluded.liquidity_sol,
            token_age_seconds = excluded.token_age_seconds,
            fetched_at = excluded.fetched_at",
        params![
            &row.mint,
            row.holders.map(|value| value as i64),
            row.liquidity_sol,
            row.token_age_seconds.map(|value| value as i64),
            row.fetched_at.to_rfc3339(),
        ],
    )
    .context("failed builder upserting token_quality_cache row")?;
    Ok(())
}

fn export_boundary_seed_lots_from_open_lots(
    open_lots: &HashMap<WalletTokenKey, VecDeque<BuilderLot>>,
) -> Vec<DiscoveryScoringBoundarySeedLot> {
    let mut lots = open_lots
        .values()
        .flat_map(|wallet_token_lots| wallet_token_lots.iter())
        .map(|lot| DiscoveryScoringBoundarySeedLot {
            buy_signature: lot.buy_signature.clone(),
            wallet_id: lot.wallet_id.clone(),
            token: lot.token.clone(),
            qty: lot.qty,
            cost_sol: lot.cost_sol,
            opened_ts: lot.opened_ts,
        })
        .collect::<Vec<_>>();
    lots.sort_by(|left, right| {
        left.wallet_id
            .cmp(&right.wallet_id)
            .then_with(|| left.token.cmp(&right.token))
            .then_with(|| left.opened_ts.cmp(&right.opened_ts))
            .then_with(|| left.buy_signature.cmp(&right.buy_signature))
    });
    lots
}
