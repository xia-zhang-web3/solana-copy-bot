use super::*;

pub(super) fn flush_prepared_discovery_scoring_builder_batch_on_conn(
    conn: &Connection,
    prepared: &PreparedDiscoveryScoringBuilderBatch,
) -> Result<()> {
    for row in prepared.quality_upserts.values() {
        upsert_token_quality_cache_on_conn(conn, row)?;
    }
    for ((wallet_id, activity_day), delta) in &prepared.day_deltas {
        upsert_wallet_scoring_day_delta_on_conn(conn, wallet_id, *activity_day, delta)?;
    }
    for ((wallet_id, minute_bucket), tx_count) in &prepared.tx_minute_deltas {
        upsert_wallet_scoring_tx_minute_delta_on_conn(conn, wallet_id, *minute_bucket, *tx_count)?;
    }
    for row in &prepared.buy_rows {
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
                &row.buy_signature,
                &row.wallet_id,
                &row.token,
                row.ts.to_rfc3339(),
                row.activity_day.format("%Y-%m-%d").to_string(),
                row.notional_sol,
                row.market_volume_5m_sol,
                row.market_unique_traders_5m as i64,
                row.market_liquidity_proxy_sol,
                match row.quality_source {
                    WalletScoringQualitySource::Fresh => "fresh",
                    WalletScoringQualitySource::Stale => "stale",
                    WalletScoringQualitySource::Deferred => "deferred",
                    WalletScoringQualitySource::Missing => "missing",
                },
                row.quality_token_age_seconds.map(|value| value as i64),
                row.quality_holders.map(|value| value as i64),
                row.quality_liquidity_sol,
                row.rug_check_after_ts.to_rfc3339(),
            ],
        )
        .context("failed builder inserting wallet_scoring_buy_facts row")?;
    }
    for row in &prepared.close_rows {
        conn.execute(
            "INSERT OR IGNORE INTO wallet_scoring_close_facts(
                sell_signature,
                segment_index,
                wallet_id,
                token,
                closed_ts,
                activity_day,
                pnl_sol,
                hold_seconds,
                win
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                &row.sell_signature,
                row.segment_index,
                &row.wallet_id,
                &row.token,
                row.closed_ts.to_rfc3339(),
                row.activity_day.format("%Y-%m-%d").to_string(),
                row.pnl_sol,
                row.hold_seconds,
                if row.win { 1 } else { 0 },
            ],
        )
        .context("failed builder inserting wallet_scoring_close_facts row")?;
    }
    for (buy_signature, lot) in &prepared.lot_mutations {
        match lot {
            Some(lot) => {
                conn.execute(
                    "INSERT INTO wallet_scoring_open_lots(
                        buy_signature,
                        wallet_id,
                        token,
                        qty,
                        cost_sol,
                        opened_ts
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                     ON CONFLICT(buy_signature) DO UPDATE SET
                        qty = excluded.qty,
                        cost_sol = excluded.cost_sol,
                        opened_ts = excluded.opened_ts",
                    params![
                        buy_signature,
                        &lot.wallet_id,
                        &lot.token,
                        lot.qty,
                        lot.cost_sol,
                        lot.opened_ts.to_rfc3339(),
                    ],
                )
                .context("failed builder upserting wallet_scoring_open_lot")?;
            }
            None => {
                conn.execute(
                    "DELETE FROM wallet_scoring_open_lots WHERE buy_signature = ?1",
                    params![buy_signature],
                )
                .context("failed builder deleting wallet_scoring_open_lot")?;
            }
        }
    }
    Ok(())
}
