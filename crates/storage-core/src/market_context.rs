use crate::{
    SqliteDiscoveryStore, TokenMarketStats, STALE_CLOSE_RELIABLE_PRICE_MAX_SAMPLES,
    STALE_CLOSE_RELIABLE_PRICE_MIN_SAMPLES, STALE_CLOSE_RELIABLE_PRICE_MIN_SOL_NOTIONAL,
    STALE_CLOSE_RELIABLE_PRICE_WINDOW_MINUTES,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use rusqlite::{params, OptionalExtension};

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

impl SqliteDiscoveryStore {
    pub fn token_market_stats(
        &self,
        token: &str,
        as_of: DateTime<Utc>,
    ) -> Result<TokenMarketStats> {
        let as_of_raw = as_of.to_rfc3339();
        let first_seen_raw: Option<String> = self
            .conn
            .query_row(
                "SELECT MIN(ts)
                 FROM (
                    SELECT ts FROM observed_swaps WHERE token_in = ?1 AND ts <= ?2
                    UNION ALL
                    SELECT ts FROM observed_swaps WHERE token_out = ?1 AND ts <= ?2
                 )",
                params![token, &as_of_raw],
                |row| row.get(0),
            )
            .context("failed querying token first_seen")?;
        let first_seen = first_seen_raw
            .as_deref()
            .map(|raw| {
                DateTime::parse_from_rfc3339(raw)
                    .map(|dt| dt.with_timezone(&Utc))
                    .with_context(|| format!("invalid observed_swaps.ts value: {raw}"))
            })
            .transpose()?;
        let holders_proxy_raw: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*)
                 FROM (
                    SELECT DISTINCT wallet_id
                    FROM observed_swaps
                    WHERE token_in = ?1 AND ts <= ?2
                    UNION
                    SELECT DISTINCT wallet_id
                    FROM observed_swaps
                    WHERE token_out = ?1 AND ts <= ?2
                 )",
                params![token, &as_of_raw],
                |row| row.get(0),
            )
            .context("failed querying token holders proxy")?;
        let window_start = (as_of - Duration::minutes(5)).to_rfc3339();
        let (volume_5m_sol, liquidity_sol_proxy, unique_traders_5m_raw): (f64, f64, i64) = self
            .conn
            .query_row(
                "SELECT
                    COALESCE(SUM(sol_notional), 0.0),
                    COALESCE(MAX(sol_notional), 0.0),
                    COUNT(DISTINCT wallet_id)
                 FROM (
                    SELECT wallet_id, qty_out AS sol_notional
                    FROM observed_swaps
                    WHERE token_in = ?1 AND token_out = ?2 AND ts >= ?3 AND ts <= ?4
                    UNION ALL
                    SELECT wallet_id, qty_in AS sol_notional
                    FROM observed_swaps
                    WHERE token_out = ?1 AND token_in = ?2 AND ts >= ?3 AND ts <= ?4
                 )",
                params![token, SOL_MINT, window_start, &as_of_raw],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .context("failed querying token 5m market stats")?;
        Ok(TokenMarketStats {
            first_seen,
            holders_proxy: holders_proxy_raw.max(0) as u64,
            liquidity_sol_proxy,
            volume_5m_sol,
            unique_traders_5m: unique_traders_5m_raw.max(0) as u64,
        })
    }

    pub fn latest_token_sol_price(&self, token: &str, as_of: DateTime<Utc>) -> Result<Option<f64>> {
        let price: Option<f64> = self
            .conn
            .query_row(
                "SELECT price
                 FROM (
                    SELECT qty_in / qty_out AS price, ts
                    FROM observed_swaps
                    WHERE token_in = ?1 AND token_out = ?2 AND qty_in > 0 AND qty_out > 0 AND ts <= ?3
                    UNION ALL
                    SELECT qty_out / qty_in AS price, ts
                    FROM observed_swaps
                    WHERE token_in = ?2 AND token_out = ?1 AND qty_in > 0 AND qty_out > 0 AND ts <= ?3
                 )
                 ORDER BY ts DESC
                 LIMIT 1",
                params![SOL_MINT, token, as_of.to_rfc3339()],
                |row| row.get(0),
            )
            .optional()
            .context("failed querying latest token/sol price")?;
        Ok(price.filter(|value| value.is_finite() && *value > 0.0))
    }

    pub fn reliable_token_sol_price_for_stale_close(
        &self,
        token: &str,
        as_of: DateTime<Utc>,
    ) -> Result<Option<f64>> {
        self.reliable_token_sol_price(
            token,
            as_of,
            STALE_CLOSE_RELIABLE_PRICE_WINDOW_MINUTES,
            STALE_CLOSE_RELIABLE_PRICE_MIN_SOL_NOTIONAL,
            STALE_CLOSE_RELIABLE_PRICE_MIN_SAMPLES,
            STALE_CLOSE_RELIABLE_PRICE_MAX_SAMPLES,
        )
    }

    fn reliable_token_sol_price(
        &self,
        token: &str,
        as_of: DateTime<Utc>,
        window_minutes: i64,
        min_sol_notional: f64,
        min_samples: usize,
        max_samples: usize,
    ) -> Result<Option<f64>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT price_sol, sol_notional
                 FROM (
                    SELECT qty_in / qty_out AS price_sol, qty_in AS sol_notional, ts
                    FROM observed_swaps
                    WHERE token_in = ?1 AND token_out = ?2 AND qty_in > 0 AND qty_out > 0 AND ts >= ?3 AND ts <= ?4
                    UNION ALL
                    SELECT qty_out / qty_in AS price_sol, qty_out AS sol_notional, ts
                    FROM observed_swaps
                    WHERE token_in = ?2 AND token_out = ?1 AND qty_in > 0 AND qty_out > 0 AND ts >= ?3 AND ts <= ?4
                 )
                 WHERE sol_notional >= ?5
                 ORDER BY ts DESC
                 LIMIT ?6",
            )
            .context("failed to prepare reliable token/sol price query")?;
        let mut rows = stmt
            .query(params![
                SOL_MINT,
                token,
                (as_of - Duration::minutes(window_minutes.max(1))).to_rfc3339(),
                as_of.to_rfc3339(),
                min_sol_notional,
                max_samples as i64,
            ])
            .context("failed querying reliable token/sol price samples")?;
        let mut prices = Vec::with_capacity(max_samples);
        while let Some(row) = rows
            .next()
            .context("failed iterating reliable token/sol price samples")?
        {
            let price_sol: f64 = row.get(0).context("failed reading price_sol sample")?;
            let sol_notional: f64 = row.get(1).context("failed reading sol_notional sample")?;
            if price_sol.is_finite()
                && price_sol > 0.0
                && sol_notional.is_finite()
                && sol_notional >= min_sol_notional
            {
                prices.push(price_sol);
            }
        }
        if prices.len() < min_samples.max(1) {
            return Ok(None);
        }
        prices.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
        let median = if prices.len() % 2 == 1 {
            prices[prices.len() / 2]
        } else {
            let upper = prices.len() / 2;
            (prices[upper - 1] + prices[upper]) / 2.0
        };
        Ok(Some(median).filter(|value| value.is_finite() && *value > 0.0))
    }
}
