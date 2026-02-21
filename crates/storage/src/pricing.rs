use super::{
    SqliteStore, LIVE_UNREALIZED_RELIABLE_PRICE_MAX_SAMPLES,
    LIVE_UNREALIZED_RELIABLE_PRICE_MIN_SAMPLES, LIVE_UNREALIZED_RELIABLE_PRICE_MIN_SOL_NOTIONAL,
    LIVE_UNREALIZED_RELIABLE_PRICE_WINDOW_MINUTES, STALE_CLOSE_RELIABLE_PRICE_MAX_SAMPLES,
    STALE_CLOSE_RELIABLE_PRICE_MIN_SAMPLES, STALE_CLOSE_RELIABLE_PRICE_MIN_SOL_NOTIONAL,
    STALE_CLOSE_RELIABLE_PRICE_WINDOW_MINUTES,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use rusqlite::{params, OptionalExtension};

impl SqliteStore {
    pub fn latest_token_sol_price(&self, token: &str, as_of: DateTime<Utc>) -> Result<Option<f64>> {
        const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
        let as_of_raw = as_of.to_rfc3339();
        let price: Option<f64> = self
            .conn
            .query_row(
                "SELECT price
                 FROM (
                    SELECT qty_in / qty_out AS price, ts
                    FROM observed_swaps
                    WHERE token_in = ?1
                      AND token_out = ?2
                      AND qty_in > 0
                      AND qty_out > 0
                      AND ts <= ?3
                    UNION ALL
                    SELECT qty_out / qty_in AS price, ts
                    FROM observed_swaps
                    WHERE token_in = ?2
                      AND token_out = ?1
                      AND qty_in > 0
                      AND qty_out > 0
                      AND ts <= ?3
                 )
                 ORDER BY ts DESC
                 LIMIT 1",
                params![SOL_MINT, token, as_of_raw],
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
            "stale-close",
        )
    }

    pub(crate) fn reliable_token_sol_price_for_live_unrealized(
        &self,
        token: &str,
        as_of: DateTime<Utc>,
    ) -> Result<Option<f64>> {
        self.reliable_token_sol_price(
            token,
            as_of,
            LIVE_UNREALIZED_RELIABLE_PRICE_WINDOW_MINUTES,
            LIVE_UNREALIZED_RELIABLE_PRICE_MIN_SOL_NOTIONAL,
            LIVE_UNREALIZED_RELIABLE_PRICE_MIN_SAMPLES,
            LIVE_UNREALIZED_RELIABLE_PRICE_MAX_SAMPLES,
            "live-unrealized",
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
        context_label: &str,
    ) -> Result<Option<f64>> {
        const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
        let as_of_raw = as_of.to_rfc3339();
        let since_raw = (as_of - Duration::minutes(window_minutes.max(1))).to_rfc3339();
        let mut stmt = self
            .conn
            .prepare(
                "SELECT price_sol, sol_notional
                 FROM (
                    SELECT qty_in / qty_out AS price_sol, qty_in AS sol_notional, ts
                    FROM observed_swaps
                    WHERE token_in = ?1
                      AND token_out = ?2
                      AND qty_in > 0
                      AND qty_out > 0
                      AND ts >= ?3
                      AND ts <= ?4
                    UNION ALL
                    SELECT qty_out / qty_in AS price_sol, qty_out AS sol_notional, ts
                    FROM observed_swaps
                    WHERE token_in = ?2
                      AND token_out = ?1
                      AND qty_in > 0
                      AND qty_out > 0
                      AND ts >= ?3
                      AND ts <= ?4
                 )
                 WHERE sol_notional >= ?5
                 ORDER BY ts DESC
                 LIMIT ?6",
            )
            .with_context(|| {
                format!(
                    "failed to prepare reliable {} token/sol price query",
                    context_label
                )
            })?;
        let mut rows = stmt
            .query(params![
                SOL_MINT,
                token,
                since_raw,
                as_of_raw,
                min_sol_notional,
                max_samples as i64,
            ])
            .with_context(|| {
                format!(
                    "failed querying reliable {} token/sol price samples",
                    context_label
                )
            })?;

        let mut prices = Vec::with_capacity(max_samples);
        while let Some(row) = rows.next().with_context(|| {
            format!(
                "failed iterating reliable {} token/sol price samples",
                context_label
            )
        })? {
            let price_sol: f64 = row.get(0).context("failed reading price_sol sample")?;
            let sol_notional: f64 = row.get(1).context("failed reading sol_notional sample")?;
            if !price_sol.is_finite() || price_sol <= 0.0 {
                continue;
            }
            if !sol_notional.is_finite() || sol_notional < min_sol_notional {
                continue;
            }
            prices.push(price_sol);
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
        if !median.is_finite() || median <= 0.0 {
            return Ok(None);
        }
        Ok(Some(median))
    }
}
