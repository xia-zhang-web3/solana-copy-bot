use super::{
    SqliteStore, LIVE_UNREALIZED_RELIABLE_PRICE_MAX_SAMPLES,
    LIVE_UNREALIZED_RELIABLE_PRICE_MIN_SAMPLES, LIVE_UNREALIZED_RELIABLE_PRICE_MIN_SOL_NOTIONAL,
    LIVE_UNREALIZED_RELIABLE_PRICE_WINDOW_MINUTES, STALE_CLOSE_RELIABLE_PRICE_MAX_SAMPLES,
    STALE_CLOSE_RELIABLE_PRICE_MIN_SAMPLES, STALE_CLOSE_RELIABLE_PRICE_MIN_SOL_NOTIONAL,
    STALE_CLOSE_RELIABLE_PRICE_WINDOW_MINUTES,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{Lamports, TokenQuantity};
use rusqlite::{params, OptionalExtension};
use std::cmp::Ordering;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ExactTokenSolPriceQuote {
    sol_lamports: Lamports,
    token_qty: TokenQuantity,
}

impl ExactTokenSolPriceQuote {
    pub(crate) fn new(sol_lamports: Lamports, token_qty: TokenQuantity) -> Result<Self> {
        if token_qty.raw() == 0 {
            return Err(anyhow!(
                "exact token/sol quote cannot use zero token quantity"
            ));
        }
        Ok(Self {
            sol_lamports,
            token_qty,
        })
    }

    fn cmp_price(self, other: Self) -> Result<Ordering> {
        if self.token_qty.decimals() != other.token_qty.decimals() {
            return Err(anyhow!(
                "cannot compare exact token/sol quotes with mismatched token decimals: {} vs {}",
                self.token_qty.decimals(),
                other.token_qty.decimals()
            ));
        }
        let lhs = u128::from(self.sol_lamports.as_u64())
            .checked_mul(u128::from(other.token_qty.raw()))
            .ok_or_else(|| anyhow!("exact token/sol quote compare overflow on lhs"))?;
        let rhs = u128::from(other.sol_lamports.as_u64())
            .checked_mul(u128::from(self.token_qty.raw()))
            .ok_or_else(|| anyhow!("exact token/sol quote compare overflow on rhs"))?;
        Ok(lhs.cmp(&rhs))
    }

    pub(crate) fn mark_value_lamports(self, qty: TokenQuantity) -> Result<Lamports> {
        if qty.decimals() != self.token_qty.decimals() {
            return Err(anyhow!(
                "cannot apply exact token/sol quote with token decimals {} to qty decimals {}",
                self.token_qty.decimals(),
                qty.decimals()
            ));
        }
        let numerator = u128::from(qty.raw())
            .checked_mul(u128::from(self.sol_lamports.as_u64()))
            .ok_or_else(|| anyhow!("exact mark-value numerator overflow"))?;
        let denominator = u128::from(self.token_qty.raw());
        let mark_value = numerator / denominator;
        let mark_value = u64::try_from(mark_value)
            .context("exact mark-value overflow while converting to lamports")?;
        Ok(Lamports::new(mark_value))
    }
}

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

    pub(crate) fn reliable_exact_token_sol_price_for_live_unrealized(
        &self,
        token: &str,
        as_of: DateTime<Utc>,
    ) -> Result<Option<ExactTokenSolPriceQuote>> {
        self.reliable_exact_token_sol_price(
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

    fn reliable_exact_token_sol_price(
        &self,
        token: &str,
        as_of: DateTime<Utc>,
        window_minutes: i64,
        min_sol_notional: f64,
        min_samples: usize,
        max_samples: usize,
        context_label: &str,
    ) -> Result<Option<ExactTokenSolPriceQuote>> {
        const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
        const SOL_DECIMALS: i64 = 9;
        let as_of_raw = as_of.to_rfc3339();
        let since_raw = (as_of - Duration::minutes(window_minutes.max(1))).to_rfc3339();
        let min_sol_notional_lamports = super::sol_to_lamports_ceil_storage(
            min_sol_notional,
            "reliable exact token/sol price min_sol_notional",
        )?;
        let mut stmt = self
            .conn
            .prepare(
                "SELECT sol_raw, sol_decimals, token_raw, token_decimals
                 FROM (
                    SELECT qty_in_raw AS sol_raw,
                           qty_in_decimals AS sol_decimals,
                           qty_out_raw AS token_raw,
                           qty_out_decimals AS token_decimals,
                           ts
                    FROM observed_swaps
                    WHERE token_in = ?1
                      AND token_out = ?2
                      AND qty_in_raw IS NOT NULL
                      AND qty_in_decimals IS NOT NULL
                      AND qty_out_raw IS NOT NULL
                      AND qty_out_decimals IS NOT NULL
                      AND ts >= ?3
                      AND ts <= ?4
                    UNION ALL
                    SELECT qty_out_raw AS sol_raw,
                           qty_out_decimals AS sol_decimals,
                           qty_in_raw AS token_raw,
                           qty_in_decimals AS token_decimals,
                           ts
                    FROM observed_swaps
                    WHERE token_in = ?2
                      AND token_out = ?1
                      AND qty_in_raw IS NOT NULL
                      AND qty_in_decimals IS NOT NULL
                      AND qty_out_raw IS NOT NULL
                      AND qty_out_decimals IS NOT NULL
                      AND ts >= ?3
                      AND ts <= ?4
                 )
                 ORDER BY ts DESC
                 LIMIT ?5",
            )
            .with_context(|| {
                format!(
                    "failed to prepare reliable exact {} token/sol price query",
                    context_label
                )
            })?;
        let mut rows = stmt
            .query(params![
                SOL_MINT,
                token,
                since_raw,
                as_of_raw,
                max_samples as i64
            ])
            .with_context(|| {
                format!(
                    "failed querying reliable exact {} token/sol price samples",
                    context_label
                )
            })?;

        let mut samples = Vec::with_capacity(max_samples);
        let mut expected_token_decimals = None;
        while let Some(row) = rows.next().with_context(|| {
            format!(
                "failed iterating reliable exact {} token/sol price samples",
                context_label
            )
        })? {
            let sol_raw: String = row.get(0).context("failed reading exact sol_raw sample")?;
            let sol_decimals: i64 = row
                .get(1)
                .context("failed reading exact sol_decimals sample")?;
            let token_raw: String = row
                .get(2)
                .context("failed reading exact token_raw sample")?;
            let token_decimals_raw: i64 = row
                .get(3)
                .context("failed reading exact token_decimals sample")?;
            if sol_decimals != SOL_DECIMALS {
                continue;
            }
            let sol_raw = sol_raw
                .parse::<u64>()
                .with_context(|| format!("invalid reliable exact sol_raw sample: {:?}", sol_raw))?;
            if sol_raw < min_sol_notional_lamports.as_u64() || sol_raw == 0 {
                continue;
            }
            let token_decimals = u8::try_from(token_decimals_raw).with_context(|| {
                format!(
                    "invalid reliable exact token_decimals sample: {}",
                    token_decimals_raw
                )
            })?;
            if let Some(expected) = expected_token_decimals {
                if token_decimals != expected {
                    continue;
                }
            } else {
                expected_token_decimals = Some(token_decimals);
            }
            let token_raw = token_raw.parse::<u64>().with_context(|| {
                format!("invalid reliable exact token_raw sample: {:?}", token_raw)
            })?;
            if token_raw == 0 {
                continue;
            }
            samples.push(ExactTokenSolPriceQuote::new(
                Lamports::new(sol_raw),
                TokenQuantity::new(token_raw, token_decimals),
            )?);
        }

        if samples.len() < min_samples.max(1) {
            return Ok(None);
        }

        samples.sort_by(|left, right| left.cmp_price(*right).unwrap_or(Ordering::Equal));
        let median_index = if samples.len() % 2 == 1 {
            samples.len() / 2
        } else {
            (samples.len() / 2).saturating_sub(1)
        };
        Ok(samples.get(median_index).copied())
    }
}
