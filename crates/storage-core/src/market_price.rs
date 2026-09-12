use crate::observed_timestamp::{
    ensure_observed_swaps_timestamps_canonical_utc_read_only, parse_rfc3339_utc,
};
use crate::SqliteDiscoveryStore;
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

/// Observed SOL per token, with the identity of the selected observed_swaps row.
/// This is neither a quote nor an executable liquidation price.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObservedTokenSolPrice {
    pub price_sol: f64,
    pub observed_at: DateTime<Utc>,
    pub signature: String,
    pub slot: u64,
}

// Each leg uses an existing (token_in, token_out, ts) / reverse index. Limit each
// leg before merging; ties have an explicit stable slot/signature ordering.
const LATEST_PRICE_SQL: &str = r#"
SELECT price, ts, signature, slot FROM (
    SELECT * FROM (
        SELECT qty_in / qty_out AS price, ts, signature, slot
        FROM observed_swaps
        WHERE token_in = ?1 AND token_out = ?2
          AND qty_in > 0 AND qty_out > 0 AND ts <= ?3
        ORDER BY ts DESC, slot DESC, signature DESC LIMIT 1
    )
    UNION ALL
    SELECT * FROM (
        SELECT qty_out / qty_in AS price, ts, signature, slot
        FROM observed_swaps
        WHERE token_in = ?2 AND token_out = ?1
          AND qty_in > 0 AND qty_out > 0 AND ts <= ?3
        ORDER BY ts DESC, slot DESC, signature DESC LIMIT 1
    )
)
ORDER BY ts DESC, slot DESC, signature DESC LIMIT 1
"#;

impl SqliteDiscoveryStore {
    /// Read-only provenance-preserving counterpart. The legacy scalar reader is
    /// deliberately unchanged, including its historical tie/selection semantics.
    pub fn latest_token_sol_price_observation(
        &self,
        token: &str,
        as_of: DateTime<Utc>,
    ) -> Result<Option<ObservedTokenSolPrice>> {
        ensure_observed_swaps_timestamps_canonical_utc_read_only(&self.conn)?;
        let row: Option<(f64, String, String, i64)> = self
            .conn
            .query_row(
                LATEST_PRICE_SQL,
                params![
                    SOL_MINT,
                    token,
                    as_of.to_rfc3339_opts(SecondsFormat::Nanos, false)
                ],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .context("failed reading observed token price provenance")?;
        let Some((price_sol, timestamp, signature, slot)) = row else {
            return Ok(None);
        };
        let observed_at = parse_rfc3339_utc(&timestamp, "observed_swaps.ts")?;
        ensure!(observed_at <= as_of, "observed token price is future-dated");
        ensure!(
            !signature.is_empty() && signature.len() <= 128,
            "observed token price identity is invalid"
        );
        let slot = u64::try_from(slot).context("observed token price slot is invalid")?;
        Ok(
            (price_sol.is_finite() && price_sol > 0.0).then_some(ObservedTokenSolPrice {
                price_sol,
                observed_at,
                signature,
                slot,
            }),
        )
    }
}
