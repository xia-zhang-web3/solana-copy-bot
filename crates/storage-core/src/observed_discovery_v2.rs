use crate::observed_budget::{interrupted_after_deadline, sqlite_progress_deadline};
use crate::observed_timestamp::ensure_observed_swaps_timestamps_canonical_utc_read_only;
use crate::{DiscoveryV2WalletPrefilter, SqliteDiscoveryStore};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, OptionalExtension};
use std::time::Instant;

impl SqliteDiscoveryStore {
    pub fn discovery_v2_wallet_prefilter_read_only(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        limit: usize,
        min_trades: u32,
        min_buy_count: u32,
        min_active_days: u32,
        min_leader_notional_sol: f64,
        deadline: Instant,
    ) -> Result<DiscoveryV2WalletPrefilter> {
        if limit == 0
            || Instant::now() >= deadline
            || !self.sqlite_table_exists("observed_swaps")?
        {
            return Ok(DiscoveryV2WalletPrefilter {
                time_budget_exhausted: Instant::now() >= deadline,
                ..DiscoveryV2WalletPrefilter::default()
            });
        }
        ensure_observed_swaps_timestamps_canonical_utc_read_only(&self.conn)?;

        let since_raw = since.to_rfc3339();
        let until_raw = until.to_rfc3339();
        let limit = limit.min(i64::MAX as usize) as i64;
        let _deadline_guard = sqlite_progress_deadline(&self.conn, deadline);
        let mut stmt = match self.conn.prepare(
            "WITH limited_window AS (
                 SELECT wallet_id, token_in, token_out, qty_in, qty_out, ts, slot, signature
                 FROM observed_swaps
                 WHERE ts >= ?1
                   AND ts <= ?2
                   AND (token_in = 'So11111111111111111111111111111111111111112'
                        OR token_out = 'So11111111111111111111111111111111111111112')
                 ORDER BY ts ASC, slot ASC, signature ASC
                 LIMIT ?3
             ),
             wallet_stats AS (
                 SELECT
                     wallet_id,
                     COUNT(*) AS trades,
                     SUM(CASE
                         WHEN token_in = 'So11111111111111111111111111111111111111112'
                          AND token_out <> 'So11111111111111111111111111111111111111112'
                         THEN 1
                         ELSE 0
                     END) AS buys,
                     COUNT(DISTINCT substr(ts, 1, 10)) AS active_days,
                     MAX(CASE
                         WHEN token_in = 'So11111111111111111111111111111111111111112'
                          AND token_out <> 'So11111111111111111111111111111111111111112'
                          AND qty_in > 0.0
                          AND qty_out > 0.0
                         THEN qty_in
                         ELSE 0.0
                     END) AS max_buy_notional_sol
                 FROM limited_window
                 GROUP BY wallet_id
             ),
             totals AS (
                 SELECT
                     COALESCE(SUM(trades), 0) AS rows_seen,
                     COUNT(*) AS unique_wallets
                 FROM wallet_stats
             ),
             eligible AS (
                 SELECT wallet_id
                 FROM wallet_stats
                 WHERE trades >= ?4
                   AND buys >= ?5
                   AND active_days >= ?6
                   AND max_buy_notional_sol + 1e-12 >= ?7
             )
             SELECT totals.rows_seen, totals.unique_wallets, eligible.wallet_id
             FROM totals
             LEFT JOIN eligible ON 1 = 1
             ORDER BY eligible.wallet_id",
        ) {
            Ok(stmt) => stmt,
            Err(error) if interrupted_after_deadline(&error, deadline) => {
                return Ok(DiscoveryV2WalletPrefilter {
                    time_budget_exhausted: true,
                    ..DiscoveryV2WalletPrefilter::default()
                });
            }
            Err(error) => return Err(error).context("failed preparing discovery v2 prefilter"),
        };
        let mut rows = match stmt.query(params![
            since_raw,
            until_raw,
            limit,
            i64::from(min_trades),
            i64::from(min_buy_count),
            i64::from(min_active_days),
            min_leader_notional_sol,
        ]) {
            Ok(rows) => rows,
            Err(error) if interrupted_after_deadline(&error, deadline) => {
                return Ok(DiscoveryV2WalletPrefilter {
                    time_budget_exhausted: true,
                    ..DiscoveryV2WalletPrefilter::default()
                });
            }
            Err(error) => return Err(error).context("failed querying discovery v2 prefilter"),
        };

        let mut result = DiscoveryV2WalletPrefilter::default();
        while let Some(row) = match rows.next() {
            Ok(row) => row,
            Err(error) if interrupted_after_deadline(&error, deadline) => {
                return Ok(DiscoveryV2WalletPrefilter {
                    time_budget_exhausted: true,
                    ..result
                });
            }
            Err(error) => return Err(error).context("failed reading discovery v2 prefilter row"),
        } {
            result.rows_seen = row
                .get::<_, i64>(0)
                .context("failed reading discovery v2 prefilter row count")?
                .max(0) as usize;
            result.unique_wallets = row
                .get::<_, i64>(1)
                .context("failed reading discovery v2 prefilter wallet count")?
                .max(0) as usize;
            if let Some(wallet_id) = row
                .get::<_, Option<String>>(2)
                .context("failed reading discovery v2 prefilter wallet id")?
            {
                result.wallet_ids.push(wallet_id);
            }
        }

        if result.rows_seen == 0 && result.unique_wallets == 0 {
            let empty = self
                .conn
                .query_row("SELECT 1 FROM observed_swaps LIMIT 1", [], |row| {
                    row.get::<_, i64>(0)
                })
                .optional()
                .context("failed checking observed_swaps emptiness after discovery v2 prefilter")?
                .is_none();
            if empty {
                return Ok(result);
            }
        }
        Ok(result)
    }
}
