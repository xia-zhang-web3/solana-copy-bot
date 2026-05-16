use crate::observed_budget::{interrupted_after_deadline, sqlite_progress_deadline};
use crate::observed_timestamp::{
    ensure_observed_swaps_timestamps_canonical_utc_read_only, parse_rfc3339_utc,
};
use crate::{SqliteDiscoveryStore, WalletSolLegActivityWindow};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::params;
use std::time::Instant;

impl SqliteDiscoveryStore {
    pub fn wallet_sol_leg_activity_in_window_read_only(
        &self,
        wallet_id: &str,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        deadline: Instant,
    ) -> Result<WalletSolLegActivityWindow> {
        if wallet_id.trim().is_empty()
            || Instant::now() >= deadline
            || !self.sqlite_table_exists("observed_swaps")?
        {
            return Ok(WalletSolLegActivityWindow {
                time_budget_exhausted: Instant::now() >= deadline,
                ..WalletSolLegActivityWindow::default()
            });
        }
        ensure_observed_swaps_timestamps_canonical_utc_read_only(&self.conn)?;
        let _deadline_guard = sqlite_progress_deadline(&self.conn, deadline);
        let row = match self.conn.query_row(
            "SELECT COUNT(*), COUNT(DISTINCT substr(ts, 1, 10)), MIN(ts), MAX(ts)
             FROM observed_swaps INDEXED BY idx_observed_swaps_wallet_ts
             WHERE wallet_id = ?1
               AND ts >= ?2
               AND ts <= ?3
               AND (token_in = 'So11111111111111111111111111111111111111112'
                    OR token_out = 'So11111111111111111111111111111111111111112')",
            params![wallet_id, since.to_rfc3339(), until.to_rfc3339()],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            },
        ) {
            Ok(row) => row,
            Err(error) if interrupted_after_deadline(&error, deadline) => {
                return Ok(WalletSolLegActivityWindow {
                    time_budget_exhausted: true,
                    ..WalletSolLegActivityWindow::default()
                });
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("failed reading SOL-leg activity window for wallet {wallet_id}")
                });
            }
        };
        Ok(WalletSolLegActivityWindow {
            trades: i64_to_u32_saturating(row.0),
            active_days: i64_to_u32_saturating(row.1),
            first_seen: row
                .2
                .as_deref()
                .map(|ts| parse_rfc3339_utc(ts, "observed_swaps.wallet_activity.first_seen"))
                .transpose()?,
            last_seen: row
                .3
                .as_deref()
                .map(|ts| parse_rfc3339_utc(ts, "observed_swaps.wallet_activity.last_seen"))
                .transpose()?,
            time_budget_exhausted: false,
        })
    }
}

fn i64_to_u32_saturating(value: i64) -> u32 {
    u32::try_from(value.max(0)).unwrap_or(u32::MAX)
}
