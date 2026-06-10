use crate::{
    observed_timestamp::parse_rfc3339_utc, SqliteDiscoveryStore, EXECUTION_STATUS_CANARY_BUILT,
    EXECUTION_STATUS_CANARY_CANDIDATE, EXECUTION_STATUS_CANARY_CONFIRMED,
    EXECUTION_STATUS_CANARY_SIMULATED, EXECUTION_STATUS_CANARY_SUBMITTED,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::params;

impl SqliteDiscoveryStore {
    pub fn latest_live_execution_canary_buy_signal_ts(
        &self,
        token: &str,
    ) -> Result<Option<DateTime<Utc>>> {
        let raw: Option<String> = self
            .conn
            .query_row(
                "SELECT MAX(signal.ts)
                 FROM orders AS orders
                 JOIN copy_signals AS signal
                   ON signal.signal_id = orders.signal_id
                 WHERE orders.order_id LIKE 'exec-canary:%'
                   AND lower(signal.side) = 'buy'
                   AND signal.token = ?1
                   AND orders.status IN (?2, ?3, ?4, ?5, ?6)",
                params![
                    token,
                    EXECUTION_STATUS_CANARY_CANDIDATE,
                    EXECUTION_STATUS_CANARY_BUILT,
                    EXECUTION_STATUS_CANARY_SIMULATED,
                    EXECUTION_STATUS_CANARY_SUBMITTED,
                    EXECUTION_STATUS_CANARY_CONFIRMED,
                ],
                |row| row.get::<_, Option<String>>(0),
            )
            .context("failed loading latest live execution canary buy signal timestamp")?;
        raw.as_deref()
            .map(|ts| parse_rfc3339_utc(ts, "latest live execution canary buy signal ts"))
            .transpose()
    }

    pub fn has_later_copy_sell_signal(
        &self,
        token: &str,
        buy_signal_ts: DateTime<Utc>,
    ) -> Result<bool> {
        let count: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*)
                 FROM copy_signals
                 WHERE token = ?1
                   AND lower(side) = 'sell'
                   AND ts > ?2",
                params![token, buy_signal_ts.to_rfc3339()],
                |row| row.get(0),
            )
            .context("failed checking later copy sell signal")?;
        Ok(count > 0)
    }
}
