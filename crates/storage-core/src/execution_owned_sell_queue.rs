use crate::{
    execution_quote_canary::ensure_execution_quote_canary_tables, SqliteDiscoveryStore,
    EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET, EXECUTION_CANARY_POSITION_STATE_OPEN,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, OptionalExtension};

impl SqliteDiscoveryStore {
    pub fn list_execution_quote_canary_owned_sell_signal_candidate_ids(
        &self,
        copy_signal_status: &str,
        since: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<String>> {
        ensure_execution_quote_canary_tables(self)?;
        let cursor: Option<(String, String)> = self
            .conn
            .query_row(
                "SELECT signal_ts, signal_id FROM execution_owned_sell_cursor WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .context("failed loading owned sell recovery cursor")?;
        let limit = limit.max(1);
        let page = |wrap: bool, page_limit: u32| -> Result<Vec<String>> {
            let mut stmt = self
            .conn
            .prepare(
                "SELECT signal.signal_id
                 FROM copy_signals AS signal
                 WHERE ((signal.status = ?1 AND signal.ts >= ?2) OR signal.status = 'execution_sell_intent')
                   AND lower(signal.side) = 'sell'
                   AND EXISTS (
                        SELECT 1
                        FROM positions AS pos
                        LEFT JOIN orders AS buy_order
                          ON pos.position_id = 'exec-canary-pos:' || buy_order.order_id
                        LEFT JOIN copy_signals AS buy_signal
                          ON buy_signal.signal_id = buy_order.signal_id
                        WHERE pos.token = signal.token
                          AND pos.accounting_bucket = ?3
                          AND pos.state = ?4
                          AND signal.ts >= COALESCE((
                              SELECT MAX(COALESCE(latest_buy_signal.ts, latest_buy_order.submit_ts))
                              FROM orders AS latest_buy_order
                              JOIN copy_signals AS latest_buy_signal
                                ON latest_buy_signal.signal_id = latest_buy_order.signal_id
                              WHERE latest_buy_order.order_id LIKE 'exec-canary:%'
                                AND latest_buy_order.status = 'execution_canary_confirmed'
                                AND latest_buy_order.confirm_ts IS NOT NULL
                                AND lower(latest_buy_signal.side) = 'buy'
                                AND latest_buy_signal.token = pos.token
                                AND latest_buy_order.submit_ts >= pos.opened_ts
                          ), CASE
                              WHEN pos.position_id LIKE 'exec-canary-pos:recovery-orphan:%'
                              THEN pos.opened_ts
                              ELSE COALESCE(buy_signal.ts, pos.opened_ts)
                          END)
                   )
                   AND (signal.status = 'execution_sell_intent' OR NOT EXISTS (
                        SELECT 1
                        FROM execution_quote_canary_events AS event
                        WHERE event.signal_id = signal.signal_id
                          AND lower(event.side) = 'sell'
                   ))
                   AND NOT EXISTS (
                        SELECT 1
                        FROM orders
                        WHERE orders.signal_id = signal.signal_id
                   )
                   AND (?6 IS NULL
                        OR (?8 = 0 AND (signal.ts, signal.signal_id) > (?6, ?7))
                        OR (?8 = 1 AND (signal.ts, signal.signal_id) <= (?6, ?7)))
                 ORDER BY signal.ts ASC, signal.signal_id ASC
                 LIMIT ?5",
            )
            .context("failed to prepare owned sell signal quote canary candidate query")?;
            let rows = stmt
                .query_map(
                    params![
                        copy_signal_status,
                        since.to_rfc3339(),
                        EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
                        EXECUTION_CANARY_POSITION_STATE_OPEN,
                        i64::from(page_limit),
                        cursor.as_ref().map(|c| c.0.as_str()),
                        cursor.as_ref().map(|c| c.1.as_str()),
                        wrap,
                    ],
                    |row| row.get(0),
                )
                .context("failed querying owned sell signal quote canary candidates")?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .context("failed reading owned sell signal quote canary candidates")
        };
        let mut ids = page(false, limit)?;
        if cursor.is_some() && ids.len() < limit as usize {
            ids.extend(page(true, limit - ids.len() as u32)?);
        }
        Ok(ids)
    }

    /// Commit traversal before loading/building a candidate, including a failed attempt.
    /// This is not a reservation or a grant; all execution guards still run afterwards.
    pub fn advance_execution_owned_sell_cursor(&self, signal_id: &str) -> Result<()> {
        self.execute_with_retry(|conn| conn.execute(
            "INSERT INTO execution_owned_sell_cursor(singleton, signal_ts, signal_id)
             SELECT 1, ts, signal_id FROM copy_signals WHERE signal_id = ?1 AND lower(side) = 'sell'
             ON CONFLICT(singleton) DO UPDATE SET signal_ts = excluded.signal_ts, signal_id = excluded.signal_id",
            [signal_id])).context("failed advancing owned sell recovery cursor")?;
        Ok(())
    }
}
