use super::{upsert_wallet_activity_days_on_conn, wallet_metrics_window_start_query_variants};
use crate::{SqliteStore, WalletActivityDayRow};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, OptionalExtension};
use std::collections::HashMap;

impl SqliteStore {
    pub fn upsert_wallet_activity_days(&self, rows: &[WalletActivityDayRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        self.with_immediate_transaction_retry("wallet_activity_days upsert", |conn| {
            upsert_wallet_activity_days_on_conn(conn, rows)
        })
    }

    pub fn wallet_active_day_counts_since(
        &self,
        wallet_ids: &[String],
        window_start: DateTime<Utc>,
    ) -> Result<HashMap<String, u32>> {
        if wallet_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let day_start = window_start.date_naive();
        let mut counts = HashMap::new();
        for chunk in wallet_ids.chunks(900) {
            let placeholders = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT wallet_id, COUNT(*)
                 FROM wallet_activity_days
                 WHERE (
                        activity_day > ?1
                        OR (activity_day = ?1 AND last_seen >= ?2)
                    )
                   AND wallet_id IN ({placeholders})
                 GROUP BY wallet_id"
            );
            let mut params = vec![
                rusqlite::types::Value::from(day_start.format("%Y-%m-%d").to_string()),
                rusqlite::types::Value::from(window_start.to_rfc3339()),
            ];
            params.extend(chunk.iter().cloned().map(rusqlite::types::Value::from));
            let mut stmt = self
                .conn
                .prepare(&sql)
                .context("failed to prepare wallet_activity_days count query")?;
            let mut rows = stmt
                .query(rusqlite::params_from_iter(params))
                .context("failed querying wallet_activity_days counts")?;
            while let Some(row) = rows
                .next()
                .context("failed iterating wallet_activity_days counts")?
            {
                let wallet_id: String = row
                    .get(0)
                    .context("failed reading wallet_activity_days.wallet_id")?;
                let count: i64 = row
                    .get(1)
                    .context("failed reading wallet_activity_days count")?;
                counts.insert(wallet_id, count.max(0) as u32);
            }
        }

        Ok(counts)
    }

    pub fn backfill_wallet_activity_days_since(
        &self,
        window_start: DateTime<Utc>,
    ) -> Result<usize> {
        self.with_immediate_transaction_retry("wallet_activity_days backfill", |conn| {
            conn.execute(
                "INSERT INTO wallet_activity_days(wallet_id, activity_day, last_seen)
                 SELECT wallet_id, substr(ts, 1, 10) AS activity_day, MAX(ts) AS last_seen
                 FROM observed_swaps
                 WHERE ts >= ?1
                 GROUP BY wallet_id, substr(ts, 1, 10)
                 ON CONFLICT(wallet_id, activity_day) DO UPDATE SET
                    last_seen = CASE
                        WHEN excluded.last_seen > wallet_activity_days.last_seen
                            THEN excluded.last_seen
                        ELSE wallet_activity_days.last_seen
                    END",
                params![window_start.to_rfc3339()],
            )
            .context("failed to backfill wallet_activity_days from observed_swaps")
        })
    }

    pub fn wallet_metrics_window_exists(&self, window_start: DateTime<Utc>) -> Result<bool> {
        let (canonical, legacy_z) = wallet_metrics_window_start_query_variants(window_start);
        let exists = self
            .conn
            .query_row(
                "SELECT EXISTS(
                    SELECT 1
                    FROM wallet_metrics
                    WHERE window_start IN (?1, ?2)
                )",
                params![canonical, legacy_z],
                |row| row.get::<_, i64>(0),
            )
            .context("failed querying wallet_metrics window_start existence")?;
        Ok(exists != 0)
    }

    pub fn latest_wallet_metrics_window_start(&self) -> Result<Option<DateTime<Utc>>> {
        let raw: Option<String> = self
            .conn
            .query_row(
                // Legacy `Z` and canonical `+00:00` differ only in the UTC suffix, so raw
                // RFC3339 string order remains chronological and only affects ties for the
                // same logical instant.
                "SELECT window_start
                 FROM wallet_metrics
                 ORDER BY window_start DESC
                 LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()
            .context("failed querying latest wallet_metrics window_start")?
            .flatten();
        raw.map(|raw| {
            DateTime::parse_from_rfc3339(&raw)
                .map(|dt| dt.with_timezone(&Utc))
                .with_context(|| {
                    format!("invalid wallet_metrics.window_start rfc3339 value: {raw}")
                })
        })
        .transpose()
    }
}
