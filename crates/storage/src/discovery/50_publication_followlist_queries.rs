use super::parse_rfc3339_utc;
use crate::{SqliteStore, WalletRecentActivityCountRow};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, OptionalExtension};
use std::collections::HashSet;

impl SqliteStore {
    pub fn list_active_follow_wallets(&self) -> Result<HashSet<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT wallet_id FROM followlist WHERE active = 1")
            .context("failed to prepare active followlist query")?;
        let mut rows = stmt
            .query([])
            .context("failed querying active followlist entries")?;

        let mut wallets = HashSet::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating active followlist entries")?
        {
            let wallet_id: String = row.get(0).context("failed reading followlist.wallet_id")?;
            wallets.insert(wallet_id);
        }
        Ok(wallets)
    }

    pub fn was_wallet_followed_at(&self, wallet_id: &str, ts: DateTime<Utc>) -> Result<bool> {
        let ts_raw = ts.to_rfc3339();
        let exists: Option<i64> = self
            .conn
            .query_row(
                "SELECT 1
                 FROM followlist
                 WHERE wallet_id = ?1
                   AND added_at <= ?2
                   AND (removed_at IS NULL OR ?2 < removed_at)
                 LIMIT 1",
                params![wallet_id, ts_raw],
                |row| row.get(0),
            )
            .optional()
            .context("failed checking temporal followlist membership")?;
        Ok(exists.is_some())
    }

    pub fn deactivate_follow_wallet(
        &self,
        wallet_id: &str,
        now: DateTime<Utc>,
        reason: &str,
    ) -> Result<bool> {
        let changed = self
            .conn
            .execute(
                "UPDATE followlist
                 SET active = 0, removed_at = ?1, reason = ?2
                 WHERE wallet_id = ?3 AND active = 1",
                params![now.to_rfc3339(), reason, wallet_id],
            )
            .context("failed to deactivate follow wallet")?;
        Ok(changed > 0)
    }

    pub fn activate_follow_wallet(
        &self,
        wallet_id: &str,
        now: DateTime<Utc>,
        reason: &str,
    ) -> Result<bool> {
        let changed = self
            .execute_with_retry(|conn| {
                conn.execute(
                    "INSERT OR IGNORE INTO followlist(wallet_id, added_at, reason, active)
                     VALUES (?1, ?2, ?3, 1)",
                    params![wallet_id, now.to_rfc3339(), reason],
                )
            })
            .context("failed to activate follow wallet")?;
        Ok(changed > 0)
    }

    pub fn recent_copy_signal_counts_for_wallets_by_status(
        &self,
        since: DateTime<Utc>,
        wallet_ids: &[String],
        status: &str,
    ) -> Result<Vec<WalletRecentActivityCountRow>> {
        if wallet_ids.is_empty() {
            return Ok(Vec::new());
        }

        let placeholders = std::iter::repeat_n("?", wallet_ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!(
            "SELECT wallet_id, COUNT(*), MAX(ts)
             FROM copy_signals
             WHERE status = ?1
               AND ts >= ?2
               AND wallet_id IN ({placeholders})
             GROUP BY wallet_id
             ORDER BY wallet_id ASC"
        );
        let mut params = vec![
            rusqlite::types::Value::from(status.to_string()),
            rusqlite::types::Value::from(since.to_rfc3339()),
        ];
        params.extend(wallet_ids.iter().cloned().map(rusqlite::types::Value::from));
        let mut stmt = self
            .conn
            .prepare(&query)
            .context("failed to prepare recent copy_signals wallet activity query")?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params))
            .context("failed querying recent copy_signals wallet activity")?;

        let mut summaries = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating recent copy_signals wallet activity rows")?
        {
            let wallet_id: String = row
                .get(0)
                .context("failed reading recent copy_signals wallet_id")?;
            let row_count_raw: i64 = row
                .get(1)
                .context("failed reading recent copy_signals row_count")?;
            let latest_ts_raw: String = row
                .get(2)
                .context("failed reading recent copy_signals latest_ts")?;
            summaries.push(WalletRecentActivityCountRow {
                wallet_id,
                row_count: row_count_raw.max(0) as usize,
                latest_ts: parse_rfc3339_utc(&latest_ts_raw, "recent copy_signals latest_ts")?,
            });
        }
        Ok(summaries)
    }
}
