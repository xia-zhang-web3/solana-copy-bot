use super::{
    FollowlistUpdateResult, SqliteStore, WalletMetricRow, WalletUpsertRow,
    DISCOVERY_WALLET_METRICS_RETENTION_WINDOWS,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, OptionalExtension};
use std::collections::HashSet;

impl SqliteStore {
    pub fn upsert_wallet(
        &self,
        wallet_id: &str,
        first_seen: DateTime<Utc>,
        last_seen: DateTime<Utc>,
        status: &str,
    ) -> Result<()> {
        self.conn
            .execute(
                "INSERT INTO wallets(wallet_id, first_seen, last_seen, status)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(wallet_id) DO UPDATE SET
                    first_seen = CASE WHEN excluded.first_seen < wallets.first_seen THEN excluded.first_seen ELSE wallets.first_seen END,
                    last_seen = CASE WHEN excluded.last_seen > wallets.last_seen THEN excluded.last_seen ELSE wallets.last_seen END,
                    status = excluded.status",
                params![
                    wallet_id,
                    first_seen.to_rfc3339(),
                    last_seen.to_rfc3339(),
                    status,
                ],
            )
            .context("failed to upsert wallet")?;
        Ok(())
    }

    pub fn insert_wallet_metric(&self, metric: &WalletMetricRow) -> Result<()> {
        self.conn
            .execute(
                "INSERT INTO wallet_metrics(
                    wallet_id,
                    window_start,
                    pnl,
                    win_rate,
                    trades,
                    closed_trades,
                    hold_median_seconds,
                    score,
                    buy_total,
                    tradable_ratio,
                    rug_ratio
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    &metric.wallet_id,
                    metric.window_start.to_rfc3339(),
                    metric.pnl,
                    metric.win_rate,
                    metric.trades as i64,
                    metric.closed_trades as i64,
                    metric.hold_median_seconds,
                    metric.score,
                    metric.buy_total as i64,
                    metric.tradable_ratio,
                    metric.rug_ratio,
                ],
            )
            .context("failed to insert wallet metric")?;
        Ok(())
    }

    pub fn persist_discovery_cycle(
        &self,
        wallets: &[WalletUpsertRow],
        metrics: &[WalletMetricRow],
        desired_wallets: &[String],
        now: DateTime<Utc>,
        reason: &str,
    ) -> Result<FollowlistUpdateResult> {
        let tx = self
            .conn
            .unchecked_transaction()
            .context("failed to open discovery write transaction")?;

        {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT INTO wallets(wallet_id, first_seen, last_seen, status)
                     VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT(wallet_id) DO UPDATE SET
                        first_seen = CASE WHEN excluded.first_seen < wallets.first_seen THEN excluded.first_seen ELSE wallets.first_seen END,
                        last_seen = CASE WHEN excluded.last_seen > wallets.last_seen THEN excluded.last_seen ELSE wallets.last_seen END,
                        status = excluded.status",
                )
                .context("failed to prepare discovery wallet upsert statement")?;
            for wallet in wallets {
                stmt.execute(params![
                    &wallet.wallet_id,
                    wallet.first_seen.to_rfc3339(),
                    wallet.last_seen.to_rfc3339(),
                    &wallet.status,
                ])
                .context("failed to upsert wallet in discovery transaction")?;
            }
        }

        {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT INTO wallet_metrics(
                        wallet_id,
                        window_start,
                        pnl,
                        win_rate,
                        trades,
                        closed_trades,
                        hold_median_seconds,
                        score,
                        buy_total,
                        tradable_ratio,
                        rug_ratio
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                )
                .context("failed to prepare discovery wallet metric insert statement")?;
            for metric in metrics {
                stmt.execute(params![
                    &metric.wallet_id,
                    metric.window_start.to_rfc3339(),
                    metric.pnl,
                    metric.win_rate,
                    metric.trades as i64,
                    metric.closed_trades as i64,
                    metric.hold_median_seconds,
                    metric.score,
                    metric.buy_total as i64,
                    metric.tradable_ratio,
                    metric.rug_ratio,
                ])
                .context("failed to insert wallet metric in discovery transaction")?;
            }
        }

        tx.execute(
            "DELETE FROM wallet_metrics
             WHERE window_start NOT IN (
                SELECT window_start
                FROM wallet_metrics
                GROUP BY window_start
                ORDER BY window_start DESC
                LIMIT ?1
             )",
            params![DISCOVERY_WALLET_METRICS_RETENTION_WINDOWS],
        )
        .context("failed to apply wallet_metrics retention in discovery transaction")?;

        let desired: HashSet<&str> = desired_wallets.iter().map(String::as_str).collect();
        let active_wallets: Vec<String> = {
            let mut stmt = tx
                .prepare("SELECT wallet_id FROM followlist WHERE active = 1")
                .context("failed to prepare active followlist query in discovery transaction")?;
            let mut rows = stmt
                .query([])
                .context("failed querying active followlist in discovery transaction")?;
            let mut wallets = Vec::new();
            while let Some(row) = rows
                .next()
                .context("failed iterating active followlist in discovery transaction")?
            {
                wallets.push(
                    row.get(0)
                        .context("failed reading followlist.wallet_id in discovery transaction")?,
                );
            }
            wallets
        };

        let now_raw = now.to_rfc3339();
        let mut result = FollowlistUpdateResult::default();
        {
            let mut deactivate_stmt = tx
                .prepare_cached(
                    "UPDATE followlist
                     SET active = 0, removed_at = ?1, reason = ?2
                     WHERE wallet_id = ?3 AND active = 1",
                )
                .context("failed to prepare followlist deactivate statement")?;
            for wallet_id in active_wallets.iter() {
                if !desired.contains(wallet_id.as_str()) {
                    let changed = deactivate_stmt
                        .execute(params![&now_raw, reason, wallet_id])
                        .context("failed to deactivate follow wallet in discovery transaction")?;
                    if changed > 0 {
                        result.deactivated += 1;
                    }
                }
            }
        }

        {
            let mut exists_stmt = tx
                .prepare_cached(
                    "SELECT id FROM followlist WHERE wallet_id = ?1 AND active = 1 LIMIT 1",
                )
                .context("failed to prepare followlist active check statement")?;
            let mut activate_stmt = tx
                .prepare_cached(
                    "INSERT INTO followlist(wallet_id, added_at, reason, active)
                     VALUES (?1, ?2, ?3, 1)",
                )
                .context("failed to prepare followlist activate statement")?;
            for wallet_id in desired_wallets {
                let already_active: Option<i64> = exists_stmt
                    .query_row(params![wallet_id], |row| row.get(0))
                    .optional()
                    .context("failed checking existing active follow wallet in transaction")?;
                if already_active.is_none() {
                    activate_stmt
                        .execute(params![wallet_id, &now_raw, reason])
                        .context("failed to activate follow wallet in discovery transaction")?;
                    result.activated += 1;
                }
            }
        }

        tx.commit()
            .context("failed to commit discovery write transaction")?;
        Ok(result)
    }

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
        let already_active: Option<i64> = self
            .conn
            .query_row(
                "SELECT id FROM followlist WHERE wallet_id = ?1 AND active = 1 LIMIT 1",
                params![wallet_id],
                |row| row.get(0),
            )
            .optional()
            .context("failed checking existing active follow entry")?;
        if already_active.is_some() {
            return Ok(false);
        }

        self.conn
            .execute(
                "INSERT INTO followlist(wallet_id, added_at, reason, active)
                 VALUES (?1, ?2, ?3, 1)",
                params![wallet_id, now.to_rfc3339(), reason],
            )
            .context("failed to activate follow wallet")?;
        Ok(true)
    }

    pub fn reconcile_followlist(
        &self,
        desired_wallets: &[String],
        now: DateTime<Utc>,
        reason: &str,
    ) -> Result<FollowlistUpdateResult> {
        let desired: HashSet<&str> = desired_wallets.iter().map(String::as_str).collect();
        let active = self.list_active_follow_wallets()?;
        let mut result = FollowlistUpdateResult::default();

        for wallet_id in active.iter() {
            if !desired.contains(wallet_id.as_str())
                && self.deactivate_follow_wallet(wallet_id, now, reason)?
            {
                result.deactivated += 1;
            }
        }

        for wallet_id in desired_wallets {
            if self.activate_follow_wallet(wallet_id, now, reason)? {
                result.activated += 1;
            }
        }

        Ok(result)
    }
}
