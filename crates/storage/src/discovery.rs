use super::{
    FollowlistUpdateResult, PersistedWalletMetricSnapshotRow, SqliteStore, WalletActivityDayRow,
    WalletMetricRow, WalletUpsertRow, DISCOVERY_WALLET_METRICS_RETENTION_WINDOWS,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::{HashMap, HashSet};

pub(crate) fn upsert_wallet_activity_days_on_conn(
    conn: &Connection,
    rows: &[WalletActivityDayRow],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let mut stmt = conn
        .prepare_cached(
            "INSERT INTO wallet_activity_days(wallet_id, activity_day, last_seen)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(wallet_id, activity_day) DO UPDATE SET
                last_seen = CASE
                    WHEN excluded.last_seen > wallet_activity_days.last_seen
                        THEN excluded.last_seen
                    ELSE wallet_activity_days.last_seen
                END",
        )
        .context("failed to prepare wallet_activity_days upsert statement")?;
    for row in rows {
        stmt.execute(params![
            &row.wallet_id,
            row.activity_day.format("%Y-%m-%d").to_string(),
            row.last_seen.to_rfc3339(),
        ])
        .context("failed to upsert wallet_activity_days row")?;
    }
    Ok(())
}

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
        let exists = self
            .conn
            .query_row(
                "SELECT EXISTS(
                    SELECT 1
                    FROM wallet_metrics
                    WHERE window_start = ?1
                )",
                params![window_start.to_rfc3339()],
                |row| row.get::<_, i64>(0),
            )
            .context("failed querying wallet_metrics window_start existence")?;
        Ok(exists != 0)
    }

    pub fn latest_wallet_metrics_window_start(&self) -> Result<Option<DateTime<Utc>>> {
        let raw: Option<String> = self
            .conn
            .query_row("SELECT MAX(window_start) FROM wallet_metrics", [], |row| {
                row.get(0)
            })
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

    pub fn load_latest_wallet_metric_snapshots(
        &self,
    ) -> Result<Vec<PersistedWalletMetricSnapshotRow>> {
        let Some(window_start) = self.latest_wallet_metrics_window_start()? else {
            return Ok(Vec::new());
        };

        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    wallet_metrics.wallet_id,
                    wallets.first_seen,
                    wallets.last_seen,
                    wallet_metrics.pnl,
                    wallet_metrics.win_rate,
                    wallet_metrics.trades,
                    wallet_metrics.closed_trades,
                    wallet_metrics.hold_median_seconds,
                    wallet_metrics.score,
                    wallet_metrics.buy_total,
                    wallet_metrics.tradable_ratio,
                    wallet_metrics.rug_ratio
                 FROM wallet_metrics
                 JOIN wallets ON wallets.wallet_id = wallet_metrics.wallet_id
                 WHERE wallet_metrics.window_start = ?1",
            )
            .context("failed to prepare latest wallet_metrics snapshot query")?;
        let mut rows = stmt
            .query(params![window_start.to_rfc3339()])
            .context("failed querying latest wallet_metrics snapshots")?;
        let mut snapshots = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating latest wallet_metrics snapshots")?
        {
            let first_seen_raw: String = row.get(1).context("failed reading wallets.first_seen")?;
            let last_seen_raw: String = row.get(2).context("failed reading wallets.last_seen")?;
            let first_seen = DateTime::parse_from_rfc3339(&first_seen_raw)
                .map(|dt| dt.with_timezone(&Utc))
                .with_context(|| {
                    format!("invalid wallets.first_seen rfc3339 value: {first_seen_raw}")
                })?;
            let last_seen = DateTime::parse_from_rfc3339(&last_seen_raw)
                .map(|dt| dt.with_timezone(&Utc))
                .with_context(|| {
                    format!("invalid wallets.last_seen rfc3339 value: {last_seen_raw}")
                })?;
            let trades: i64 = row.get(5).context("failed reading wallet_metrics.trades")?;
            let closed_trades: i64 = row
                .get(6)
                .context("failed reading wallet_metrics.closed_trades")?;
            let buy_total: i64 = row
                .get(9)
                .context("failed reading wallet_metrics.buy_total")?;
            if trades < 0 || closed_trades < 0 || buy_total < 0 {
                return Err(anyhow::anyhow!(
                    "invalid negative wallet_metrics counts in latest snapshot window"
                ));
            }
            snapshots.push(PersistedWalletMetricSnapshotRow {
                wallet_id: row
                    .get(0)
                    .context("failed reading wallet_metrics.wallet_id")?,
                window_start,
                first_seen,
                last_seen,
                pnl: row.get(3).context("failed reading wallet_metrics.pnl")?,
                win_rate: row
                    .get(4)
                    .context("failed reading wallet_metrics.win_rate")?,
                trades: trades as u32,
                closed_trades: closed_trades as u32,
                hold_median_seconds: row
                    .get(7)
                    .context("failed reading wallet_metrics.hold_median_seconds")?,
                score: row.get(8).context("failed reading wallet_metrics.score")?,
                buy_total: buy_total as u32,
                tradable_ratio: row
                    .get(10)
                    .context("failed reading wallet_metrics.tradable_ratio")?,
                rug_ratio: row
                    .get(11)
                    .context("failed reading wallet_metrics.rug_ratio")?,
            });
        }

        Ok(snapshots)
    }

    pub fn upsert_wallet(
        &self,
        wallet_id: &str,
        first_seen: DateTime<Utc>,
        last_seen: DateTime<Utc>,
        status: &str,
    ) -> Result<()> {
        self.execute_with_retry(|conn| {
            conn.execute(
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
        })
            .context("failed to upsert wallet")?;
        Ok(())
    }

    pub fn insert_wallet_metric(&self, metric: &WalletMetricRow) -> Result<()> {
        self.execute_with_retry(|conn| {
            conn.execute(
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
        })
        .context("failed to insert wallet metric")?;
        Ok(())
    }

    pub fn persist_discovery_cycle(
        &self,
        wallets: &[WalletUpsertRow],
        metrics: &[WalletMetricRow],
        desired_wallets: &[String],
        allow_followlist_activate: bool,
        allow_followlist_deactivate: bool,
        now: DateTime<Utc>,
        reason: &str,
    ) -> Result<FollowlistUpdateResult> {
        self.with_immediate_transaction_retry("discovery write", |conn| {
            let retention_offset = DISCOVERY_WALLET_METRICS_RETENTION_WINDOWS.saturating_sub(1);
            {
                let mut stmt = conn
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

            if !metrics.is_empty() {
                {
                    let mut stmt = conn
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

                conn.execute(
                    "DELETE FROM wallet_metrics
                     WHERE window_start < (
                        SELECT DISTINCT window_start
                        FROM wallet_metrics
                        ORDER BY window_start DESC
                        LIMIT 1 OFFSET ?1
                     )",
                    params![retention_offset],
                )
                .context("failed to apply wallet_metrics retention in discovery transaction")?;
            }

            let desired: HashSet<&str> = desired_wallets.iter().map(String::as_str).collect();
            let active_wallets: Vec<String> = {
                let mut stmt = conn
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
                        row.get(0).context(
                            "failed reading followlist.wallet_id in discovery transaction",
                        )?,
                    );
                }
                wallets
            };

            let now_raw = now.to_rfc3339();
            let mut result = FollowlistUpdateResult::default();
            if allow_followlist_deactivate {
                let mut deactivate_stmt = conn
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
                            .context(
                                "failed to deactivate follow wallet in discovery transaction",
                            )?;
                        if changed > 0 {
                            result.deactivated += 1;
                        }
                    }
                }
            }

            if allow_followlist_activate {
                let mut activate_stmt = conn
                    .prepare_cached(
                    "INSERT OR IGNORE INTO followlist(wallet_id, added_at, reason, active)
                     VALUES (?1, ?2, ?3, 1)",
                    )
                    .context("failed to prepare followlist activate statement")?;
                for wallet_id in desired_wallets {
                    let changed = activate_stmt
                        .execute(params![wallet_id, &now_raw, reason])
                        .context("failed to activate follow wallet in discovery transaction")?;
                    if changed > 0 {
                        result.activated += 1;
                    }
                }
            }

            Ok(result)
        })
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
}
