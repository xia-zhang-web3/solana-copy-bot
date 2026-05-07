use super::{
    canonical_wallet_metrics_window_start, insert_trusted_wallet_metrics_snapshot_on_conn,
};
use crate::{
    FollowlistUpdateResult, SqliteStore, TrustedWalletMetricsSnapshotWrite, WalletMetricRow,
    WalletUpsertRow, DISCOVERY_WALLET_METRICS_RETENTION_WINDOWS,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::params;
use std::collections::HashSet;

impl SqliteStore {
    pub fn persist_discovery_cycle_with_snapshot_metadata(
        &self,
        wallets: &[WalletUpsertRow],
        metrics: &[WalletMetricRow],
        desired_wallets: &[String],
        allow_followlist_activate: bool,
        allow_followlist_deactivate: bool,
        now: DateTime<Utc>,
        reason: &str,
        snapshot_write: Option<&TrustedWalletMetricsSnapshotWrite>,
    ) -> Result<FollowlistUpdateResult> {
        if snapshot_write.is_some() {
            self.ensure_trusted_wallet_metrics_snapshots_table()?;
        }
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
                            canonical_wallet_metrics_window_start(metric.window_start),
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
                     WHERE unixepoch(window_start) < (
                        SELECT cutoff_window_epoch
                        FROM (
                            SELECT unixepoch(window_start) AS cutoff_window_epoch
                            FROM wallet_metrics
                            GROUP BY unixepoch(window_start)
                            ORDER BY cutoff_window_epoch DESC
                            LIMIT 1 OFFSET ?1
                        )
                     )",
                    params![retention_offset],
                )
                .context("failed to apply wallet_metrics retention in discovery transaction")?;
                if let Some(snapshot_write) = snapshot_write {
                    insert_trusted_wallet_metrics_snapshot_on_conn(conn, snapshot_write)?;
                }
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
}
