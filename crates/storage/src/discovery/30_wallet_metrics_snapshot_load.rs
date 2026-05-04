impl SqliteStore {
    pub fn load_latest_wallet_metric_snapshots(
        &self,
    ) -> Result<Vec<PersistedWalletMetricSnapshotRow>> {
        let Some(window_start) = self.latest_wallet_metrics_window_start()? else {
            return Ok(Vec::new());
        };
        self.load_wallet_metric_snapshots_for_window(window_start)
    }

    pub fn load_wallet_metric_snapshots_for_window(
        &self,
        window_start: DateTime<Utc>,
    ) -> Result<Vec<PersistedWalletMetricSnapshotRow>> {
        let (canonical, legacy_z) = wallet_metrics_window_start_query_variants(window_start);

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
                 JOIN (
                    SELECT
                        wallet_id,
                        COALESCE(
                            MAX(CASE WHEN window_start = ?1 THEN id END),
                            MAX(id)
                        ) AS selected_id
                    FROM wallet_metrics
                    WHERE window_start IN (?1, ?2)
                    GROUP BY wallet_id
                 ) AS selected_wallet_metrics
                    ON selected_wallet_metrics.selected_id = wallet_metrics.id
                 JOIN wallets ON wallets.wallet_id = wallet_metrics.wallet_id
                 ORDER BY wallet_metrics.score DESC, wallet_metrics.wallet_id ASC",
            )
            .context("failed to prepare wallet_metrics snapshot query for requested window")?;
        let mut rows = stmt
            .query(params![canonical, legacy_z])
            .context("failed querying wallet_metrics snapshots for requested window")?;
        let mut snapshots = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating wallet_metrics snapshots for requested window")?
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
                    "invalid negative wallet_metrics counts in requested snapshot window"
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

    pub fn wallet_metrics_row_count_for_window(
        &self,
        window_start: DateTime<Utc>,
    ) -> Result<usize> {
        let (canonical, legacy_z) = wallet_metrics_window_start_query_variants(window_start);
        let count = self
            .conn
            .query_row(
                "SELECT COUNT(*)
                 FROM (
                    SELECT wallet_id
                    FROM wallet_metrics
                    WHERE window_start IN (?1, ?2)
                    GROUP BY wallet_id
                 )",
                params![canonical, legacy_z],
                |row| row.get::<_, i64>(0),
            )
            .context("failed counting wallet_metrics rows for window")?;
        Ok(count.max(0) as usize)
    }
}
