impl SqliteStore {
    pub fn has_wallet_scoring_data_since(&self, window_start: DateTime<Utc>) -> Result<bool> {
        let exists = self
            .conn
            .query_row(
                "SELECT EXISTS(
                    SELECT 1
                    FROM wallet_scoring_days
                    WHERE activity_day >= ?1
                )",
                params![window_start.date_naive().format("%Y-%m-%d").to_string()],
                |row| row.get::<_, i64>(0),
            )
            .context("failed querying wallet_scoring_days existence")?;
        Ok(exists != 0)
    }

    pub fn load_wallet_scoring_days_since(
        &self,
        window_start: DateTime<Utc>,
    ) -> Result<Vec<WalletScoringDayRow>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT wallet_id, activity_day, first_seen, last_seen, trades, spent_sol, max_buy_notional_sol
                 FROM wallet_scoring_days
                 WHERE activity_day >= ?1
                 ORDER BY activity_day ASC, wallet_id ASC",
            )
            .context("failed to prepare wallet_scoring_days query")?;
        let mut rows = stmt
            .query(params![window_start
                .date_naive()
                .format("%Y-%m-%d")
                .to_string()])
            .context("failed querying wallet_scoring_days")?;
        let mut out = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating wallet_scoring_days rows")?
        {
            let activity_day_raw: String = row
                .get(1)
                .context("failed reading wallet_scoring_days.activity_day")?;
            let first_seen_raw: String = row
                .get(2)
                .context("failed reading wallet_scoring_days.first_seen")?;
            let last_seen_raw: String = row
                .get(3)
                .context("failed reading wallet_scoring_days.last_seen")?;
            let trades_raw: i64 = row
                .get(4)
                .context("failed reading wallet_scoring_days.trades")?;
            out.push(WalletScoringDayRow {
                wallet_id: row
                    .get(0)
                    .context("failed reading wallet_scoring_days.wallet_id")?,
                activity_day: parse_day(&activity_day_raw, "wallet_scoring_days.activity_day")?,
                first_seen: parse_ts(&first_seen_raw, "wallet_scoring_days.first_seen")?,
                last_seen: parse_ts(&last_seen_raw, "wallet_scoring_days.last_seen")?,
                trades: trades_raw.max(0) as u32,
                spent_sol: row
                    .get(5)
                    .context("failed reading wallet_scoring_days.spent_sol")?,
                max_buy_notional_sol: row
                    .get(6)
                    .context("failed reading wallet_scoring_days.max_buy_notional_sol")?,
            });
        }
        Ok(out)
    }
}
