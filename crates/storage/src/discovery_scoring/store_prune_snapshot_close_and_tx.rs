use super::*;

impl SqliteStore {
    pub fn load_wallet_scoring_close_facts_since(
        &self,
        window_start: DateTime<Utc>,
    ) -> Result<Vec<WalletScoringCloseFactRow>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT wallet_id, token, closed_ts, pnl_sol, hold_seconds, win
                 FROM wallet_scoring_close_facts
                 WHERE closed_ts >= ?1
                 ORDER BY closed_ts ASC, wallet_id ASC",
            )
            .context("failed to prepare wallet_scoring_close_facts query")?;
        let mut rows = stmt
            .query(params![window_start.to_rfc3339()])
            .context("failed querying wallet_scoring_close_facts")?;
        let mut out = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating wallet_scoring_close_facts rows")?
        {
            let closed_ts_raw: String = row
                .get(2)
                .context("failed reading wallet_scoring_close_facts.closed_ts")?;
            let hold_seconds_raw: i64 = row
                .get(4)
                .context("failed reading wallet_scoring_close_facts.hold_seconds")?;
            let win_raw: i64 = row
                .get(5)
                .context("failed reading wallet_scoring_close_facts.win")?;
            out.push(WalletScoringCloseFactRow {
                wallet_id: row
                    .get(0)
                    .context("failed reading wallet_scoring_close_facts.wallet_id")?,
                token: row
                    .get(1)
                    .context("failed reading wallet_scoring_close_facts.token")?,
                closed_ts: parse_ts(&closed_ts_raw, "wallet_scoring_close_facts.closed_ts")?,
                pnl_sol: row
                    .get(3)
                    .context("failed reading wallet_scoring_close_facts.pnl_sol")?,
                hold_seconds: hold_seconds_raw.max(0),
                win: win_raw != 0,
            });
        }
        Ok(out)
    }

    pub fn wallet_scoring_max_tx_counts_since(
        &self,
        window_start: DateTime<Utc>,
    ) -> Result<HashMap<String, u32>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT wallet_id, MAX(tx_count)
                 FROM wallet_scoring_tx_minutes
                 WHERE minute_bucket >= ?1
                 GROUP BY wallet_id",
            )
            .context("failed to prepare wallet_scoring_tx_minutes max query")?;
        let mut rows = stmt
            .query(params![window_start.timestamp().div_euclid(60)])
            .context("failed querying wallet_scoring_tx_minutes maxima")?;
        let mut out = HashMap::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating wallet_scoring_tx_minutes maxima")?
        {
            let wallet_id: String = row
                .get(0)
                .context("failed reading wallet_scoring_tx_minutes.wallet_id")?;
            let max_count_raw: i64 = row
                .get(1)
                .context("failed reading wallet_scoring_tx_minutes max(tx_count)")?;
            out.insert(wallet_id, max_count_raw.max(0) as u32);
        }
        Ok(out)
    }

    pub fn load_wallet_scoring_snapshot_since(
        &self,
        window_start: DateTime<Utc>,
    ) -> Result<WalletScoringSnapshot> {
        self.conn
            .execute_batch("BEGIN DEFERRED TRANSACTION")
            .context("failed to open deferred wallet_scoring snapshot transaction")?;
        let snapshot_result = (|| -> Result<WalletScoringSnapshot> {
            Ok(WalletScoringSnapshot {
                days: self.load_wallet_scoring_days_since(window_start)?,
                buy_facts: self.load_wallet_scoring_buy_facts_since(window_start)?,
                close_facts: self.load_wallet_scoring_close_facts_since(window_start)?,
                max_tx_counts: self.wallet_scoring_max_tx_counts_since(window_start)?,
            })
        })();

        match snapshot_result {
            Ok(snapshot) => {
                self.conn
                    .execute_batch("COMMIT")
                    .context("failed to commit deferred wallet_scoring snapshot transaction")?;
                Ok(snapshot)
            }
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }
}
