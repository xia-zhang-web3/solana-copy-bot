use crate::{
    RugWalletFeedback, SqliteDiscoveryStore, SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY,
    SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE, SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE,
    SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::params;
use std::collections::HashMap;

impl SqliteDiscoveryStore {
    pub fn rug_wallet_feedback_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<HashMap<String, RugWalletFeedback>> {
        self.rug_wallet_feedback_window(since, None)
    }

    pub fn rug_wallet_feedback_between(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<HashMap<String, RugWalletFeedback>> {
        if until <= since {
            anyhow::bail!("rug wallet feedback until must be after since");
        }
        self.rug_wallet_feedback_window(since, Some(until))
    }

    fn rug_wallet_feedback_window(
        &self,
        since: DateTime<Utc>,
        until: Option<DateTime<Utc>>,
    ) -> Result<HashMap<String, RugWalletFeedback>> {
        if !self.sqlite_table_exists("shadow_closed_trades")? {
            return Ok(HashMap::new());
        }
        let until_raw = until.map(|until| until.to_rfc3339());

        let mut stmt = self
            .conn
            .prepare(
                "WITH feedback_rows AS (
                    SELECT
                        wallet_id,
                        entry_cost_sol,
                        pnl_sol,
                        CASE
                            WHEN COALESCE(close_context, 'market') IN (?4, ?5, ?6)
                            THEN 1 ELSE 0
                        END AS stale_terminal
                    FROM shadow_closed_trades
                    WHERE closed_ts >= ?1
                      AND (?2 IS NULL OR closed_ts < ?2)
                      AND COALESCE(close_context, 'market') != ?3
                 )
                 SELECT
                    wallet_id,
                    COUNT(*) AS closed_trades,
                    SUM(stale_terminal) AS stale_terminal_closes,
                    SUM(CASE WHEN stale_terminal = 1 THEN pnl_sol ELSE 0.0 END)
                        AS stale_terminal_pnl_sol,
                    SUM(CASE WHEN stale_terminal = 1 THEN entry_cost_sol ELSE 0.0 END)
                        AS stale_terminal_entry_cost_sol
                 FROM feedback_rows
                 GROUP BY wallet_id",
            )
            .context("failed to prepare rug wallet feedback query")?;
        let mut rows = stmt
            .query(params![
                since.to_rfc3339(),
                until_raw.as_deref(),
                SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY,
                SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE,
                SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE,
                SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE,
            ])
            .context("failed querying rug wallet feedback")?;
        let mut feedback = HashMap::<String, RugWalletFeedback>::new();
        while let Some(row) = rows.next().context("failed reading rug feedback row")? {
            let wallet_id: String = row
                .get(0)
                .context("failed reading rug feedback wallet_id")?;
            feedback.insert(
                wallet_id,
                RugWalletFeedback {
                    closed_trades: i64_to_u64(row.get(1)?)?,
                    stale_terminal_closes: i64_to_u64(row.get(2)?)?,
                    stale_terminal_pnl_sol: row
                        .get(3)
                        .context("failed reading rug feedback pnl")?,
                    stale_terminal_entry_cost_sol: row
                        .get(4)
                        .context("failed reading rug feedback entry cost")?,
                },
            );
        }
        Ok(feedback)
    }
}

fn i64_to_u64(value: i64) -> Result<u64> {
    u64::try_from(value).context("negative aggregate count")
}
