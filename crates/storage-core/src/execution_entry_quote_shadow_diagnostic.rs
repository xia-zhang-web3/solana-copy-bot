use crate::{
    execution_quote_canary::ensure_execution_quote_canary_tables,
    observed_timestamp::parse_rfc3339_utc, SqliteDiscoveryStore,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::{CopySignalRow, Lamports};
use rusqlite::params;

impl SqliteDiscoveryStore {
    pub fn list_entry_quote_shadow_diagnostic_candidates(
        &self,
        copy_signal_status: &str,
        since: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<CopySignalRow>> {
        ensure_execution_quote_canary_tables(self)?;
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    signal_id,
                    wallet_id,
                    side,
                    token,
                    notional_sol,
                    notional_lamports,
                    notional_origin,
                    ts,
                    status
                 FROM copy_signals
                 WHERE status = ?1
                   AND ts >= ?2
                   AND lower(side) = 'buy'
                   AND NOT EXISTS (
                        SELECT 1 FROM execution_quote_canary_events
                        WHERE execution_quote_canary_events.event_id =
                              'quote:entry-shadow-diag:' || copy_signals.signal_id
                          AND lower(execution_quote_canary_events.side) = 'buy'
                   )
                 ORDER BY ts ASC
                 LIMIT ?3",
            )
            .context("failed to prepare entry quote shadow diagnostic candidate query")?;
        let mut rows = stmt
            .query(params![
                copy_signal_status,
                since.to_rfc3339(),
                limit.max(1) as i64
            ])
            .context("failed querying entry quote shadow diagnostic candidates")?;

        let mut signals = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating entry quote shadow diagnostic candidates")?
        {
            signals.push(copy_signal_from_row(row)?);
        }
        Ok(signals)
    }
}

fn copy_signal_from_row(row: &rusqlite::Row<'_>) -> Result<CopySignalRow> {
    let ts_raw: String = row.get(7).context("failed reading copy_signals.ts")?;
    let notional_lamports_raw: Option<i64> = row
        .get(5)
        .context("failed reading copy_signals.notional_lamports")?;
    let notional_lamports = notional_lamports_raw
        .map(|value| {
            u64::try_from(value)
                .map(Lamports::new)
                .with_context(|| format!("invalid copy_signals.notional_lamports: {value}"))
        })
        .transpose()?;
    Ok(CopySignalRow {
        signal_id: row
            .get(0)
            .context("failed reading copy_signals.signal_id")?,
        wallet_id: row
            .get(1)
            .context("failed reading copy_signals.wallet_id")?,
        side: row.get(2).context("failed reading copy_signals.side")?,
        token: row.get(3).context("failed reading copy_signals.token")?,
        notional_sol: row
            .get(4)
            .context("failed reading copy_signals.notional_sol")?,
        notional_lamports,
        notional_origin: row
            .get(6)
            .context("failed reading copy_signals.notional_origin")?,
        ts: parse_rfc3339_utc(&ts_raw, "copy_signals.ts")?,
        status: row.get(8).context("failed reading copy_signals.status")?,
    })
}
