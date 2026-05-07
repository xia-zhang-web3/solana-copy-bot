use crate::{money::u64_to_sql_i64, SqliteDiscoveryStore};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::{
    CopySignalRow, Lamports, COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE,
    COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS,
};
use rusqlite::params;

impl SqliteDiscoveryStore {
    pub fn insert_copy_signal(&self, signal: &CopySignalRow) -> Result<bool> {
        let notional_origin = signal.notional_origin.as_str();
        if signal
            .notional_lamports
            .is_some_and(|value| value.as_u64() == 0)
        {
            return Err(anyhow!(
                "copy signal {} has zero notional_lamports for notional_origin={}",
                signal.signal_id,
                notional_origin
            ));
        }
        match notional_origin {
            COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS => {
                if signal.notional_lamports.is_none() {
                    return Err(anyhow!(
                        "copy signal {} missing notional_lamports for exact notional_origin={}",
                        signal.signal_id,
                        notional_origin
                    ));
                }
            }
            COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE => {}
            other => {
                return Err(anyhow!(
                    "copy signal {} has unsupported notional_origin={}",
                    signal.signal_id,
                    other
                ));
            }
        }
        let notional_lamports_sql = signal
            .notional_lamports
            .map(|value| u64_to_sql_i64("copy_signals.notional_lamports", value.as_u64()))
            .transpose()?;
        let written = self
            .execute_with_retry(|conn| {
                conn.execute(
                    "INSERT OR IGNORE INTO copy_signals(
                        signal_id,
                        wallet_id,
                        side,
                        token,
                        notional_sol,
                        notional_lamports,
                        notional_origin,
                        ts,
                        status
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    params![
                        &signal.signal_id,
                        &signal.wallet_id,
                        &signal.side,
                        &signal.token,
                        signal.notional_sol,
                        notional_lamports_sql,
                        notional_origin,
                        signal.ts.to_rfc3339(),
                        &signal.status,
                    ],
                )
            })
            .context("failed to insert copy signal")?;
        Ok(written > 0)
    }

    pub fn list_copy_signals_by_status(
        &self,
        status: &str,
        limit: u32,
    ) -> Result<Vec<CopySignalRow>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT signal_id, wallet_id, side, token, notional_sol, notional_lamports, notional_origin, ts, status
                 FROM copy_signals
                 WHERE status = ?1
                 ORDER BY ts ASC
                 LIMIT ?2",
            )
            .context("failed to prepare copy_signals by status query")?;
        let mut rows = stmt
            .query(params![status, limit.max(1) as i64])
            .context("failed querying copy_signals by status")?;

        let mut signals = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating copy_signals by status rows")?
        {
            let notional_origin: String = row
                .get(6)
                .context("failed reading copy_signals.notional_origin")?;
            match notional_origin.as_str() {
                COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS
                | COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE => {}
                other => {
                    let signal_id: String = row
                        .get(0)
                        .context("failed reading copy_signals.signal_id")?;
                    return Err(anyhow!(
                        "unsupported copy_signals.notional_origin={} for signal {}",
                        other,
                        signal_id
                    ));
                }
            }
            let ts_raw: String = row.get(7).context("failed reading copy_signals.ts")?;
            let ts = DateTime::parse_from_rfc3339(&ts_raw)
                .map(|dt| dt.with_timezone(&Utc))
                .with_context(|| format!("invalid copy_signals.ts rfc3339 value: {ts_raw}"))?;
            let notional_lamports = parse_non_negative_i64(
                "copy_signals.notional_lamports",
                "copy-signal",
                row.get(5)
                    .context("failed reading copy_signals.notional_lamports")?,
            )?
            .map(Lamports::new);
            signals.push(CopySignalRow {
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
                notional_origin,
                ts,
                status: row.get(8).context("failed reading copy_signals.status")?,
            });
        }
        Ok(signals)
    }
}

fn parse_non_negative_i64(field: &str, row_kind: &str, value: Option<i64>) -> Result<Option<u64>> {
    match value {
        Some(value) if value < 0 => Err(anyhow!(
            "invalid {}={} for {} (must be >= 0)",
            field,
            value,
            row_kind
        )),
        Some(value) => Ok(Some(value as u64)),
        None => Ok(None),
    }
}
