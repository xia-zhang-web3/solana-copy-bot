use super::{parse_non_negative_i64, u64_to_sql_i64, CopySignalRow, SqliteStore};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::{
    COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS,
};
use rusqlite::params;

impl SqliteStore {
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
        self.list_copy_signals_by_status_with_side_priority(status, limit, false)
    }

    pub fn list_copy_signals_by_status_with_side_priority(
        &self,
        status: &str,
        limit: u32,
        prioritize_sell: bool,
    ) -> Result<Vec<CopySignalRow>> {
        let query = if prioritize_sell {
            "SELECT signal_id, wallet_id, side, token, notional_sol, notional_lamports, notional_origin, ts, status
             FROM copy_signals
             WHERE status = ?1
             ORDER BY CASE WHEN lower(side) = 'sell' THEN 0 ELSE 1 END, ts ASC
             LIMIT ?2"
        } else {
            "SELECT signal_id, wallet_id, side, token, notional_sol, notional_lamports, notional_origin, ts, status
             FROM copy_signals
             WHERE status = ?1
             ORDER BY ts ASC
             LIMIT ?2"
        };
        let mut stmt = self
            .conn
            .prepare(query)
            .context("failed to prepare copy_signals by status query")?;
        let mut rows = stmt
            .query(params![status, limit.max(1) as i64])
            .context("failed querying copy_signals by status")?;

        let mut out = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating copy_signals by status rows")?
        {
            let notional_origin: String = row
                .get(6)
                .context("failed reading copy_signals.notional_origin")?;
            let ts_raw: String = row.get(7).context("failed reading copy_signals.ts")?;
            let ts = DateTime::parse_from_rfc3339(&ts_raw)
                .map(|dt| dt.with_timezone(&Utc))
                .with_context(|| format!("invalid copy_signals.ts rfc3339 value: {ts_raw}"))?;
            let parsed_notional_lamports = parse_non_negative_i64(
                "copy_signals.notional_lamports",
                "copy-signal",
                row.get(5)
                    .context("failed reading copy_signals.notional_lamports")?,
            )?;
            match notional_origin.as_str() {
                COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS
                | COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE => {}
                other => {
                    return Err(anyhow!(
                        "unsupported copy_signals.notional_origin={} for signal {}",
                        other,
                        row.get::<_, String>(0).context(
                            "failed reading copy_signals.signal_id for notional_origin error"
                        )?
                    ));
                }
            }
            let notional_lamports = parsed_notional_lamports.map(crate::Lamports::new);
            out.push(CopySignalRow {
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
        Ok(out)
    }

    pub fn update_copy_signal_status(&self, signal_id: &str, status: &str) -> Result<bool> {
        let changed = self
            .execute_with_retry(|conn| {
                conn.execute(
                    "UPDATE copy_signals SET status = ?1 WHERE signal_id = ?2",
                    params![status, signal_id],
                )
            })
            .with_context(|| format!("failed updating copy_signals status for {}", signal_id))?;
        Ok(changed > 0)
    }
}
