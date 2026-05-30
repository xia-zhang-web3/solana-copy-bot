use crate::{
    ExecutionDryRunOrder, ExecutionDryRunRecordOutcome, SqliteDiscoveryStore,
    EXECUTION_SIMULATION_STATUS_DRY_RUN_SKIPPED, EXECUTION_STATUS_DRY_RUN_CONFIRMED,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::CopySignalRow;
use rusqlite::{params, OptionalExtension};

impl SqliteDiscoveryStore {
    pub fn list_execution_canary_candidates(
        &self,
        copy_signal_status: &str,
        since: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<CopySignalRow>> {
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
                   AND lower(side) IN ('buy', 'sell')
                   AND NOT EXISTS (
                        SELECT 1 FROM orders
                        WHERE orders.signal_id = copy_signals.signal_id
                   )
                 ORDER BY
                    CASE WHEN lower(side) = 'sell' THEN 0 ELSE 1 END,
                    ts ASC
                 LIMIT ?3",
            )
            .context("failed to prepare execution canary candidate query")?;
        let mut rows = stmt
            .query(params![
                copy_signal_status,
                since.to_rfc3339(),
                limit.max(1) as i64
            ])
            .context("failed querying execution canary candidates")?;

        let mut signals = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating execution canary candidates")?
        {
            let ts_raw: String = row.get(7).context("failed reading copy_signals.ts")?;
            let ts = DateTime::parse_from_rfc3339(&ts_raw)
                .map(|dt| dt.with_timezone(&Utc))
                .with_context(|| format!("invalid copy_signals.ts rfc3339 value: {ts_raw}"))?;
            let notional_lamports_raw: Option<i64> = row
                .get(5)
                .context("failed reading copy_signals.notional_lamports")?;
            let notional_lamports = notional_lamports_raw
                .map(|value| {
                    u64::try_from(value)
                        .map(copybot_core_types::Lamports::new)
                        .with_context(|| {
                            format!(
                                "invalid copy_signals.notional_lamports={value} for execution canary"
                            )
                        })
                })
                .transpose()?;
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
                notional_origin: row
                    .get(6)
                    .context("failed reading copy_signals.notional_origin")?,
                ts,
                status: row.get(8).context("failed reading copy_signals.status")?,
            });
        }
        Ok(signals)
    }

    pub fn record_execution_dry_run_order(
        &self,
        signal_id: &str,
        route: &str,
        now: DateTime<Utc>,
    ) -> Result<ExecutionDryRunRecordOutcome> {
        let order_id = execution_dry_run_order_id(signal_id);
        let client_order_id = execution_dry_run_client_order_id(signal_id);
        let now_rfc3339 = now.to_rfc3339();
        self.with_immediate_transaction_retry("execution dry-run order record", |conn| {
            let existing: Option<String> = conn
                .query_row(
                    "SELECT order_id FROM orders WHERE signal_id = ?1 LIMIT 1",
                    params![signal_id],
                    |row| row.get(0),
                )
                .optional()
                .context("failed checking existing execution order for signal")?;
            if existing.is_some() {
                return Ok(ExecutionDryRunRecordOutcome::Existing);
            }

            conn.execute(
                "INSERT INTO orders(
                    order_id,
                    signal_id,
                    route,
                    submit_ts,
                    confirm_ts,
                    status,
                    err_code,
                    client_order_id,
                    tx_signature,
                    simulation_status,
                    simulation_error,
                    attempt
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, NULL, ?8, NULL, 1)",
                params![
                    order_id,
                    signal_id,
                    route,
                    now_rfc3339,
                    now_rfc3339,
                    EXECUTION_STATUS_DRY_RUN_CONFIRMED,
                    client_order_id,
                    EXECUTION_SIMULATION_STATUS_DRY_RUN_SKIPPED,
                ],
            )
            .context("failed inserting execution dry-run order")?;

            Ok(ExecutionDryRunRecordOutcome::Inserted)
        })
    }

    pub fn latest_execution_dry_run_order(&self) -> Result<Option<ExecutionDryRunOrder>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    order_id,
                    signal_id,
                    route,
                    submit_ts,
                    confirm_ts,
                    status,
                    client_order_id,
                    simulation_status,
                    attempt
                 FROM orders
                 WHERE status = ?1
                 ORDER BY submit_ts DESC, order_id DESC
                 LIMIT 1",
            )
            .context("failed to prepare latest execution dry-run order query")?;
        stmt.query_row(params![EXECUTION_STATUS_DRY_RUN_CONFIRMED], |row| {
            let submit_ts_raw: String = row.get(3)?;
            let confirm_ts_raw: Option<String> = row.get(4)?;
            let submit_ts = DateTime::parse_from_rfc3339(&submit_ts_raw)
                .map(|dt| dt.with_timezone(&Utc))
                .map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        3,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?;
            let confirm_ts = match confirm_ts_raw {
                Some(value) => Some(
                    DateTime::parse_from_rfc3339(&value)
                        .map(|dt| dt.with_timezone(&Utc))
                        .map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                4,
                                rusqlite::types::Type::Text,
                                Box::new(error),
                            )
                        })?,
                ),
                None => None,
            };
            let attempt_raw: i64 = row.get(8)?;
            let attempt = u32::try_from(attempt_raw).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    8,
                    rusqlite::types::Type::Integer,
                    Box::new(error),
                )
            })?;
            Ok(ExecutionDryRunOrder {
                order_id: row.get(0)?,
                signal_id: row.get(1)?,
                route: row.get(2)?,
                submit_ts,
                confirm_ts,
                status: row.get(5)?,
                client_order_id: row.get(6)?,
                simulation_status: row.get(7)?,
                attempt,
            })
        })
        .optional()
        .context("failed querying latest execution dry-run order")
    }
}

pub fn execution_dry_run_order_id(signal_id: &str) -> String {
    format!("exec-dry-run:{signal_id}")
}

pub fn execution_dry_run_client_order_id(signal_id: &str) -> String {
    format!("copybot:dry-run:{signal_id}")
}
