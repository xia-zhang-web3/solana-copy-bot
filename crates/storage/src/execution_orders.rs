use super::{
    parse_non_negative_i64, u64_to_sql_i64, CopySignalRow, ExecutionOrderRow,
    InsertExecutionOrderPendingOutcome, SqliteStore,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, OptionalExtension};

impl SqliteStore {
    pub fn insert_copy_signal(&self, signal: &CopySignalRow) -> Result<bool> {
        let written = self
            .execute_with_retry(|conn| {
                conn.execute(
                    "INSERT OR IGNORE INTO copy_signals(
                    signal_id,
                    wallet_id,
                    side,
                    token,
                    notional_sol,
                    ts,
                    status
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        &signal.signal_id,
                        &signal.wallet_id,
                        &signal.side,
                        &signal.token,
                        signal.notional_sol,
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
            "SELECT signal_id, wallet_id, side, token, notional_sol, ts, status
             FROM copy_signals
             WHERE status = ?1
             ORDER BY CASE WHEN lower(side) = 'sell' THEN 0 ELSE 1 END, ts ASC
             LIMIT ?2"
        } else {
            "SELECT signal_id, wallet_id, side, token, notional_sol, ts, status
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
            let ts_raw: String = row.get(5).context("failed reading copy_signals.ts")?;
            let ts = DateTime::parse_from_rfc3339(&ts_raw)
                .map(|dt| dt.with_timezone(&Utc))
                .with_context(|| format!("invalid copy_signals.ts rfc3339 value: {ts_raw}"))?;
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
                ts,
                status: row.get(6).context("failed reading copy_signals.status")?,
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

    pub fn execution_order_by_client_order_id(
        &self,
        client_order_id: &str,
    ) -> Result<Option<ExecutionOrderRow>> {
        let row = self
            .conn
            .query_row(
                "SELECT
                    order_id,
                    signal_id,
                    client_order_id,
                    route,
                    applied_tip_lamports,
                    ata_create_rent_lamports,
                    network_fee_lamports_hint,
                    base_fee_lamports_hint,
                    priority_fee_lamports_hint,
                    submit_ts,
                    confirm_ts,
                    status,
                    err_code,
                    tx_signature,
                    simulation_status,
                    simulation_error,
                    attempt
                 FROM orders
                 WHERE client_order_id = ?1
                 ORDER BY submit_ts DESC
                 LIMIT 1",
                params![client_order_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<i64>>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                        row.get::<_, Option<i64>>(6)?,
                        row.get::<_, Option<i64>>(7)?,
                        row.get::<_, Option<i64>>(8)?,
                        row.get::<_, String>(9)?,
                        row.get::<_, Option<String>>(10)?,
                        row.get::<_, String>(11)?,
                        row.get::<_, Option<String>>(12)?,
                        row.get::<_, Option<String>>(13)?,
                        row.get::<_, Option<String>>(14)?,
                        row.get::<_, Option<String>>(15)?,
                        row.get::<_, i64>(16)?,
                    ))
                },
            )
            .optional()
            .context("failed querying order by client_order_id")?;

        row.map(
            |(
                order_id,
                signal_id,
                client_id,
                route,
                applied_tip_lamports_raw,
                ata_create_rent_lamports_raw,
                network_fee_lamports_hint_raw,
                base_fee_lamports_hint_raw,
                priority_fee_lamports_hint_raw,
                submit_ts_raw,
                confirm_ts_raw,
                status,
                err_code,
                tx_signature,
                simulation_status,
                simulation_error,
                attempt_raw,
            )| {
                let submit_ts = DateTime::parse_from_rfc3339(&submit_ts_raw)
                    .map(|dt| dt.with_timezone(&Utc))
                    .with_context(|| {
                        format!("invalid orders.submit_ts rfc3339 value: {submit_ts_raw}")
                    })?;
                let confirm_ts = confirm_ts_raw
                    .as_deref()
                    .map(|raw| {
                        DateTime::parse_from_rfc3339(raw)
                            .map(|dt| dt.with_timezone(&Utc))
                            .with_context(|| {
                                format!("invalid orders.confirm_ts rfc3339 value: {raw}")
                            })
                    })
                    .transpose()?;
                let applied_tip_lamports = parse_non_negative_i64(
                    "orders.applied_tip_lamports",
                    &order_id,
                    applied_tip_lamports_raw,
                )?;
                let ata_create_rent_lamports = parse_non_negative_i64(
                    "orders.ata_create_rent_lamports",
                    &order_id,
                    ata_create_rent_lamports_raw,
                )?;
                let network_fee_lamports_hint = parse_non_negative_i64(
                    "orders.network_fee_lamports_hint",
                    &order_id,
                    network_fee_lamports_hint_raw,
                )?;
                let base_fee_lamports_hint = parse_non_negative_i64(
                    "orders.base_fee_lamports_hint",
                    &order_id,
                    base_fee_lamports_hint_raw,
                )?;
                let priority_fee_lamports_hint = parse_non_negative_i64(
                    "orders.priority_fee_lamports_hint",
                    &order_id,
                    priority_fee_lamports_hint_raw,
                )?;
                Ok(ExecutionOrderRow {
                    order_id,
                    signal_id,
                    client_order_id: client_id,
                    route,
                    applied_tip_lamports,
                    ata_create_rent_lamports,
                    network_fee_lamports_hint,
                    base_fee_lamports_hint,
                    priority_fee_lamports_hint,
                    submit_ts,
                    confirm_ts,
                    status,
                    err_code,
                    tx_signature,
                    simulation_status,
                    simulation_error,
                    attempt: attempt_raw.max(0) as u32,
                })
            },
        )
        .transpose()
    }

    pub fn insert_execution_order_pending(
        &self,
        order_id: &str,
        signal_id: &str,
        client_order_id: &str,
        route: &str,
        submit_ts: DateTime<Utc>,
        attempt: u32,
    ) -> Result<InsertExecutionOrderPendingOutcome> {
        let written = self
            .execute_with_retry(|conn| {
                conn.execute(
                    "INSERT OR IGNORE INTO orders(
                        order_id,
                        signal_id,
                        client_order_id,
                        route,
                        submit_ts,
                        status,
                        attempt
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        order_id,
                        signal_id,
                        client_order_id,
                        route,
                        submit_ts.to_rfc3339(),
                        "execution_pending",
                        attempt.max(1) as i64,
                    ],
                )
            })
            .context("failed inserting pending execution order")?;
        if written > 0 {
            return Ok(InsertExecutionOrderPendingOutcome::Inserted);
        }

        let duplicate = self
            .conn
            .query_row(
                "SELECT 1
                 FROM orders
                 WHERE client_order_id = ?1
                    OR order_id = ?2
                 LIMIT 1",
                params![client_order_id, order_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .context("failed verifying duplicate execution order insert")?;
        if duplicate.is_some() {
            return Ok(InsertExecutionOrderPendingOutcome::Duplicate);
        }

        Err(anyhow!(
            "execution order insert ignored without duplicate detection order_id={} client_order_id={}",
            order_id,
            client_order_id
        ))
    }

    pub fn mark_order_simulated(
        &self,
        order_id: &str,
        simulation_status: &str,
        simulation_detail: Option<&str>,
    ) -> Result<()> {
        let changed = self.execute_with_retry(|conn| {
            conn.execute(
                "UPDATE orders
                 SET status = 'execution_simulated',
                     simulation_status = ?1,
                     simulation_error = ?2
                 WHERE order_id = ?3",
                params![simulation_status, simulation_detail, order_id],
            )
        })?;
        if changed == 0 {
            return Err(anyhow!(
                "failed marking order simulated: order_id={} not found",
                order_id
            ));
        }
        Ok(())
    }

    pub fn mark_order_submitted(
        &self,
        order_id: &str,
        route: &str,
        tx_signature: &str,
        submit_ts: DateTime<Utc>,
        applied_tip_lamports: Option<u64>,
        ata_create_rent_lamports: Option<u64>,
        network_fee_lamports_hint: Option<u64>,
        base_fee_lamports_hint: Option<u64>,
        priority_fee_lamports_hint: Option<u64>,
    ) -> Result<()> {
        let applied_tip_lamports_sql = applied_tip_lamports
            .map(|value| u64_to_sql_i64("orders.applied_tip_lamports", value))
            .transpose()?;
        let ata_create_rent_lamports_sql = ata_create_rent_lamports
            .map(|value| u64_to_sql_i64("orders.ata_create_rent_lamports", value))
            .transpose()?;
        let network_fee_lamports_hint_sql = network_fee_lamports_hint
            .map(|value| u64_to_sql_i64("orders.network_fee_lamports_hint", value))
            .transpose()?;
        let base_fee_lamports_hint_sql = base_fee_lamports_hint
            .map(|value| u64_to_sql_i64("orders.base_fee_lamports_hint", value))
            .transpose()?;
        let priority_fee_lamports_hint_sql = priority_fee_lamports_hint
            .map(|value| u64_to_sql_i64("orders.priority_fee_lamports_hint", value))
            .transpose()?;
        let changed = self.execute_with_retry(|conn| {
            conn.execute(
                "UPDATE orders
                 SET status = 'execution_submitted',
                     route = ?1,
                     tx_signature = ?2,
                     submit_ts = ?3,
                     applied_tip_lamports = ?4,
                     ata_create_rent_lamports = ?5,
                     network_fee_lamports_hint = ?6,
                     base_fee_lamports_hint = ?7,
                     priority_fee_lamports_hint = ?8
                 WHERE order_id = ?9",
                params![
                    route,
                    tx_signature,
                    submit_ts.to_rfc3339(),
                    applied_tip_lamports_sql,
                    ata_create_rent_lamports_sql,
                    network_fee_lamports_hint_sql,
                    base_fee_lamports_hint_sql,
                    priority_fee_lamports_hint_sql,
                    order_id
                ],
            )
        })?;
        if changed == 0 {
            return Err(anyhow!(
                "failed marking order submitted: order_id={} not found",
                order_id
            ));
        }
        Ok(())
    }

    pub fn set_order_attempt(
        &self,
        order_id: &str,
        attempt: u32,
        detail: Option<&str>,
    ) -> Result<()> {
        let changed = self.execute_with_retry(|conn| {
            conn.execute(
                "UPDATE orders
                 SET attempt = ?1,
                     simulation_error = COALESCE(?2, simulation_error)
                 WHERE order_id = ?3",
                params![attempt.max(1) as i64, detail, order_id],
            )
        })?;
        if changed == 0 {
            return Err(anyhow!(
                "failed setting order attempt: order_id={} not found",
                order_id
            ));
        }
        Ok(())
    }

    pub fn mark_order_confirmed(&self, order_id: &str, confirm_ts: DateTime<Utc>) -> Result<()> {
        let changed = self.execute_with_retry(|conn| {
            conn.execute(
                "UPDATE orders
                 SET status = 'execution_confirmed',
                     confirm_ts = ?1
                 WHERE order_id = ?2",
                params![confirm_ts.to_rfc3339(), order_id],
            )
        })?;
        if changed == 0 {
            return Err(anyhow!(
                "failed marking order confirmed: order_id={} not found",
                order_id
            ));
        }
        Ok(())
    }

    pub fn mark_order_dropped(
        &self,
        order_id: &str,
        err_code: &str,
        detail: Option<&str>,
    ) -> Result<()> {
        let changed = self.execute_with_retry(|conn| {
            conn.execute(
                "UPDATE orders
                 SET status = 'execution_dropped',
                     err_code = ?1,
                     simulation_error = COALESCE(?2, simulation_error)
                 WHERE order_id = ?3",
                params![err_code, detail, order_id],
            )
        })?;
        if changed == 0 {
            return Err(anyhow!(
                "failed marking order dropped: order_id={} not found",
                order_id
            ));
        }
        Ok(())
    }

    pub fn mark_order_failed(
        &self,
        order_id: &str,
        err_code: &str,
        detail: Option<&str>,
    ) -> Result<()> {
        let changed = self.execute_with_retry(|conn| {
            conn.execute(
                "UPDATE orders
                 SET status = 'execution_failed',
                     err_code = ?1,
                     simulation_error = COALESCE(?2, simulation_error)
                 WHERE order_id = ?3",
                params![err_code, detail, order_id],
            )
        })?;
        if changed == 0 {
            return Err(anyhow!(
                "failed marking order failed: order_id={} not found",
                order_id
            ));
        }
        Ok(())
    }
}
