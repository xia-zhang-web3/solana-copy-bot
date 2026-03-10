use super::{
    parse_non_negative_i64, u64_to_sql_i64, CopySignalRow, ExecutionOrderRow,
    InsertExecutionOrderPendingOutcome, SqliteStore, EXECUTION_CONFIRMED_RECONCILE_PENDING_STATUS,
    EXECUTION_SUBMITTED_RECONCILE_PENDING_STATUS,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::{
    COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS,
};
use rusqlite::{params, OptionalExtension};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarkOrderDroppedOutcome {
    Applied,
    UnexpectedStatus(String),
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleOrderRetryOutcome {
    Applied,
    UnexpectedStatus(String),
    NotFound,
}

impl SqliteStore {
    pub(crate) fn current_order_status(
        &self,
        order_id: &str,
        action: &str,
    ) -> Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT status FROM orders WHERE order_id = ?1 LIMIT 1",
                params![order_id],
                |row| row.get(0),
            )
            .optional()
            .with_context(|| {
                format!(
                    "failed reading current order status for action={action} order_id={order_id}"
                )
            })
    }

    pub(crate) fn unexpected_order_status_error(
        &self,
        order_id: &str,
        action: &str,
        expected_statuses: &[&str],
    ) -> Result<anyhow::Error> {
        let status = self.current_order_status(order_id, action)?;
        let expected = expected_statuses.join(", ");
        Ok(match status {
            Some(status) => anyhow!(
                "failed {}: order_id={} has unexpected status={} expected one of [{}]",
                action,
                order_id,
                status,
                expected
            ),
            None => anyhow!("failed {}: order_id={} not found", action, order_id),
        })
    }

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
        const EXPECTED: &[&str] = &["execution_pending"];
        let changed = self.execute_with_retry(|conn| {
            conn.execute(
                "UPDATE orders
                 SET status = 'execution_simulated',
                     simulation_status = ?1,
                     simulation_error = ?2
                 WHERE order_id = ?3
                   AND status = 'execution_pending'",
                params![simulation_status, simulation_detail, order_id],
            )
        })?;
        if changed == 0 {
            return Err(self.unexpected_order_status_error(
                order_id,
                "marking order simulated",
                EXPECTED,
            )?);
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
        const EXPECTED: &[&str] = &["execution_pending", "execution_simulated"];
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
                 WHERE order_id = ?9
                   AND status IN ('execution_pending', 'execution_simulated')",
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
            return Err(self.unexpected_order_status_error(
                order_id,
                "marking order submitted",
                EXPECTED,
            )?);
        }
        Ok(())
    }

    pub fn try_schedule_order_retry(
        &self,
        order_id: &str,
        expected_status: &str,
        attempt: u32,
        detail: Option<&str>,
    ) -> Result<ScheduleOrderRetryOutcome> {
        const ACTION: &str = "scheduling order retry";
        let changed = self.execute_with_retry(|conn| {
            conn.execute(
                "UPDATE orders
                 SET attempt = ?1,
                     simulation_error = COALESCE(?2, simulation_error)
                 WHERE order_id = ?3
                   AND status = ?4",
                params![attempt.max(1) as i64, detail, order_id, expected_status],
            )
        })?;
        if changed > 0 {
            return Ok(ScheduleOrderRetryOutcome::Applied);
        }
        Ok(match self.current_order_status(order_id, ACTION)? {
            Some(status) => ScheduleOrderRetryOutcome::UnexpectedStatus(status),
            None => ScheduleOrderRetryOutcome::NotFound,
        })
    }

    pub fn mark_order_confirmed(&self, order_id: &str, confirm_ts: DateTime<Utc>) -> Result<()> {
        const ACTION: &str = "marking order confirmed";
        const EXPECTED: &[&str] = &[
            "execution_submitted",
            EXECUTION_SUBMITTED_RECONCILE_PENDING_STATUS,
            EXECUTION_CONFIRMED_RECONCILE_PENDING_STATUS,
        ];
        let changed = self.execute_with_retry(|conn| {
            conn.execute(
                "UPDATE orders
                 SET status = 'execution_confirmed',
                     confirm_ts = ?1
                 WHERE order_id = ?2
                   AND status IN ('execution_submitted', ?3, ?4)",
                params![
                    confirm_ts.to_rfc3339(),
                    order_id,
                    EXECUTION_SUBMITTED_RECONCILE_PENDING_STATUS,
                    EXECUTION_CONFIRMED_RECONCILE_PENDING_STATUS
                ],
            )
        })?;
        if changed == 0 {
            return Err(self.unexpected_order_status_error(order_id, ACTION, EXPECTED)?);
        }
        Ok(())
    }

    pub fn mark_order_dropped(
        &self,
        order_id: &str,
        err_code: &str,
        detail: Option<&str>,
    ) -> Result<()> {
        const ACTION: &str = "marking order dropped";
        const EXPECTED: &[&str] = &["execution_pending", "execution_simulated"];
        match self.try_mark_order_dropped(order_id, err_code, detail)? {
            MarkOrderDroppedOutcome::Applied => Ok(()),
            MarkOrderDroppedOutcome::UnexpectedStatus(status) => Err(anyhow!(
                "failed {}: order_id={} has unexpected status={} expected one of [{}]",
                ACTION,
                order_id,
                status,
                EXPECTED.join(", ")
            )),
            MarkOrderDroppedOutcome::NotFound => Err(anyhow!(
                "failed {}: order_id={} not found",
                ACTION,
                order_id
            )),
        }
    }

    pub fn try_mark_order_dropped(
        &self,
        order_id: &str,
        err_code: &str,
        detail: Option<&str>,
    ) -> Result<MarkOrderDroppedOutcome> {
        const ACTION: &str = "marking order dropped";
        let changed = self.execute_with_retry(|conn| {
            conn.execute(
                "UPDATE orders
                 SET status = 'execution_dropped',
                     err_code = ?1,
                     simulation_error = COALESCE(?2, simulation_error)
                 WHERE order_id = ?3
                   AND status IN ('execution_pending', 'execution_simulated')",
                params![err_code, detail, order_id],
            )
        })?;
        if changed > 0 {
            return Ok(MarkOrderDroppedOutcome::Applied);
        }
        Ok(match self.current_order_status(order_id, ACTION)? {
            Some(status) => MarkOrderDroppedOutcome::UnexpectedStatus(status),
            None => MarkOrderDroppedOutcome::NotFound,
        })
    }

    pub fn mark_order_failed(
        &self,
        order_id: &str,
        err_code: &str,
        detail: Option<&str>,
    ) -> Result<()> {
        const EXPECTED: &[&str] = &[
            "execution_pending",
            "execution_simulated",
            "execution_submitted",
            EXECUTION_SUBMITTED_RECONCILE_PENDING_STATUS,
            EXECUTION_CONFIRMED_RECONCILE_PENDING_STATUS,
        ];
        let changed = self.execute_with_retry(|conn| {
            conn.execute(
                "UPDATE orders
                 SET status = 'execution_failed',
                     err_code = ?1,
                     simulation_error = COALESCE(?2, simulation_error)
                 WHERE order_id = ?3
                   AND status IN (
                       'execution_pending',
                       'execution_simulated',
                       'execution_submitted',
                       ?4,
                       ?5
                   )",
                params![
                    err_code,
                    detail,
                    order_id,
                    EXECUTION_SUBMITTED_RECONCILE_PENDING_STATUS,
                    EXECUTION_CONFIRMED_RECONCILE_PENDING_STATUS
                ],
            )
        })?;
        if changed == 0 {
            return Err(self.unexpected_order_status_error(
                order_id,
                "marking order failed",
                EXPECTED,
            )?);
        }
        Ok(())
    }

    pub fn mark_order_reconcile_pending(&self, order_id: &str, err_code: &str) -> Result<()> {
        self.mark_order_reconcile_pending_with_status(
            order_id,
            err_code,
            EXECUTION_SUBMITTED_RECONCILE_PENDING_STATUS,
            None,
        )
    }

    pub fn mark_order_confirmed_reconcile_pending(
        &self,
        order_id: &str,
        err_code: &str,
        confirm_ts: DateTime<Utc>,
    ) -> Result<()> {
        self.mark_order_reconcile_pending_with_status(
            order_id,
            err_code,
            EXECUTION_CONFIRMED_RECONCILE_PENDING_STATUS,
            Some(confirm_ts),
        )
    }

    fn mark_order_reconcile_pending_with_status(
        &self,
        order_id: &str,
        err_code: &str,
        pending_status: &str,
        confirm_ts: Option<DateTime<Utc>>,
    ) -> Result<()> {
        const SUBMITTED_RECONCILE_EXPECTED: &[&str] = &[
            "execution_submitted",
            EXECUTION_SUBMITTED_RECONCILE_PENDING_STATUS,
        ];
        const CONFIRMED_RECONCILE_EXPECTED: &[&str] = &[
            "execution_submitted",
            EXECUTION_SUBMITTED_RECONCILE_PENDING_STATUS,
            EXECUTION_CONFIRMED_RECONCILE_PENDING_STATUS,
        ];
        let confirm_ts = confirm_ts.map(|value| value.to_rfc3339());
        let (changed, expected) = if pending_status == EXECUTION_SUBMITTED_RECONCILE_PENDING_STATUS
        {
            (
                self.execute_with_retry(|conn| {
                    conn.execute(
                        "UPDATE orders
                         SET status = ?1,
                             err_code = ?2,
                             confirm_ts = COALESCE(?3, confirm_ts)
                         WHERE order_id = ?4
                           AND status IN ('execution_submitted', ?1)",
                        params![pending_status, err_code, confirm_ts, order_id],
                    )
                })?,
                SUBMITTED_RECONCILE_EXPECTED,
            )
        } else if pending_status == EXECUTION_CONFIRMED_RECONCILE_PENDING_STATUS {
            (
                self.execute_with_retry(|conn| {
                    conn.execute(
                        "UPDATE orders
                         SET status = ?1,
                             err_code = ?2,
                             confirm_ts = COALESCE(?3, confirm_ts)
                         WHERE order_id = ?4
                           AND status IN ('execution_submitted', ?1, ?5)",
                        params![
                            pending_status,
                            err_code,
                            confirm_ts,
                            order_id,
                            EXECUTION_SUBMITTED_RECONCILE_PENDING_STATUS
                        ],
                    )
                })?,
                CONFIRMED_RECONCILE_EXPECTED,
            )
        } else {
            return Err(anyhow!(
                "failed marking order reconcile-pending: unsupported pending status={pending_status}"
            ));
        };
        if changed == 0 {
            return Err(self.unexpected_order_status_error(
                order_id,
                "marking order reconcile-pending",
                expected,
            )?);
        }
        Ok(())
    }
}
