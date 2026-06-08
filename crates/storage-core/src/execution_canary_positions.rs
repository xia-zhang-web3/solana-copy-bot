use crate::{
    money::{sol_to_lamports_ceil, token_quantity_from_sql, u64_to_sql_i64},
    shadow_lots::{reject_zero_raw_exact_qty, to_sql_conversion_error},
    ExecutionCanaryOwnedPosition, ExecutionCanaryOwnedPositionRecordResult,
    ExecutionCanaryPositionRecordOutcome, ExecutionCanarySellDecision, SqliteDiscoveryStore,
    EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET, EXECUTION_CANARY_POSITION_CLOSE_CLOSED,
    EXECUTION_CANARY_POSITION_CLOSE_DUST_CLOSED, EXECUTION_CANARY_POSITION_STATE_OPEN,
    EXECUTION_CANARY_SELL_DECISION_EXECUTE, EXECUTION_CANARY_SELL_DECISION_FORCE_EXIT,
    EXECUTION_CANARY_SELL_DECISION_NO_POSITION,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{Lamports, TokenQuantity};
use rusqlite::{params, OptionalExtension};

const EXECUTION_CANARY_STALE_WRITE_OFF_EXIT_PRICE_SOL: f64 = 0.0;
const EXECUTION_CANARY_STALE_WRITE_OFF_DUST_QTY_EPSILON: f64 = 0.0;

impl SqliteDiscoveryStore {
    pub fn record_execution_canary_open_position(
        &self,
        order_id: &str,
        token: &str,
        qty: f64,
        qty_exact: Option<TokenQuantity>,
        cost_sol: f64,
        opened_ts: DateTime<Utc>,
    ) -> Result<ExecutionCanaryOwnedPositionRecordResult> {
        validate_position_inputs(order_id, token, qty)?;
        let qty_exact = reject_zero_raw_exact_qty(qty_exact, "execution canary open position")?;
        let position_id = execution_canary_position_id(order_id);
        let cost_lamports = sol_to_lamports_ceil(cost_sol, "execution canary position cost_sol")?;
        let qty_raw = qty_exact.map(|value| value.raw().to_string());
        let qty_decimals = qty_exact.map(|value| i64::from(value.decimals()));
        let opened_ts_raw = opened_ts.to_rfc3339();
        let inserted = self.with_immediate_transaction_retry(
            "execution canary open position record",
            |conn| {
                let existing: Option<String> = conn
                    .query_row(
                        "SELECT position_id
                         FROM positions
                         WHERE position_id = ?1
                         LIMIT 1",
                        params![position_id],
                        |row| row.get(0),
                    )
                    .optional()
                    .context("failed checking existing execution canary position")?;
                if existing.is_some() {
                    return Ok(false);
                }

                conn.execute(
                    "INSERT INTO positions(
                        position_id,
                        token,
                        qty,
                        cost_sol,
                        opened_ts,
                        closed_ts,
                        pnl_sol,
                        state,
                        cost_lamports,
                        qty_raw,
                        qty_decimals,
                        pnl_lamports,
                        accounting_bucket
                    ) VALUES (?1, ?2, ?3, ?4, ?5, NULL, NULL, ?6, ?7, ?8, ?9, NULL, ?10)",
                    params![
                        position_id,
                        token,
                        qty,
                        cost_sol,
                        opened_ts_raw,
                        EXECUTION_CANARY_POSITION_STATE_OPEN,
                        u64_to_sql_i64("positions.cost_lamports", cost_lamports.as_u64())?,
                        qty_raw,
                        qty_decimals,
                        EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
                    ],
                )
                .context("failed inserting execution canary position")?;
                Ok(true)
            },
        )?;
        let position = self
            .load_execution_canary_position(&position_id)?
            .ok_or_else(|| anyhow!("missing execution canary position for order {order_id}"))?;
        let outcome = if inserted {
            ExecutionCanaryPositionRecordOutcome::Inserted
        } else {
            ExecutionCanaryPositionRecordOutcome::Existing
        };
        Ok(ExecutionCanaryOwnedPositionRecordResult { outcome, position })
    }

    pub fn load_execution_canary_open_position(
        &self,
        token: &str,
    ) -> Result<Option<ExecutionCanaryOwnedPosition>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    position_id,
                    token,
                    qty,
                    cost_sol,
                    cost_lamports,
                    qty_raw,
                    qty_decimals,
                    opened_ts,
                    state,
                    accounting_bucket
                 FROM positions
                 WHERE token = ?1
                   AND accounting_bucket = ?2
                   AND state = ?3
                 LIMIT 1",
            )
            .context("failed to prepare execution canary open position query")?;
        stmt.query_row(
            params![
                token,
                EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
                EXECUTION_CANARY_POSITION_STATE_OPEN,
            ],
            execution_canary_position_from_row,
        )
        .optional()
        .context("failed querying execution canary open position")
    }

    pub fn execution_canary_open_position_count(&self) -> Result<u64> {
        let count: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*)
                 FROM positions
                 WHERE accounting_bucket = ?1
                   AND state = ?2",
                params![
                    EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
                    EXECUTION_CANARY_POSITION_STATE_OPEN,
                ],
                |row| row.get(0),
            )
            .context("failed counting execution canary open positions")?;
        if count < 0 {
            return Err(anyhow!(
                "invalid negative execution canary open position count={count}"
            ));
        }
        Ok(count as u64)
    }

    pub fn close_stale_execution_canary_open_positions_as_zero_loss(
        &self,
        now: DateTime<Utc>,
        max_age_seconds: i64,
    ) -> Result<u64> {
        if max_age_seconds <= 0 {
            return Err(anyhow!(
                "execution canary stale position max_age_seconds must be positive"
            ));
        }
        let cutoff = now - Duration::seconds(max_age_seconds);
        let positions = self.load_execution_canary_open_positions()?;
        let mut closed = 0;
        for position in positions
            .into_iter()
            .filter(|position| position.opened_ts <= cutoff)
        {
            let result = self.close_execution_canary_open_position(
                &position.token,
                position.qty,
                position.qty_exact,
                EXECUTION_CANARY_STALE_WRITE_OFF_EXIT_PRICE_SOL,
                EXECUTION_CANARY_STALE_WRITE_OFF_DUST_QTY_EPSILON,
                now,
            )?;
            if result.close_status == EXECUTION_CANARY_POSITION_CLOSE_CLOSED
                || result.close_status == EXECUTION_CANARY_POSITION_CLOSE_DUST_CLOSED
            {
                closed += 1;
            }
        }
        Ok(closed)
    }

    pub fn execution_canary_realized_loss_sol_since(&self, since: DateTime<Utc>) -> Result<f64> {
        let since_raw = since.to_rfc3339();
        self.conn
            .query_row(
                "SELECT COALESCE(SUM(
                    CASE
                      WHEN COALESCE(pnl_lamports, ROUND(COALESCE(pnl_sol, 0.0) * 1000000000.0)) < 0
                      THEN -COALESCE(pnl_lamports, ROUND(COALESCE(pnl_sol, 0.0) * 1000000000.0)) / 1000000000.0
                      ELSE 0.0
                    END
                 ), 0.0)
                 FROM positions
                 WHERE accounting_bucket = ?1
                   AND state = ?2
                   AND closed_ts >= ?3",
                params![
                    EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
                    crate::EXECUTION_CANARY_POSITION_STATE_CLOSED,
                    since_raw,
                ],
                |row| row.get(0),
            )
            .context("failed summing execution canary realized loss")
    }

    pub fn execution_canary_sell_decision(
        &self,
        token: &str,
        slippage_bps: Option<f64>,
        soft_slippage_limit_bps: f64,
    ) -> Result<ExecutionCanarySellDecision> {
        validate_slippage_inputs(slippage_bps, soft_slippage_limit_bps)?;
        let position = self.load_execution_canary_open_position(token)?;
        let Some(position) = position else {
            return Ok(ExecutionCanarySellDecision {
                decision_status: EXECUTION_CANARY_SELL_DECISION_NO_POSITION.to_string(),
                decision_reason: "no_owned_position".to_string(),
                position: None,
                slippage_bps,
            });
        };
        let (decision_status, decision_reason) =
            if slippage_bps.is_some_and(|value| value > soft_slippage_limit_bps) {
                (
                    EXECUTION_CANARY_SELL_DECISION_FORCE_EXIT,
                    "sell_slippage_above_soft_limit",
                )
            } else {
                (EXECUTION_CANARY_SELL_DECISION_EXECUTE, "owned_position")
            };
        Ok(ExecutionCanarySellDecision {
            decision_status: decision_status.to_string(),
            decision_reason: decision_reason.to_string(),
            position: Some(position),
            slippage_bps,
        })
    }

    fn load_execution_canary_position(
        &self,
        position_id: &str,
    ) -> Result<Option<ExecutionCanaryOwnedPosition>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    position_id,
                    token,
                    qty,
                    cost_sol,
                    cost_lamports,
                    qty_raw,
                    qty_decimals,
                    opened_ts,
                    state,
                    accounting_bucket
                 FROM positions
                 WHERE position_id = ?1
                 LIMIT 1",
            )
            .context("failed to prepare execution canary position query")?;
        stmt.query_row(params![position_id], execution_canary_position_from_row)
            .optional()
            .context("failed querying execution canary position")
    }

    fn load_execution_canary_open_positions(&self) -> Result<Vec<ExecutionCanaryOwnedPosition>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    position_id,
                    token,
                    qty,
                    cost_sol,
                    cost_lamports,
                    qty_raw,
                    qty_decimals,
                    opened_ts,
                    state,
                    accounting_bucket
                 FROM positions
                 WHERE accounting_bucket = ?1
                   AND state = ?2
                 ORDER BY opened_ts ASC, position_id ASC",
            )
            .context("failed to prepare execution canary open positions query")?;
        let rows = stmt
            .query_map(
                params![
                    EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
                    EXECUTION_CANARY_POSITION_STATE_OPEN,
                ],
                execution_canary_position_from_row,
            )
            .context("failed querying execution canary open positions")?;
        let positions = rows
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("failed reading execution canary open positions")?;
        Ok(positions)
    }
}

fn validate_position_inputs(order_id: &str, token: &str, qty: f64) -> Result<()> {
    if order_id.trim().is_empty() {
        return Err(anyhow!(
            "execution canary position order_id must not be empty"
        ));
    }
    if token.trim().is_empty() {
        return Err(anyhow!("execution canary position token must not be empty"));
    }
    if !qty.is_finite() || qty <= 0.0 {
        return Err(anyhow!(
            "invalid execution canary position qty={qty} (must be finite and > 0)"
        ));
    }
    Ok(())
}

fn validate_slippage_inputs(slippage_bps: Option<f64>, soft_slippage_limit_bps: f64) -> Result<()> {
    if !soft_slippage_limit_bps.is_finite() || soft_slippage_limit_bps < 0.0 {
        return Err(anyhow!(
            "invalid execution canary sell soft_slippage_limit_bps={soft_slippage_limit_bps}"
        ));
    }
    if let Some(slippage_bps) = slippage_bps {
        if !slippage_bps.is_finite() {
            return Err(anyhow!(
                "invalid execution canary sell slippage_bps={slippage_bps}"
            ));
        }
    }
    Ok(())
}

pub(crate) fn execution_canary_position_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ExecutionCanaryOwnedPosition> {
    read_execution_canary_position_row(row).map_err(to_sql_conversion_error)
}

fn read_execution_canary_position_row(
    row: &rusqlite::Row<'_>,
) -> Result<ExecutionCanaryOwnedPosition> {
    let opened_raw: String = row.get(7).context("failed reading positions.opened_ts")?;
    let opened_ts = DateTime::parse_from_rfc3339(&opened_raw)
        .map(|dt| dt.with_timezone(&Utc))
        .with_context(|| format!("invalid positions.opened_ts value: {opened_raw}"))?;
    let cost_lamports_raw: Option<i64> = row
        .get(4)
        .context("failed reading positions.cost_lamports")?;
    let cost_lamports = match cost_lamports_raw {
        Some(raw) if raw < 0 => {
            return Err(anyhow!(
                "invalid negative positions.cost_lamports={raw} in execution canary position"
            ));
        }
        Some(raw) => Some(Lamports::new(raw as u64)),
        None => None,
    };
    let qty_exact = token_quantity_from_sql(
        row.get(5).context("failed reading positions.qty_raw")?,
        row.get(6)
            .context("failed reading positions.qty_decimals")?,
        "execution canary position",
    )?;
    Ok(ExecutionCanaryOwnedPosition {
        position_id: row.get(0).context("failed reading positions.position_id")?,
        token: row.get(1).context("failed reading positions.token")?,
        accounting_bucket: row
            .get(9)
            .context("failed reading positions.accounting_bucket")?,
        qty: row.get(2).context("failed reading positions.qty")?,
        qty_exact,
        cost_sol: row.get(3).context("failed reading positions.cost_sol")?,
        cost_lamports,
        opened_ts,
        state: row.get(8).context("failed reading positions.state")?,
    })
}

fn execution_canary_position_id(order_id: &str) -> String {
    format!("exec-canary-pos:{order_id}")
}
