use crate::{
    money::token_quantity_from_sql, shadow_lots::to_sql_conversion_error,
    ExecutionCanaryOwnedPosition, ExecutionCanarySellDecision, SqliteDiscoveryStore,
    EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET, EXECUTION_CANARY_POSITION_STATE_OPEN,
    EXECUTION_CANARY_SELL_DECISION_EXECUTE, EXECUTION_CANARY_SELL_DECISION_FORCE_EXIT,
    EXECUTION_CANARY_SELL_DECISION_NO_POSITION,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::Lamports;
use rusqlite::{params, OptionalExtension};

impl SqliteDiscoveryStore {
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

    pub fn has_execution_canary_position_history(&self, token: &str) -> Result<bool> {
        let found: Option<i64> = self
            .conn
            .query_row(
                "SELECT 1
                 FROM positions
                 WHERE token = ?1
                   AND accounting_bucket = ?2
                 LIMIT 1",
                params![token, EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET],
                |row| row.get(0),
            )
            .optional()
            .context("failed checking execution canary position history")?;
        Ok(found.is_some())
    }

    pub fn execution_canary_recovery_opened_ts(
        &self,
        token: &str,
    ) -> Result<Option<DateTime<Utc>>> {
        let opened_ts: Option<String> = self
            .conn
            .query_row(
                "SELECT opened_ts
                 FROM positions
                 WHERE token = ?1
                   AND accounting_bucket = ?2
                   AND position_id NOT LIKE 'exec-canary-pos:recovery-orphan:%'
                 ORDER BY opened_ts ASC, position_id ASC
                 LIMIT 1",
                params![token, EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET],
                |row| row.get(0),
            )
            .optional()
            .context("failed loading execution canary recovery opened_ts")?;
        opened_ts
            .as_deref()
            .map(|raw| parse_position_ts(raw, "positions.opened_ts"))
            .transpose()
    }

    pub fn retimestamp_execution_canary_orphan_open_position(
        &self,
        token: &str,
        opened_ts: DateTime<Utc>,
    ) -> Result<bool> {
        let opened_ts = opened_ts.to_rfc3339();
        let written = self
            .execute_with_retry(|conn| {
                conn.execute(
                    "UPDATE positions
                     SET opened_ts = ?3
                     WHERE token = ?1
                       AND accounting_bucket = ?2
                       AND state = ?4
                       AND position_id LIKE 'exec-canary-pos:recovery-orphan:%'",
                    params![
                        token,
                        EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
                        opened_ts,
                        EXECUTION_CANARY_POSITION_STATE_OPEN,
                    ],
                )
            })
            .context("failed retimestamping execution canary orphan position")?;
        Ok(written > 0)
    }

    pub fn list_execution_canary_open_recovery_orphan_positions(
        &self,
    ) -> Result<Vec<ExecutionCanaryOwnedPosition>> {
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
                   AND position_id LIKE 'exec-canary-pos:recovery-orphan:%'
                 ORDER BY opened_ts ASC, position_id ASC",
            )
            .context("failed to prepare execution canary recovery orphan position list")?;
        let rows = stmt
            .query_map(
                params![
                    EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
                    EXECUTION_CANARY_POSITION_STATE_OPEN,
                ],
                execution_canary_position_from_row,
            )
            .context("failed querying execution canary recovery orphan positions")?;
        let mut positions = Vec::new();
        for row in rows {
            positions.push(row.context("failed reading execution canary recovery orphan row")?);
        }
        Ok(positions)
    }

    pub fn list_execution_canary_open_positions(
        &self,
    ) -> Result<Vec<ExecutionCanaryOwnedPosition>> {
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
            .context("failed to prepare execution canary open position list")?;
        let rows = stmt
            .query_map(
                params![
                    EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
                    EXECUTION_CANARY_POSITION_STATE_OPEN,
                ],
                execution_canary_position_from_row,
            )
            .context("failed querying execution canary open positions")?;
        let mut positions = Vec::new();
        for row in rows {
            positions.push(row.context("failed reading execution canary open position row")?);
        }
        Ok(positions)
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

    pub fn execution_canary_realized_loss_sol_since(&self, since: DateTime<Utc>) -> Result<f64> {
        self.execution_canary_realized_loss_sol_since_query(since, true)
    }

    pub fn execution_canary_entry_safety_loss_sol_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<f64> {
        self.execution_canary_realized_loss_sol_since_query(since, false)
    }

    fn execution_canary_realized_loss_sol_since_query(
        &self,
        since: DateTime<Utc>,
        include_recovery_orphans: bool,
    ) -> Result<f64> {
        let since_raw = since.to_rfc3339();
        let recovery_filter = if include_recovery_orphans {
            ""
        } else {
            " AND position_id NOT LIKE 'exec-canary-pos:recovery-orphan:%'"
        };
        let sql = format!(
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
               AND closed_ts >= ?3{recovery_filter}"
        );
        self.conn
            .query_row(
                &sql,
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
}

fn parse_position_ts(raw: &str, label: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .with_context(|| format!("invalid {label} value: {raw}"))
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
