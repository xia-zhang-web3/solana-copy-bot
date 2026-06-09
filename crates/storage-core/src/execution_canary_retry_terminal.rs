use crate::{
    ExecutionCanaryOrder, SqliteDiscoveryStore, EXECUTION_ERROR_BUILD_FAILED,
    EXECUTION_ERROR_SIMULATION_FAILED, EXECUTION_ERROR_TERMINAL_SELL_NO_ROUTE,
    EXECUTION_ERROR_TERMINAL_SELL_SIMULATION_FAILED, EXECUTION_SIMULATION_STATUS_NOT_RUN,
    EXECUTION_STATUS_CANARY_CANDIDATE, EXECUTION_STATUS_CANARY_FAILED,
};
use anyhow::{anyhow, Context, Result};
use rusqlite::{params, OptionalExtension};

impl SqliteDiscoveryStore {
    pub fn mark_execution_canary_terminal_sell_simulation_blocked(
        &self,
        order_id: &str,
        reason: &str,
    ) -> Result<ExecutionCanaryOrder> {
        validate_terminal_reason(reason)?;
        self.with_immediate_transaction_retry(
            "execution canary terminal sell simulation blocked mark",
            |conn| {
                let current: Option<(String, Option<String>, Option<String>)> = conn
                    .query_row(
                        "SELECT status, err_code, tx_signature
                         FROM orders
                         WHERE order_id = ?1
                         LIMIT 1",
                        params![order_id],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .optional()
                    .context("failed loading terminal sell simulation state")?;
                let Some((status, err_code, tx_signature)) = current else {
                    return Err(anyhow!("missing execution canary order {order_id}"));
                };
                if status != EXECUTION_STATUS_CANARY_FAILED
                    || err_code.as_deref() != Some(EXECUTION_ERROR_SIMULATION_FAILED)
                {
                    return Err(anyhow!(
                        "invalid terminal sell simulation transition for {order_id}: status={status} err_code={:?}",
                        err_code
                    ));
                }
                if tx_signature
                    .as_deref()
                    .is_some_and(|signature| !signature.trim().is_empty())
                {
                    return Err(anyhow!(
                        "terminal sell simulation block is unsafe for {order_id}: tx_signature is present"
                    ));
                }
                conn.execute(
                    "UPDATE orders
                     SET err_code = ?2,
                         simulation_error = CASE
                             WHEN simulation_error IS NULL OR TRIM(simulation_error) = '' THEN ?3
                             ELSE ?3 || ': ' || simulation_error
                         END
                     WHERE order_id = ?1",
                    params![
                        order_id,
                        EXECUTION_ERROR_TERMINAL_SELL_SIMULATION_FAILED,
                        reason,
                    ],
                )
                .context("failed marking terminal sell simulation blocked")?;
                Ok(())
            },
        )?;
        self.load_execution_canary_order(order_id)?.ok_or_else(|| {
            anyhow!("missing execution canary order after terminal sell simulation blocked mark")
        })
    }

    pub fn mark_execution_canary_terminal_sell_no_route_blocked(
        &self,
        order_id: &str,
        reason: &str,
    ) -> Result<ExecutionCanaryOrder> {
        validate_terminal_reason(reason)?;
        self.with_immediate_transaction_retry(
            "execution canary terminal sell no-route blocked mark",
            |conn| {
                let current: Option<(String, Option<String>, Option<String>)> = conn
                    .query_row(
                        "SELECT status, err_code, tx_signature
                         FROM orders
                         WHERE order_id = ?1
                         LIMIT 1",
                        params![order_id],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .optional()
                    .context("failed loading terminal sell no-route state")?;
                let Some((status, err_code, tx_signature)) = current else {
                    return Err(anyhow!("missing execution canary order {order_id}"));
                };
                if status != EXECUTION_STATUS_CANARY_FAILED
                    || err_code.as_deref() != Some(EXECUTION_ERROR_BUILD_FAILED)
                {
                    return Err(anyhow!(
                        "invalid terminal sell no-route transition for {order_id}: status={status} err_code={:?}",
                        err_code
                    ));
                }
                if tx_signature
                    .as_deref()
                    .is_some_and(|signature| !signature.trim().is_empty())
                {
                    return Err(anyhow!(
                        "terminal sell no-route block is unsafe for {order_id}: tx_signature is present"
                    ));
                }
                conn.execute(
                    "UPDATE orders
                     SET err_code = ?2,
                         simulation_error = CASE
                             WHEN simulation_error IS NULL OR TRIM(simulation_error) = '' THEN ?3
                             ELSE ?3 || ': ' || simulation_error
                         END
                     WHERE order_id = ?1",
                    params![order_id, EXECUTION_ERROR_TERMINAL_SELL_NO_ROUTE, reason],
                )
                .context("failed marking terminal sell no-route blocked")?;
                Ok(())
            },
        )?;
        self.load_execution_canary_order(order_id)?.ok_or_else(|| {
            anyhow!("missing execution canary order after terminal sell no-route blocked mark")
        })
    }

    pub fn mark_execution_canary_terminal_sell_no_route_retry_candidate(
        &self,
        order_id: &str,
        now: chrono::DateTime<chrono::Utc>,
        reason: &str,
    ) -> Result<ExecutionCanaryOrder> {
        validate_terminal_reason(reason)?;
        let now_rfc3339 = now.to_rfc3339();
        self.with_immediate_transaction_retry(
            "execution canary terminal sell no-route retry candidate mark",
            |conn| {
                let current: Option<(String, Option<String>, Option<String>)> = conn
                    .query_row(
                        "SELECT status, err_code, tx_signature
                         FROM orders
                         WHERE order_id = ?1
                         LIMIT 1",
                        params![order_id],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .optional()
                    .context("failed loading terminal sell no-route retry state")?;
                let Some((status, err_code, tx_signature)) = current else {
                    return Err(anyhow!("missing execution canary order {order_id}"));
                };
                if status != EXECUTION_STATUS_CANARY_FAILED
                    || err_code.as_deref() != Some(EXECUTION_ERROR_TERMINAL_SELL_NO_ROUTE)
                {
                    return Err(anyhow!(
                        "invalid terminal sell no-route retry transition for {order_id}: status={status} err_code={:?}",
                        err_code
                    ));
                }
                if tx_signature
                    .as_deref()
                    .is_some_and(|signature| !signature.trim().is_empty())
                {
                    return Err(anyhow!(
                        "terminal sell no-route retry is unsafe for {order_id}: tx_signature is present"
                    ));
                }
                conn.execute(
                    "UPDATE orders
                     SET status = ?2,
                         submit_ts = ?3,
                         confirm_ts = NULL,
                         err_code = NULL,
                         simulation_status = ?4,
                         simulation_error = ?5,
                         attempt = 1
                     WHERE order_id = ?1",
                    params![
                        order_id,
                        EXECUTION_STATUS_CANARY_CANDIDATE,
                        now_rfc3339,
                        EXECUTION_SIMULATION_STATUS_NOT_RUN,
                        reason,
                    ],
                )
                .context("failed marking terminal sell no-route retry candidate")?;
                Ok(())
            },
        )?;
        self.load_execution_canary_order(order_id)?.ok_or_else(|| {
            anyhow!("missing execution canary order after terminal sell no-route retry candidate")
        })
    }
}

fn validate_terminal_reason(reason: &str) -> Result<()> {
    if reason.trim().is_empty() {
        return Err(anyhow!(
            "execution canary terminal sell simulation reason must be non-empty"
        ));
    }
    Ok(())
}
