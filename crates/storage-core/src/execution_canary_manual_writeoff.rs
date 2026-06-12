use crate::{
    execution_canary_position_close::close_execution_canary_open_position_on_conn,
    execution_canary_state::{execution_canary_client_order_id, execution_canary_order_id},
    money::u64_to_sql_i64,
    ExecutionCanaryManualWriteOffResult, SqliteDiscoveryStore,
    EXECUTION_CANARY_POSITION_CLOSE_NO_POSITION, EXECUTION_ERROR_TERMINAL_SELL_NO_ROUTE,
    EXECUTION_SIMULATION_STATUS_NOT_RUN, EXECUTION_STATUS_CANARY_FAILED,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS;
use rusqlite::params;
use serde_json::json;

const MANUAL_WRITE_OFF_SIGNAL_PREFIX: &str = "manual:terminal-write-off";
const MANUAL_WRITE_OFF_EVENT_TYPE: &str = "execution_canary_manual_terminal_write_off";
const MANUAL_WRITE_OFF_REASON_PREFIX: &str = "manual_terminal_write_off";
const TERMINAL_WRITE_OFF_REASON: &str = "terminal_failed_sell_no_route_written_off";
const TERMINAL_WRITE_OFF_EXIT_PRICE_SOL: f64 = 0.0;
const TERMINAL_WRITE_OFF_DUST_QTY_EPSILON: f64 = 1e-12;
const TERMINAL_WRITE_OFF_MAX_POSITIONS_PER_TOKEN: usize = 32;

impl SqliteDiscoveryStore {
    pub fn record_execution_canary_manual_terminal_write_off(
        &self,
        token: &str,
        route: &str,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<ExecutionCanaryManualWriteOffResult> {
        validate_manual_write_off_inputs(token, route, reason)?;
        self.ensure_history_retention_tables()?;
        let nonce = uuid::Uuid::new_v4();
        let signal_id = format!("{MANUAL_WRITE_OFF_SIGNAL_PREFIX}:{nonce}");
        let order_id = execution_canary_order_id(&signal_id);
        let client_order_id = execution_canary_client_order_id(&signal_id);
        let now_raw = now.to_rfc3339();
        let simulation_error =
            format!("{MANUAL_WRITE_OFF_REASON_PREFIX}: {TERMINAL_WRITE_OFF_REASON}: {reason}");
        let details_json = json!({
            "source": "copybot_execution_tiny_writeoff",
            "token": token,
            "route": route,
            "reason": reason,
            "order_id": order_id,
            "signal_id": signal_id,
        })
        .to_string();

        self.with_immediate_transaction_retry(
            "execution canary manual terminal write-off",
            |conn| {
                let first_position =
                    self.load_execution_canary_open_position(token)?.ok_or_else(|| {
                        anyhow!("no execution canary open position for token {token}")
                    })?;

                conn.execute(
                    "INSERT INTO copy_signals(
                        signal_id,
                        wallet_id,
                        side,
                        token,
                        notional_sol,
                        notional_lamports,
                        notional_origin,
                        ts,
                        status
                     ) VALUES (?1, ?2, 'sell', ?3, 0.0, ?4, ?5, ?6, ?7)",
                    params![
                        signal_id,
                        "manual-terminal-write-off",
                        token,
                        u64_to_sql_i64("copy_signals.notional_lamports", 1)?,
                        COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS,
                        now_raw,
                        MANUAL_WRITE_OFF_SIGNAL_PREFIX,
                    ],
                )
                .context("failed inserting manual terminal write-off copy signal")?;

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
                    ) VALUES (?1, ?2, ?3, ?4, ?4, ?5, ?6, ?7, NULL, ?8, ?9, 1)",
                    params![
                        order_id,
                        signal_id,
                        route,
                        now_raw,
                        EXECUTION_STATUS_CANARY_FAILED,
                        EXECUTION_ERROR_TERMINAL_SELL_NO_ROUTE,
                        client_order_id,
                        EXECUTION_SIMULATION_STATUS_NOT_RUN,
                        simulation_error,
                    ],
                )
                .context("failed inserting manual terminal write-off order")?;

                conn.execute(
                    "INSERT INTO risk_events(event_id, type, severity, ts, details_json)
                     VALUES (?1, ?2, 'warn', ?3, ?4)",
                    params![
                        uuid::Uuid::new_v4().to_string(),
                        MANUAL_WRITE_OFF_EVENT_TYPE,
                        now_raw,
                        details_json,
                    ],
                )
                .context("failed inserting manual terminal write-off risk event")?;

                let mut close_results = Vec::new();
                let mut position = Some(first_position);
                for _ in 0..TERMINAL_WRITE_OFF_MAX_POSITIONS_PER_TOKEN {
                    let Some(open_position) = position else {
                        return Ok(ExecutionCanaryManualWriteOffResult {
                            signal_id: signal_id.clone(),
                            order_id: order_id.clone(),
                            close_results,
                        });
                    };
                    close_results.push(close_execution_canary_open_position_on_conn(
                        conn,
                        None,
                        token,
                        open_position.qty,
                        open_position.qty_exact,
                        TERMINAL_WRITE_OFF_EXIT_PRICE_SOL,
                        TERMINAL_WRITE_OFF_DUST_QTY_EPSILON,
                        now,
                    )?);
                    position = self.load_execution_canary_open_position(token)?;
                }

                if position.is_some() {
                    return Err(anyhow!(
                        "manual terminal write-off left more than {TERMINAL_WRITE_OFF_MAX_POSITIONS_PER_TOKEN} open positions for token {token}"
                    ));
                }
                if close_results
                    .iter()
                    .all(|result| result.close_status == EXECUTION_CANARY_POSITION_CLOSE_NO_POSITION)
                {
                    return Err(anyhow!(
                        "manual terminal write-off did not close an open position for token {token}"
                    ));
                }
                Ok(ExecutionCanaryManualWriteOffResult {
                    signal_id: signal_id.clone(),
                    order_id: order_id.clone(),
                    close_results,
                })
            },
        )
    }
}

fn validate_manual_write_off_inputs(token: &str, route: &str, reason: &str) -> Result<()> {
    if token.trim().is_empty() {
        return Err(anyhow!("manual terminal write-off token must not be empty"));
    }
    if route.trim().is_empty() {
        return Err(anyhow!("manual terminal write-off route must not be empty"));
    }
    if reason.trim().is_empty() {
        return Err(anyhow!(
            "manual terminal write-off reason must not be empty"
        ));
    }
    Ok(())
}
