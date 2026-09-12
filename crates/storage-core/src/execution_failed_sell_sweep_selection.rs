use crate::{
    ExecutionCanaryOrder, SqliteDiscoveryStore, EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
    EXECUTION_CANARY_POSITION_STATE_OPEN, EXECUTION_ERROR_BUILD_FAILED,
    EXECUTION_ERROR_SIMULATION_FAILED, EXECUTION_STATUS_CANARY_FAILED,
};
use anyhow::{Context, Result};
use rusqlite::{params, OptionalExtension};

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionFailedSellSweepOrder {
    pub order: ExecutionCanaryOrder,
    pub quote_event_id: Option<String>,
}

impl SqliteDiscoveryStore {
    /// A selector hint only. The existing handlers/atomic writer recheck authority.
    pub fn load_execution_failed_sell_sweep_order(
        &self,
        route: &str,
        order_id: &str,
    ) -> Result<Option<ExecutionFailedSellSweepOrder>> {
        let Some(order) = self.load_execution_canary_order(order_id)? else {
            return Ok(None);
        };
        if !order.order_id.starts_with("exec-canary:")
            || order.route != route
            || order.status != EXECUTION_STATUS_CANARY_FAILED
            || !matches!(
                order.err_code.as_deref(),
                Some(EXECUTION_ERROR_BUILD_FAILED | EXECUTION_ERROR_SIMULATION_FAILED)
            )
            || order
                .tx_signature
                .as_deref()
                .is_some_and(|s| !s.trim().is_empty())
        {
            return Ok(None);
        }
        let owned_sell: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM copy_signals s
            JOIN positions p ON p.token=s.token WHERE s.signal_id=?1 AND lower(s.side)='sell'
            AND p.accounting_bucket=?2 AND p.state=?3)",
            params![
                order.signal_id,
                EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
                EXECUTION_CANARY_POSITION_STATE_OPEN
            ],
            |r| r.get(0),
        )?;
        if !owned_sell {
            return Ok(None);
        }
        let quote_event_id = if order.err_code.as_deref() == Some(EXECUTION_ERROR_BUILD_FAILED) {
            // One event per order, with the same eligibility/latest-event rule as the
            // legacy failed-build reader. Duplicate quote rows cannot consume visits.
            let event = self.conn.query_row("SELECT event.event_id FROM execution_quote_canary_events event
                JOIN copy_signals s ON s.signal_id=event.signal_id
                WHERE event.signal_id=?1 AND lower(event.side)='sell' AND event.quote_status='ok'
                AND event.decision_status IN ('would_execute','would_force_exit')
                AND EXISTS(SELECT 1 FROM positions p WHERE p.token=s.token AND p.accounting_bucket=?2 AND p.state=?3
                    AND event.request_ts>=p.opened_ts AND (p.position_id LIKE 'exec-canary-pos:recovery-orphan:%'
                        OR (substr(p.position_id,1,length('exec-canary-pos:'))='exec-canary-pos:'
                            AND EXISTS(SELECT 1 FROM orders buy_order
                                WHERE buy_order.order_id=substr(p.position_id,length('exec-canary-pos:')+1)))))
                ORDER BY event.request_ts DESC,event.event_id DESC LIMIT 1",
                params![order.signal_id,EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,EXECUTION_CANARY_POSITION_STATE_OPEN], |r|r.get::<_,String>(0))
                .optional().context("failed selecting one failed SELL quote")?;
            let Some(event) = event else {
                return Ok(None);
            };
            Some(event)
        } else {
            None
        };
        Ok(Some(ExecutionFailedSellSweepOrder {
            order,
            quote_event_id,
        }))
    }
}
