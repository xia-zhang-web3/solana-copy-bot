use crate::{
    ExecutionCanaryOrder, ExecutionCanaryOwnedPositionRecordResult,
    ExecutionCanaryPositionCloseResult, SqliteDiscoveryStore, EXECUTION_STATUS_CANARY_CONFIRMED,
};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::TokenQuantity;

impl SqliteDiscoveryStore {
    pub fn record_execution_canary_confirmed_buy_fill(
        &self,
        order_id: &str,
        token: &str,
        qty: f64,
        qty_exact: Option<TokenQuantity>,
        cost_sol: f64,
        fill_ts: DateTime<Utc>,
    ) -> Result<ExecutionCanaryOwnedPositionRecordResult> {
        let order = self.load_confirmed_canary_fill_order(order_id, "buy")?;
        self.record_execution_canary_open_position(
            &order.order_id,
            token,
            qty,
            qty_exact,
            cost_sol,
            fill_ts,
        )
    }

    pub fn close_execution_canary_confirmed_sell_fill(
        &self,
        order_id: &str,
        token: &str,
        target_qty: f64,
        target_qty_exact: Option<TokenQuantity>,
        exit_price_sol: f64,
        dust_qty_epsilon: f64,
        fill_ts: DateTime<Utc>,
    ) -> Result<ExecutionCanaryPositionCloseResult> {
        self.load_confirmed_canary_fill_order(order_id, "sell")?;
        self.close_execution_canary_open_position(
            token,
            target_qty,
            target_qty_exact,
            exit_price_sol,
            dust_qty_epsilon,
            fill_ts,
        )
    }

    fn load_confirmed_canary_fill_order(
        &self,
        order_id: &str,
        expected_side: &str,
    ) -> Result<ExecutionCanaryOrder> {
        let order = self
            .load_execution_canary_order(order_id)?
            .ok_or_else(|| anyhow!("missing execution canary fill order {order_id}"))?;
        if order.status != EXECUTION_STATUS_CANARY_CONFIRMED {
            return Err(anyhow!(
                "execution canary fill order {order_id} requires confirmed status, got {}",
                order.status
            ));
        }
        if order
            .tx_signature
            .as_deref()
            .map_or(true, |signature| signature.trim().is_empty())
        {
            return Err(anyhow!(
                "execution canary fill order {order_id} requires confirmed tx_signature"
            ));
        }
        let signal = self
            .load_copy_signal_by_signal_id(&order.signal_id)?
            .ok_or_else(|| anyhow!("missing copy signal for execution canary order {order_id}"))?;
        if !signal.side.eq_ignore_ascii_case(expected_side) {
            return Err(anyhow!(
                "execution canary fill order {order_id} expected {expected_side} signal, got {}",
                signal.side
            ));
        }
        Ok(order)
    }
}
