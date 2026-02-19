use anyhow::{anyhow, Result};

use crate::intent::ExecutionIntent;

#[derive(Debug, Clone)]
pub struct ExecutionFill {
    pub order_id: String,
    pub token: String,
    pub qty: f64,
    pub avg_price_sol: f64,
    pub fee_sol: f64,
    pub slippage_bps: f64,
}

pub fn build_fill(
    intent: &ExecutionIntent,
    order_id: &str,
    avg_price_sol: f64,
    slippage_bps: f64,
) -> Result<ExecutionFill> {
    if !avg_price_sol.is_finite() || avg_price_sol <= 0.0 {
        return Err(anyhow!("invalid avg_price_sol={avg_price_sol}"));
    }
    let qty = intent.notional_sol / avg_price_sol;
    if !qty.is_finite() || qty <= 0.0 {
        return Err(anyhow!("invalid computed qty={qty}"));
    }

    Ok(ExecutionFill {
        order_id: order_id.to_string(),
        token: intent.token.clone(),
        qty,
        avg_price_sol,
        fee_sol: 0.0,
        slippage_bps,
    })
}
