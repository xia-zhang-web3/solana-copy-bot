use anyhow::{anyhow, Result};

use crate::confirm::ObservedExecutionFill;
use crate::intent::ExecutionIntent;

#[derive(Debug, Clone)]
pub struct ExecutionFill {
    pub order_id: String,
    pub token: String,
    pub notional_sol: f64,
    pub qty: f64,
    pub avg_price_sol: f64,
    pub fee_sol: f64,
    pub slippage_bps: f64,
}

pub fn build_fill_from_priced_intent(
    intent: &ExecutionIntent,
    order_id: &str,
    avg_price_sol: f64,
    slippage_bps: f64,
    fee_sol: f64,
) -> Result<ExecutionFill> {
    if !avg_price_sol.is_finite() || avg_price_sol <= 0.0 {
        return Err(anyhow!("invalid avg_price_sol={avg_price_sol}"));
    }
    if !fee_sol.is_finite() || fee_sol < 0.0 {
        return Err(anyhow!("invalid fee_sol={fee_sol}"));
    }
    let notional_sol = intent.notional_sol;
    if !notional_sol.is_finite() || notional_sol <= 0.0 {
        return Err(anyhow!("invalid notional_sol={notional_sol}"));
    }
    let qty = notional_sol / avg_price_sol;
    if !qty.is_finite() || qty <= 0.0 {
        return Err(anyhow!("invalid computed qty={qty}"));
    }

    Ok(ExecutionFill {
        order_id: order_id.to_string(),
        token: intent.token.clone(),
        notional_sol,
        qty,
        avg_price_sol,
        fee_sol,
        slippage_bps,
    })
}

pub fn build_fill_from_confirmed_observation(
    intent: &ExecutionIntent,
    order_id: &str,
    observed_fill: ObservedExecutionFill,
    slippage_bps: f64,
    fee_sol: f64,
) -> Result<ExecutionFill> {
    if !fee_sol.is_finite() || fee_sol < 0.0 {
        return Err(anyhow!("invalid fee_sol={fee_sol}"));
    }
    if !observed_fill.token_delta_qty.is_finite() || observed_fill.token_delta_qty.abs() <= 1e-12 {
        return Err(anyhow!(
            "invalid observed token_delta_qty={}",
            observed_fill.token_delta_qty
        ));
    }

    let gross_sol_delta_sol =
        (observed_fill.signer_balance_delta_lamports as f64) / 1_000_000_000.0;
    if !gross_sol_delta_sol.is_finite() {
        return Err(anyhow!(
            "invalid observed signer_balance_delta_lamports={}",
            observed_fill.signer_balance_delta_lamports
        ));
    }

    let notional_sol = match intent.side.as_str() {
        "buy" => (-gross_sol_delta_sol - fee_sol).max(0.0),
        "sell" => (gross_sol_delta_sol + fee_sol).max(0.0),
        other => return Err(anyhow!("unsupported execution side: {other}")),
    };
    if !notional_sol.is_finite() || notional_sol <= 0.0 {
        return Err(anyhow!(
            "invalid observed notional_sol={} side={} balance_delta_lamports={} fee_sol={}",
            notional_sol,
            intent.side.as_str(),
            observed_fill.signer_balance_delta_lamports,
            fee_sol
        ));
    }

    let qty = observed_fill.token_delta_qty.abs();
    if !qty.is_finite() || qty <= 0.0 {
        return Err(anyhow!("invalid observed qty={qty}"));
    }

    let avg_price_sol = notional_sol / qty;
    if !avg_price_sol.is_finite() || avg_price_sol <= 0.0 {
        return Err(anyhow!("invalid observed avg_price_sol={avg_price_sol}"));
    }

    Ok(ExecutionFill {
        order_id: order_id.to_string(),
        token: intent.token.clone(),
        notional_sol,
        qty,
        avg_price_sol,
        fee_sol,
        slippage_bps,
    })
}
