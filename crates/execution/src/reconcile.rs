use anyhow::{anyhow, Result};
use copybot_core_types::Lamports;

use crate::confirm::ObservedExecutionFill;
use crate::intent::ExecutionIntent;
use crate::money::{lamports_to_sol, sol_to_lamports_ceil};

#[derive(Debug, Clone)]
pub struct ExecutionFill {
    pub order_id: String,
    pub token: String,
    pub notional_sol: f64,
    pub notional_lamports: Lamports,
    pub qty: f64,
    pub avg_price_sol: f64,
    pub fee_sol: f64,
    pub fee_lamports: Lamports,
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
    let notional_lamports = intent.notional_lamports()?;
    let fee_lamports = sol_to_lamports_ceil(fee_sol, "execution fill fee_sol")?;
    let qty = notional_sol / avg_price_sol;
    if !qty.is_finite() || qty <= 0.0 {
        return Err(anyhow!("invalid computed qty={qty}"));
    }

    Ok(ExecutionFill {
        order_id: order_id.to_string(),
        token: intent.token.clone(),
        notional_sol,
        notional_lamports,
        qty,
        avg_price_sol,
        fee_sol,
        fee_lamports,
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
    match intent.side.as_str() {
        "buy" if observed_fill.token_delta_qty <= 1e-12 => {
            return Err(anyhow!(
                "observed token_delta_qty sign mismatch side=buy token_delta_qty={}",
                observed_fill.token_delta_qty
            ));
        }
        "sell" if observed_fill.token_delta_qty >= -1e-12 => {
            return Err(anyhow!(
                "observed token_delta_qty sign mismatch side=sell token_delta_qty={}",
                observed_fill.token_delta_qty
            ));
        }
        "buy" | "sell" => {}
        other => return Err(anyhow!("unsupported execution side: {other}")),
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
    let fee_lamports = sol_to_lamports_ceil(fee_sol, "execution fill fee_sol")?;
    let notional_lamports_i128 = match intent.side.as_str() {
        "buy" => (-i128::from(observed_fill.signer_balance_delta_lamports))
            .checked_sub(i128::from(fee_lamports.as_u64()))
            .ok_or_else(|| anyhow!("observed buy notional_lamports overflow"))?,
        "sell" => i128::from(observed_fill.signer_balance_delta_lamports)
            .checked_add(i128::from(fee_lamports.as_u64()))
            .ok_or_else(|| anyhow!("observed sell notional_lamports overflow"))?,
        other => return Err(anyhow!("unsupported execution side: {other}")),
    };
    let notional_lamports = u64::try_from(notional_lamports_i128)
        .map(Lamports::new)
        .map_err(|_| {
            anyhow!(
                "invalid observed notional_lamports={} side={} balance_delta_lamports={} fee_lamports={}",
                notional_lamports_i128,
                intent.side.as_str(),
                observed_fill.signer_balance_delta_lamports,
                fee_lamports.as_u64()
            )
        })?;
    let notional_sol = lamports_to_sol(notional_lamports);

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
        notional_lamports,
        qty,
        avg_price_sol,
        fee_sol,
        fee_lamports,
        slippage_bps,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent::{ExecutionIntent, ExecutionSide};
    use chrono::Utc;

    fn make_intent(side: ExecutionSide) -> ExecutionIntent {
        ExecutionIntent {
            signal_id: "sig".to_string(),
            leader_wallet: "wallet".to_string(),
            side,
            token: "token-a".to_string(),
            notional_sol: 0.1,
            signal_ts: Utc::now(),
        }
    }

    #[test]
    fn build_fill_from_confirmed_observation_rejects_wrong_sign_for_buy() {
        let error = build_fill_from_confirmed_observation(
            &make_intent(ExecutionSide::Buy),
            "order-1",
            ObservedExecutionFill {
                signer_balance_delta_lamports: -100_005_000,
                token_delta_qty: -2.0,
            },
            50.0,
            0.000005,
        )
        .expect_err("buy should reject negative token delta");
        assert!(
            error
                .to_string()
                .contains("observed token_delta_qty sign mismatch"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn build_fill_from_confirmed_observation_rejects_wrong_sign_for_sell() {
        let error = build_fill_from_confirmed_observation(
            &make_intent(ExecutionSide::Sell),
            "order-1",
            ObservedExecutionFill {
                signer_balance_delta_lamports: 89_995_000,
                token_delta_qty: 2.0,
            },
            50.0,
            0.000005,
        )
        .expect_err("sell should reject positive token delta");
        assert!(
            error
                .to_string()
                .contains("observed token_delta_qty sign mismatch"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn build_fill_from_confirmed_observation_preserves_exact_lamports_for_buy() {
        let fill = build_fill_from_confirmed_observation(
            &make_intent(ExecutionSide::Buy),
            "order-1",
            ObservedExecutionFill {
                signer_balance_delta_lamports: -100_005_000,
                token_delta_qty: 2.0,
            },
            50.0,
            0.000005,
        )
        .expect("buy fill should succeed");

        assert_eq!(fill.notional_lamports, Lamports::new(100_000_000));
        assert_eq!(fill.fee_lamports, Lamports::new(5_000));
    }

    #[test]
    fn build_fill_from_confirmed_observation_preserves_exact_lamports_for_sell() {
        let fill = build_fill_from_confirmed_observation(
            &make_intent(ExecutionSide::Sell),
            "order-1",
            ObservedExecutionFill {
                signer_balance_delta_lamports: 89_995_000,
                token_delta_qty: -2.0,
            },
            50.0,
            0.000005,
        )
        .expect("sell fill should succeed");

        assert_eq!(fill.notional_lamports, Lamports::new(90_000_000));
        assert_eq!(fill.fee_lamports, Lamports::new(5_000));
    }
}
