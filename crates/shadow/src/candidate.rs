use super::{EPS, SOL_MINT};
use copybot_core_types::{Lamports, SwapEvent, TokenQuantity};

#[derive(Debug, Clone)]
pub(super) struct ShadowCandidate {
    pub(super) side: String,
    pub(super) token: String,
    pub(super) leader_notional_sol: f64,
    pub(super) price_sol_per_token: f64,
    pub(super) exact_token_qty: Option<TokenQuantity>,
    pub(super) exact_leader_notional_lamports: Option<Lamports>,
}

pub(super) fn to_shadow_candidate(swap: &SwapEvent) -> Option<ShadowCandidate> {
    if swap.amount_in <= EPS || swap.amount_out <= EPS {
        return None;
    }

    if swap.token_in == SOL_MINT && swap.token_out != SOL_MINT {
        return Some(ShadowCandidate {
            side: "buy".to_string(),
            token: swap.token_out.clone(),
            leader_notional_sol: swap.amount_in,
            price_sol_per_token: swap.amount_in / swap.amount_out,
            exact_token_qty: swap
                .exact_amounts
                .as_ref()
                .and_then(|exact| exact.amount_out_quantity().ok()),
            exact_leader_notional_lamports: swap
                .exact_amounts
                .as_ref()
                .and_then(|exact| exact.amount_in_quantity().ok())
                .filter(|amount| amount.decimals() == 9)
                .map(|amount| Lamports::new(amount.raw())),
        });
    }

    if swap.token_out == SOL_MINT && swap.token_in != SOL_MINT {
        return Some(ShadowCandidate {
            side: "sell".to_string(),
            token: swap.token_in.clone(),
            leader_notional_sol: swap.amount_out,
            price_sol_per_token: swap.amount_out / swap.amount_in,
            exact_token_qty: swap
                .exact_amounts
                .as_ref()
                .and_then(|exact| exact.amount_in_quantity().ok()),
            exact_leader_notional_lamports: swap
                .exact_amounts
                .as_ref()
                .and_then(|exact| exact.amount_out_quantity().ok())
                .filter(|amount| amount.decimals() == 9)
                .map(|amount| Lamports::new(amount.raw())),
        });
    }

    None
}
