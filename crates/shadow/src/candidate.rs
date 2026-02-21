use super::{EPS, SOL_MINT};
use copybot_core_types::SwapEvent;

#[derive(Debug, Clone)]
pub(super) struct ShadowCandidate {
    pub(super) side: String,
    pub(super) token: String,
    pub(super) leader_notional_sol: f64,
    pub(super) price_sol_per_token: f64,
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
        });
    }

    if swap.token_out == SOL_MINT && swap.token_in != SOL_MINT {
        return Some(ShadowCandidate {
            side: "sell".to_string(),
            token: swap.token_in.clone(),
            leader_notional_sol: swap.amount_out,
            price_sol_per_token: swap.amount_out / swap.amount_in,
        });
    }

    None
}
