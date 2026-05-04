use copybot_core_types::SwapEvent;

pub(crate) const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

#[derive(Debug, Clone)]
pub(crate) struct SolLegTrade {
    pub ts: chrono::DateTime<chrono::Utc>,
    pub wallet_id: String,
    pub sol_notional: f64,
}

pub(crate) fn is_sol_buy(swap: &SwapEvent) -> bool {
    swap.token_in == SOL_MINT && swap.token_out != SOL_MINT
}

pub(crate) fn is_sol_sell(swap: &SwapEvent) -> bool {
    swap.token_out == SOL_MINT && swap.token_in != SOL_MINT
}

pub(crate) fn sol_leg_token_and_notional(swap: &SwapEvent) -> Option<(&str, f64)> {
    if is_sol_buy(swap) {
        Some((swap.token_out.as_str(), swap.amount_in))
    } else if is_sol_sell(swap) {
        Some((swap.token_in.as_str(), swap.amount_out))
    } else {
        None
    }
}
