use copybot_storage_core::ObservedSolLegSwap;

#[derive(Debug, Clone)]
pub(crate) struct SolLegTrade {
    pub ts: chrono::DateTime<chrono::Utc>,
    pub trader_id: u32,
    pub sol_notional: f64,
}

pub(crate) fn is_sol_buy(swap: &ObservedSolLegSwap) -> bool {
    swap.is_buy
}

pub(crate) fn is_sol_sell(swap: &ObservedSolLegSwap) -> bool {
    !swap.is_buy
}

pub(crate) fn sol_leg_token_and_notional(swap: &ObservedSolLegSwap) -> (&str, f64) {
    (swap.token_mint.as_str(), swap.sol_notional)
}
