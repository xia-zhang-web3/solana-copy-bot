use copybot_core_types::SwapEvent;

use super::{ShadowSwapSide, ShadowTaskKey};

pub(crate) fn classify_swap_side(swap: &SwapEvent) -> Option<ShadowSwapSide> {
    const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
    if swap.token_in == SOL_MINT && swap.token_out != SOL_MINT {
        return Some(ShadowSwapSide::Buy);
    }
    if swap.token_out == SOL_MINT && swap.token_in != SOL_MINT {
        return Some(ShadowSwapSide::Sell);
    }
    None
}

pub(crate) fn shadow_task_key_for_swap(swap: &SwapEvent, side: ShadowSwapSide) -> ShadowTaskKey {
    match side {
        ShadowSwapSide::Buy => ShadowTaskKey {
            wallet: swap.wallet.clone(),
            token: swap.token_out.clone(),
        },
        ShadowSwapSide::Sell => ShadowTaskKey {
            wallet: swap.wallet.clone(),
            token: swap.token_in.clone(),
        },
    }
}
