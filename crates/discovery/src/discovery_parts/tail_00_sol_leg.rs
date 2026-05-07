use super::*;

pub(crate) fn is_sol_buy(swap: &SwapEvent) -> bool {
    swap.token_in == SOL_MINT && swap.token_out != SOL_MINT
}

pub(crate) fn is_sol_sell(swap: &SwapEvent) -> bool {
    swap.token_out == SOL_MINT && swap.token_in != SOL_MINT
}

pub(crate) fn sol_leg_token(swap: &SwapEvent) -> Option<&str> {
    if is_sol_buy(swap) {
        Some(swap.token_out.as_str())
    } else if is_sol_sell(swap) {
        Some(swap.token_in.as_str())
    } else {
        None
    }
}
