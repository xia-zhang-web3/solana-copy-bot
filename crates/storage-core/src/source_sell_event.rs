use crate::observed_row::row_to_swap_event;
use anyhow::Result;
use copybot_core_types::SwapEvent;
use rusqlite::Connection;
const SOL: &str = "So11111111111111111111111111111111111111112";

/// Same shape/exact-operand boundary as legacy execution_sell_intent, without
/// its active-follow admission. Do not reinterpret source amounts as owned size.
pub(crate) fn valid(swap: &SwapEvent) -> bool {
    if swap.token_out != SOL
        || swap.token_in == SOL
        || swap.token_in.trim().is_empty()
        || swap.signature.trim().is_empty()
        || swap.wallet.trim().is_empty()
        || !swap.amount_in.is_finite()
        || !swap.amount_out.is_finite()
        || swap.amount_in <= 1e-12
        || swap.amount_out <= 1e-12
    {
        return false;
    }
    match swap.exact_amounts.as_ref() {
        None => true,
        Some(exact) => match (exact.amount_in_quantity(), exact.amount_out_quantity()) {
            (Ok(token), Ok(sol)) => token.raw() > 0 && sol.raw() > 0 && sol.decimals() == 9,
            _ => false,
        },
    }
}

pub(crate) fn same(a: &SwapEvent, b: &SwapEvent) -> bool {
    a.signature == b.signature
        && a.wallet == b.wallet
        && a.token_in == b.token_in
        && a.token_out == b.token_out
        && a.ts_utc == b.ts_utc
        && a.slot == b.slot
        && a.dex == b.dex
        && a.amount_in == b.amount_in
        && a.amount_out == b.amount_out
        && a.exact_amounts == b.exact_amounts
}

pub(crate) fn matches_observed(conn: &Connection, swap: &SwapEvent) -> Result<bool> {
    let mut statement = conn.prepare("SELECT signature,wallet_id,dex,token_in,token_out,qty_in,qty_out,slot,ts,
        qty_in_raw,qty_in_decimals,qty_out_raw,qty_out_decimals FROM observed_swaps WHERE signature=?1")?;
    let mut rows = statement.query([&swap.signature])?;
    let Some(row) = rows.next()? else {
        return Ok(false);
    };
    Ok(same(&row_to_swap_event(row)?, swap))
}

pub(crate) fn intent_id(signature: &str) -> String {
    format!("source-sell:{signature}")
}
pub(crate) fn signal_id(swap: &SwapEvent) -> String {
    format!(
        "shadow:{}:{}:sell:{}",
        swap.signature, swap.wallet, swap.token_in
    )
}
