use anyhow::{Context, Result};

const SOL_LAMPORTS: f64 = 1_000_000_000.0;

pub(crate) struct QuotePnlAmounts<'a> {
    pub(crate) entry_in_raw: Option<&'a str>,
    pub(crate) entry_out_raw: Option<&'a str>,
    pub(crate) exit_in_raw: Option<&'a str>,
    pub(crate) exit_out_raw: Option<&'a str>,
    pub(crate) buy_priority_fee_lamports: Option<u64>,
    pub(crate) sell_priority_fee_lamports: Option<u64>,
}

pub(crate) struct ComputedPnl {
    pub(crate) entry_cost_sol: f64,
    pub(crate) exit_quote_sol: f64,
    pub(crate) closed_qty_ratio: f64,
    pub(crate) quote_adjusted_pnl_sol: f64,
    pub(crate) quote_adjusted_pnl_after_priority_fee_sol: f64,
    pub(crate) priority_fee_lamports_total: u64,
    pub(crate) scaled_exit_to_entry_qty: bool,
}

pub(crate) fn compute_quote_pnl(amounts: QuotePnlAmounts<'_>) -> Result<Option<ComputedPnl>> {
    let Some(entry_in_raw) = parse_raw_amount(amounts.entry_in_raw)? else {
        return Ok(None);
    };
    let Some(entry_out_raw) = parse_raw_amount(amounts.entry_out_raw)? else {
        return Ok(None);
    };
    let Some(exit_in_raw) = parse_raw_amount(amounts.exit_in_raw)? else {
        return Ok(None);
    };
    let Some(exit_out_raw) = parse_raw_amount(amounts.exit_out_raw)? else {
        return Ok(None);
    };
    if entry_in_raw == 0 || entry_out_raw == 0 || exit_in_raw == 0 {
        return Ok(None);
    }

    let exit_qty_for_pnl = exit_in_raw.min(entry_out_raw);
    let closed_qty_ratio = exit_qty_for_pnl as f64 / entry_out_raw as f64;
    let entry_cost_sol = (entry_in_raw as f64 / SOL_LAMPORTS) * closed_qty_ratio;
    let exit_quote_sol =
        (exit_out_raw as f64 / SOL_LAMPORTS) * (exit_qty_for_pnl as f64 / exit_in_raw as f64);
    let priority_fee_lamports_total = amounts.buy_priority_fee_lamports.unwrap_or(0)
        + amounts.sell_priority_fee_lamports.unwrap_or(0);
    let quote_adjusted_pnl_sol = exit_quote_sol - entry_cost_sol;
    let quote_adjusted_pnl_after_priority_fee_sol =
        quote_adjusted_pnl_sol - priority_fee_lamports_total as f64 / SOL_LAMPORTS;
    Ok(Some(ComputedPnl {
        entry_cost_sol,
        exit_quote_sol,
        closed_qty_ratio,
        quote_adjusted_pnl_sol,
        quote_adjusted_pnl_after_priority_fee_sol,
        priority_fee_lamports_total,
        scaled_exit_to_entry_qty: exit_in_raw > entry_out_raw,
    }))
}

fn parse_raw_amount(raw: Option<&str>) -> Result<Option<u128>> {
    raw.map(|value| {
        value
            .parse::<u128>()
            .with_context(|| format!("invalid quote raw amount: {value}"))
    })
    .transpose()
}
