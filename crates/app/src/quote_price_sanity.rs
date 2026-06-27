pub(crate) const QUOTE_RATIO_MIN: f64 = 0.1;
pub(crate) const QUOTE_RATIO_MAX: f64 = 10.0;
pub(crate) const QUOTE_QTY_RELATIVE_TOLERANCE: f64 = 0.05;

pub(crate) fn quote_ratio(quote_price: Option<f64>, shadow_price: Option<f64>) -> Option<f64> {
    let (Some(quote_price), Some(shadow_price)) = (quote_price, shadow_price) else {
        return None;
    };
    if !quote_price.is_finite() || !shadow_price.is_finite() || shadow_price <= 0.0 {
        return None;
    }
    Some(quote_price / shadow_price)
}

pub(crate) fn quote_ratio_is_sane(ratio: f64) -> bool {
    ratio.is_finite() && (QUOTE_RATIO_MIN..=QUOTE_RATIO_MAX).contains(&ratio)
}

pub(crate) fn quote_value_to_cost_ratio(quote_out_sol: f64, cost_sol: f64) -> Option<f64> {
    if !quote_out_sol.is_finite() || !cost_sol.is_finite() || cost_sol <= 0.0 {
        return None;
    }
    Some(quote_out_sol / cost_sol)
}

pub(crate) fn raw_amount_matches_ui_qty(raw: &str, decimals: u8, ui_qty: f64) -> Option<bool> {
    if !ui_qty.is_finite() || ui_qty <= 0.0 {
        return None;
    }
    let raw_amount = raw.trim().parse::<u128>().ok()?;
    if raw_amount == 0 {
        return None;
    }
    let raw_ui = raw_amount as f64 / 10f64.powi(i32::from(decimals));
    if !raw_ui.is_finite() || raw_ui <= 0.0 {
        return None;
    }
    let abs_tolerance = (2.0 / 10f64.powi(i32::from(decimals))).max(1e-12);
    let diff = (raw_ui - ui_qty).abs();
    Some(diff <= abs_tolerance || diff / raw_ui.max(ui_qty) <= QUOTE_QTY_RELATIVE_TOLERANCE)
}

pub(crate) fn raw_amount_mismatch_error(
    raw: &str,
    decimals: Option<u8>,
    ui_qty: f64,
    context: &str,
) -> Option<String> {
    let decimals = decimals?;
    if raw_amount_matches_ui_qty(raw, decimals, ui_qty) == Some(true) {
        return None;
    }
    Some(format!(
        "{context} qty_raw/decimals mismatch raw={raw} decimals={decimals} qty={ui_qty:.12}"
    ))
}
