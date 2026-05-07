use anyhow::{anyhow, Context, Result};
use copybot_core_types::{Lamports, SignedLamports, TokenQuantity};

const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;

pub(crate) fn u64_to_sql_i64(field: &str, value: u64) -> Result<i64> {
    i64::try_from(value).with_context(|| format!("{field}={value} exceeds sqlite INTEGER max"))
}

pub fn lamports_to_sol(lamports: Lamports) -> f64 {
    lamports.as_u64() as f64 / LAMPORTS_PER_SOL
}

pub fn signed_lamports_to_sol(lamports: SignedLamports) -> f64 {
    lamports.as_i128() as f64 / LAMPORTS_PER_SOL
}

pub(crate) fn sol_to_lamports_ceil(sol: f64, label: &str) -> Result<Lamports> {
    if !sol.is_finite() || sol < 0.0 {
        return Err(anyhow!("invalid {label}={sol} (must be finite and >= 0)"));
    }
    let scaled = sol * LAMPORTS_PER_SOL;
    if !scaled.is_finite() || scaled > u64::MAX as f64 {
        return Err(anyhow!(
            "invalid {label}={sol} (exceeds representable lamports)"
        ));
    }
    Ok(Lamports::new(scaled.ceil() as u64))
}

pub(crate) fn sol_to_lamports_floor(sol: f64, label: &str) -> Result<Lamports> {
    if !sol.is_finite() || sol < 0.0 {
        return Err(anyhow!("invalid {label}={sol} (must be finite and >= 0)"));
    }
    let scaled = sol * LAMPORTS_PER_SOL;
    if !scaled.is_finite() || scaled > u64::MAX as f64 {
        return Err(anyhow!(
            "invalid {label}={sol} (exceeds representable lamports)"
        ));
    }
    Ok(Lamports::new(scaled.floor() as u64))
}

fn sol_to_signed_lamports_conservative(sol: f64, label: &str) -> Result<SignedLamports> {
    if !sol.is_finite() {
        return Err(anyhow!("invalid {label}={sol} (must be finite)"));
    }
    let magnitude = sol.abs() * LAMPORTS_PER_SOL;
    if !magnitude.is_finite() || magnitude > i64::MAX as f64 {
        return Err(anyhow!(
            "invalid {label}={sol} (exceeds representable signed lamports)"
        ));
    }
    let signed = if sol >= 0.0 {
        magnitude.floor() as i128
    } else {
        -(magnitude.ceil() as i128)
    };
    Ok(SignedLamports::new(signed))
}

pub(crate) fn token_quantity_from_sql(
    raw: Option<String>,
    decimals: Option<i64>,
    context: &str,
) -> Result<Option<TokenQuantity>> {
    match (raw, decimals) {
        (None, None) => Ok(None),
        (Some(raw), Some(decimals)) => {
            let decimals = u8::try_from(decimals).with_context(|| {
                format!("invalid qty_decimals={decimals} in {context} (must fit into u8)")
            })?;
            let raw_value = raw.parse::<u64>().with_context(|| {
                format!("invalid qty_raw={raw:?} in {context} (must parse as u64)")
            })?;
            Ok(Some(TokenQuantity::new(raw_value, decimals)))
        }
        _ => Err(anyhow!(
            "partial exact quantity sidecar in {context} (qty_raw and qty_decimals must both be NULL or both be populated)"
        )),
    }
}

pub(crate) fn split_token_quantity_pro_rata(
    total: TokenQuantity,
    consumed_qty: f64,
    remaining_qty: f64,
    final_segment: bool,
    context: &str,
) -> Result<(Option<TokenQuantity>, Option<TokenQuantity>)> {
    if final_segment {
        return Ok((Some(total), None));
    }
    if !consumed_qty.is_finite()
        || !remaining_qty.is_finite()
        || consumed_qty < 0.0
        || remaining_qty <= 0.0
    {
        return Err(anyhow!(
            "invalid pro-rata {context} token split inputs consumed_qty={consumed_qty} remaining_qty={remaining_qty}"
        ));
    }
    let share = consumed_qty / remaining_qty;
    if !share.is_finite() || !(0.0..=1.0).contains(&share) {
        return Err(anyhow!(
            "invalid pro-rata {context} token split share consumed_qty={consumed_qty} remaining_qty={remaining_qty}"
        ));
    }
    let raw = ((total.raw() as f64) * share).floor() as u64;
    let raw = raw.min(total.raw());
    let segment = Some(TokenQuantity::new(raw, total.decimals()));
    let remainder_raw = total.raw().saturating_sub(raw);
    let remainder = if remainder_raw == 0 {
        None
    } else {
        Some(TokenQuantity::new(remainder_raw, total.decimals()))
    };
    Ok((segment, remainder))
}

pub(crate) fn merge_position_qty_exact_on_sell(
    current: Option<TokenQuantity>,
    closed: Option<TokenQuantity>,
    closing: bool,
) -> Result<Option<TokenQuantity>> {
    match (current, closed) {
        (Some(current), Some(closed)) if current.decimals() == closed.decimals() => {
            let Some(raw) = current.raw().checked_sub(closed.raw()) else {
                return Ok(None);
            };
            if closing && raw == 0 {
                Ok(Some(TokenQuantity::new(0, current.decimals())))
            } else if closing {
                Ok(None)
            } else {
                Ok(Some(TokenQuantity::new(raw, current.decimals())))
            }
        }
        (Some(_), Some(_)) | (Some(_), None) | (None, Some(_)) | (None, None) => Ok(None),
    }
}

pub(crate) fn shadow_lot_cost_lamports(
    cost_sol: f64,
    cost_lamports_raw: Option<i64>,
    context: &str,
) -> Result<Lamports> {
    if let Some(raw) = cost_lamports_raw {
        if raw < 0 {
            return Err(anyhow!(
                "invalid negative shadow_lots.cost_lamports={raw} in {context}"
            ));
        }
        return Ok(Lamports::new(raw as u64));
    }
    sol_to_lamports_ceil(cost_sol, "shadow_lots.cost_sol")
        .with_context(|| format!("failed deriving shadow lot cost_lamports in {context}"))
}

pub(crate) fn shadow_closed_trade_entry_cost_lamports(
    entry_cost_sol: f64,
    entry_cost_lamports_raw: Option<i64>,
    context: &str,
) -> Result<Lamports> {
    if let Some(raw) = entry_cost_lamports_raw {
        if raw < 0 {
            return Err(anyhow!(
                "invalid negative shadow_closed_trades.entry_cost_lamports={raw} in {context}"
            ));
        }
        return Ok(Lamports::new(raw as u64));
    }
    sol_to_lamports_ceil(entry_cost_sol, "shadow_closed_trades.entry_cost_sol").with_context(|| {
        format!("failed deriving shadow closed trade entry_cost_lamports in {context}")
    })
}

pub(crate) fn shadow_closed_trade_pnl_lamports(
    pnl_sol: f64,
    pnl_lamports_raw: Option<i64>,
    context: &str,
) -> Result<SignedLamports> {
    if let Some(raw) = pnl_lamports_raw {
        return Ok(SignedLamports::new(i128::from(raw)));
    }
    sol_to_signed_lamports_conservative(pnl_sol, "shadow_closed_trades.pnl_sol")
        .with_context(|| format!("failed deriving shadow closed trade pnl_lamports in {context}"))
}

pub(crate) fn signed_lamports_to_sql_i64(field: &str, value: SignedLamports) -> Result<i64> {
    i64::try_from(value.as_i128())
        .with_context(|| format!("{field}={} exceeds sqlite INTEGER range", value.as_i128()))
}
