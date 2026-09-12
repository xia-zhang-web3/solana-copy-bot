use copybot_core_types::ExactSwapAmounts;
use std::collections::HashMap;
use yellowstone_grpc_proto::prelude::{TransactionStatusMeta, UiTokenAmount};

use super::{MintDelta, ParsedUiAmount, SOL_MINT};

#[allow(dead_code)]
pub(in crate::source) fn infer_swap_from_proto_balances(
    meta: &TransactionStatusMeta,
    signer_index: usize,
    signer: &str,
) -> Option<(String, ParsedUiAmount, String, ParsedUiAmount)> {
    infer_swap_from_proto_balances_with_attribution(meta, signer_index, signer, || {
        crate::source::native_attribution::Attribution::NotApplicable
    })
}

pub(in crate::source) fn infer_swap_from_proto_balances_with_attribution(
    meta: &TransactionStatusMeta,
    signer_index: usize,
    signer: &str,
    attribution: impl FnOnce() -> crate::source::native_attribution::Attribution,
) -> Option<(String, ParsedUiAmount, String, ParsedUiAmount)> {
    crate::source::target_balance_rows::protobuf(meta, signer)?;
    const TOKEN_EPS: f64 = 1e-12;
    const SOL_EPS: f64 = 1e-8;
    let mut mint_deltas: HashMap<String, MintDelta> = HashMap::new();

    for item in &meta.pre_token_balances {
        if item.owner == signer {
            let amount = parse_proto_ui_amount(item.ui_token_amount.as_ref())?;
            mint_deltas
                .entry(item.mint.clone())
                .or_default()
                .apply_sub(&amount);
        }
    }
    for item in &meta.post_token_balances {
        if item.owner == signer {
            let amount = parse_proto_ui_amount(item.ui_token_amount.as_ref())?;
            mint_deltas
                .entry(item.mint.clone())
                .or_default()
                .apply_add(&amount);
        }
    }

    let mut token_in_candidates = Vec::new();
    let mut token_out_candidates = Vec::new();
    for (mint, delta) in &mint_deltas {
        if delta.amount_delta < -TOKEN_EPS {
            token_in_candidates.push((mint.clone(), delta.candidate()));
        } else if delta.amount_delta > TOKEN_EPS {
            token_out_candidates.push((mint.clone(), delta.candidate()));
        }
    }
    // UI quantities of different mints are incomparable. Reject ambiguity
    // before any WSOL/native fallback can select another pair.
    if token_in_candidates
        .iter()
        .filter(|(mint, _)| mint != SOL_MINT)
        .count()
        > 1
        || token_out_candidates
            .iter()
            .filter(|(mint, _)| mint != SOL_MINT)
            .count()
            > 1
    {
        return None;
    }

    use crate::source::native_attribution::Attribution;
    match attribution() {
        Attribution::Unknown => return None,
        Attribution::Known(trade) => {
            let (input, in_raw, in_dec, output, out_raw, out_dec) = trade.legs();
            let amount = |raw: u64, decimals: u8| ParsedUiAmount {
                amount: raw as f64 / 10f64.powi(i32::from(decimals)),
                raw_amount: Some(raw.to_string()),
                decimals: Some(decimals),
            };
            return Some((
                input,
                amount(in_raw, in_dec),
                output,
                amount(out_raw, out_dec),
            ));
        }
        Attribution::NotApplicable => {}
    }

    let sol_token_delta = mint_deltas
        .get(SOL_MINT)
        .map(|delta| delta.amount_delta)
        .unwrap_or(0.0);
    if sol_token_delta < -TOKEN_EPS {
        if let Some((out_mint, out_amt)) = single_non_sol_leg(&token_out_candidates) {
            return Some((
                SOL_MINT.to_string(),
                ParsedUiAmount {
                    amount: sol_token_delta.abs(),
                    raw_amount: mint_deltas
                        .get(SOL_MINT)
                        .and_then(|delta| delta.raw_delta.map(|value| value.abs().to_string())),
                    decimals: mint_deltas.get(SOL_MINT).and_then(|delta| delta.decimals),
                },
                out_mint,
                out_amt,
            ));
        }
    }
    if sol_token_delta > TOKEN_EPS {
        if let Some((in_mint, in_amt)) = single_non_sol_leg(&token_in_candidates) {
            return Some((
                in_mint,
                in_amt,
                SOL_MINT.to_string(),
                ParsedUiAmount {
                    amount: sol_token_delta,
                    raw_amount: mint_deltas
                        .get(SOL_MINT)
                        .and_then(|delta| delta.raw_delta.map(|value| value.abs().to_string())),
                    decimals: mint_deltas.get(SOL_MINT).and_then(|delta| delta.decimals),
                },
            ));
        }
    }

    let sol_delta = signer_sol_delta_from_proto(meta, signer_index);
    let sol_amount = sol_delta.as_ref().map(|value| value.amount).unwrap_or(0.0);
    let sol_exact = sol_delta.as_ref().map(|value| ParsedUiAmount {
        amount: value.amount.abs(),
        raw_amount: value.raw_amount.clone(),
        decimals: value.decimals,
    });
    if sol_amount < -SOL_EPS {
        if let Some((out_mint, out_amt)) = single_non_sol_leg(&token_out_candidates) {
            return Some((SOL_MINT.to_string(), sol_exact.clone()?, out_mint, out_amt));
        }
    }
    if sol_amount > SOL_EPS {
        if let Some((in_mint, in_amt)) = single_non_sol_leg(&token_in_candidates) {
            return Some((in_mint, in_amt, SOL_MINT.to_string(), sol_exact?));
        }
    }

    if sol_amount.abs() <= SOL_EPS && sol_token_delta.abs() <= TOKEN_EPS {
        let token_in_non_sol: Vec<_> = token_in_candidates
            .iter()
            .filter(|(mint, _)| mint != SOL_MINT)
            .cloned()
            .collect();
        let token_out_non_sol: Vec<_> = token_out_candidates
            .iter()
            .filter(|(mint, _)| mint != SOL_MINT)
            .cloned()
            .collect();
        if token_in_non_sol.len() == 1 && token_out_non_sol.len() == 1 {
            let (in_mint, in_amt) = token_in_non_sol[0].clone();
            let (out_mint, out_amt) = token_out_non_sol[0].clone();
            if in_mint != out_mint {
                return Some((in_mint, in_amt, out_mint, out_amt));
            }
        }
    }

    None
}

pub(in crate::source) fn parse_proto_ui_amount(
    ui_amount: Option<&UiTokenAmount>,
) -> Option<ParsedUiAmount> {
    let ui_amount = ui_amount?;
    let decimals = u8::try_from(ui_amount.decimals).ok()?;
    if !ui_amount.ui_amount_string.is_empty() {
        let parsed = ui_amount.ui_amount_string.parse::<f64>().ok()?;
        return parsed.is_finite().then_some(ParsedUiAmount {
            amount: parsed,
            raw_amount: (!ui_amount.amount.is_empty()).then(|| ui_amount.amount.clone()),
            decimals: Some(decimals),
        });
    }
    if !ui_amount.amount.is_empty() {
        let raw = ui_amount.amount.parse::<f64>().ok()?;
        let normalized = raw / 10f64.powi(ui_amount.decimals as i32);
        return normalized.is_finite().then_some(ParsedUiAmount {
            amount: normalized,
            raw_amount: Some(ui_amount.amount.clone()),
            decimals: Some(decimals),
        });
    }
    if ui_amount.ui_amount.is_finite() {
        return Some(ParsedUiAmount {
            amount: ui_amount.ui_amount,
            raw_amount: None,
            decimals: Some(decimals),
        });
    }
    None
}

fn signer_sol_delta_from_proto(
    meta: &TransactionStatusMeta,
    signer_index: usize,
) -> Option<ParsedUiAmount> {
    let pre_sol = *meta.pre_balances.get(signer_index)? as i128;
    let post_sol = *meta.post_balances.get(signer_index)? as i128;
    let delta = post_sol - pre_sol;
    Some(ParsedUiAmount {
        amount: delta as f64 / 1_000_000_000.0,
        raw_amount: None,
        decimals: None,
    })
}

pub(in crate::source) fn build_exact_swap_amounts(
    amount_in: &ParsedUiAmount,
    amount_out: &ParsedUiAmount,
) -> Option<ExactSwapAmounts> {
    Some(ExactSwapAmounts {
        amount_in_raw: amount_in.raw_amount.clone()?,
        amount_in_decimals: amount_in.decimals?,
        amount_out_raw: amount_out.raw_amount.clone()?,
        amount_out_decimals: amount_out.decimals?,
    })
}

fn single_non_sol_leg(entries: &[(String, ParsedUiAmount)]) -> Option<(String, ParsedUiAmount)> {
    const EPS: f64 = 1e-12;
    let mut non_sol = entries
        .iter()
        .filter(|(mint, value)| mint != SOL_MINT && value.amount > EPS);
    let single = non_sol.next()?;
    non_sol.next().is_none().then(|| single.clone())
}
