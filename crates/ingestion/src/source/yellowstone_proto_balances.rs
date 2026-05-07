use copybot_core_types::ExactSwapAmounts;
use std::collections::HashMap;
use yellowstone_grpc_proto::prelude::{TransactionStatusMeta, UiTokenAmount};

use super::{MintDelta, ParsedUiAmount, SOL_MINT};

pub(in crate::source) fn infer_swap_from_proto_balances(
    meta: &TransactionStatusMeta,
    signer_index: usize,
    signer: &str,
) -> Option<(String, ParsedUiAmount, String, ParsedUiAmount)> {
    const TOKEN_EPS: f64 = 1e-12;
    const SOL_EPS: f64 = 1e-8;
    let mut mint_deltas: HashMap<String, MintDelta> = HashMap::new();

    for item in &meta.pre_token_balances {
        if item.owner == signer {
            let Some(amount) = parse_proto_ui_amount(item.ui_token_amount.as_ref()) else {
                continue;
            };
            mint_deltas
                .entry(item.mint.clone())
                .or_default()
                .apply_sub(&amount);
        }
    }
    for item in &meta.post_token_balances {
        if item.owner == signer {
            let Some(amount) = parse_proto_ui_amount(item.ui_token_amount.as_ref()) else {
                continue;
            };
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
    token_in_candidates.sort_by(|a, b| {
        b.1.amount
            .partial_cmp(&a.1.amount)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    token_out_candidates.sort_by(|a, b| {
        b.1.amount
            .partial_cmp(&a.1.amount)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let sol_token_delta = mint_deltas
        .get(SOL_MINT)
        .map(|delta| delta.amount_delta)
        .unwrap_or(0.0);
    if sol_token_delta < -TOKEN_EPS {
        if let Some((out_mint, out_amt)) = dominant_non_sol_leg(&token_out_candidates) {
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
        if let Some((in_mint, in_amt)) = dominant_non_sol_leg(&token_in_candidates) {
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
        if let Some((out_mint, out_amt)) = dominant_non_sol_leg(&token_out_candidates) {
            return Some((SOL_MINT.to_string(), sol_exact.clone()?, out_mint, out_amt));
        }
    }
    if sol_amount > SOL_EPS {
        if let Some((in_mint, in_amt)) = dominant_non_sol_leg(&token_in_candidates) {
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

fn dominant_non_sol_leg(entries: &[(String, ParsedUiAmount)]) -> Option<(String, ParsedUiAmount)> {
    const EPS: f64 = 1e-12;
    const SECOND_LEG_AMBIGUITY_RATIO: f64 = 0.15;
    let non_sol: Vec<(String, ParsedUiAmount)> = entries
        .iter()
        .filter(|(mint, value)| mint != SOL_MINT && value.amount > EPS)
        .cloned()
        .collect();
    let (primary_mint, primary_value) = non_sol.first()?.clone();
    if non_sol.len() >= 2 {
        let second_value = non_sol[1].1.amount;
        if second_value > primary_value.amount * SECOND_LEG_AMBIGUITY_RATIO {
            return None;
        }
    }
    Some((primary_mint, primary_value))
}
