use serde_json::Value;
use std::collections::HashMap;

use copybot_core_types::ExactSwapAmounts;

use super::super::{HeliusWsSource, SOL_MINT};
use super::amounts::{MintDelta, ParsedUiAmount};

impl HeliusWsSource {
    pub(in crate::source) fn infer_swap_from_json_balances(
        meta: &Value,
        signer_index: usize,
        signer: &str,
    ) -> Option<(String, ParsedUiAmount, String, ParsedUiAmount)> {
        Self::infer_swap_from_json_balances_with_attribution(meta, signer_index, signer, || {
            crate::source::native_attribution::Attribution::NotApplicable
        })
    }

    pub(in crate::source) fn infer_swap_from_json_balances_with_attribution(
        meta: &Value,
        signer_index: usize,
        signer: &str,
        attribution: impl FnOnce() -> crate::source::native_attribution::Attribution,
    ) -> Option<(String, ParsedUiAmount, String, ParsedUiAmount)> {
        crate::source::target_balance_rows::json(meta, signer)?;
        const TOKEN_EPS: f64 = 1e-12;
        const SOL_EPS: f64 = 1e-8;
        let mut mint_deltas: HashMap<String, MintDelta> = HashMap::new();

        let pre = meta
            .get("preTokenBalances")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let post = meta
            .get("postTokenBalances")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        for item in pre {
            if item.get("owner").and_then(Value::as_str) == Some(signer) {
                let mint = item.get("mint").and_then(Value::as_str)?.to_string();
                let amount = Self::parse_ui_amount_json(item.get("uiTokenAmount"))?;
                mint_deltas.entry(mint).or_default().apply_sub(&amount);
            }
        }
        for item in post {
            if item.get("owner").and_then(Value::as_str) == Some(signer) {
                let mint = item.get("mint").and_then(Value::as_str)?.to_string();
                let amount = Self::parse_ui_amount_json(item.get("uiTokenAmount"))?;
                mint_deltas.entry(mint).or_default().apply_add(&amount);
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
            if let Some((out_mint, out_amt)) = Self::single_non_sol_leg(&token_out_candidates) {
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
            if let Some((in_mint, in_amt)) = Self::single_non_sol_leg(&token_in_candidates) {
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

        let sol_delta = Self::signer_sol_delta(meta, signer_index);
        let sol_amount = sol_delta.as_ref().map(|value| value.amount).unwrap_or(0.0);
        let sol_exact = sol_delta.and_then(|value| {
            Some(ParsedUiAmount {
                amount: value.amount.abs(),
                raw_amount: value.raw_amount,
                decimals: value.decimals,
            })
        });
        if sol_amount < -SOL_EPS {
            if let Some((out_mint, out_amt)) = Self::single_non_sol_leg(&token_out_candidates) {
                return Some((SOL_MINT.to_string(), sol_exact.clone()?, out_mint, out_amt));
            }
        }
        if sol_amount > SOL_EPS {
            if let Some((in_mint, in_amt)) = Self::single_non_sol_leg(&token_in_candidates) {
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

    pub(in crate::source) fn signer_sol_delta(
        meta: &Value,
        signer_index: usize,
    ) -> Option<ParsedUiAmount> {
        let pre_sol = meta
            .get("preBalances")
            .and_then(Value::as_array)
            .and_then(|balances| balances.get(signer_index))
            .and_then(Value::as_u64)?;
        let post_sol = meta
            .get("postBalances")
            .and_then(Value::as_array)
            .and_then(|balances| balances.get(signer_index))
            .and_then(Value::as_u64)?;
        let delta = post_sol as i128 - pre_sol as i128;
        Some(ParsedUiAmount {
            amount: delta as f64 / 1_000_000_000.0,
            raw_amount: None,
            decimals: None,
        })
    }

    pub(in crate::source) fn single_non_sol_leg(
        entries: &[(String, ParsedUiAmount)],
    ) -> Option<(String, ParsedUiAmount)> {
        const EPS: f64 = 1e-12;
        let mut non_sol = entries
            .iter()
            .filter(|(mint, value)| mint != SOL_MINT && value.amount > EPS);
        let single = non_sol.next()?;
        non_sol.next().is_none().then(|| single.clone())
    }

    pub(in crate::source) fn parse_ui_amount_json(
        ui_amount: Option<&Value>,
    ) -> Option<ParsedUiAmount> {
        let ui_amount = ui_amount?;
        if let Some(amount) = ui_amount.get("uiAmountString").and_then(Value::as_str) {
            let parsed = amount.parse::<f64>().ok()?;
            return parsed.is_finite().then(|| ParsedUiAmount {
                amount: parsed,
                raw_amount: ui_amount
                    .get("amount")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
                decimals: ui_amount
                    .get("decimals")
                    .and_then(Value::as_u64)
                    .and_then(|value| u8::try_from(value).ok()),
            });
        }
        if let Some(amount) = ui_amount.get("uiAmount").and_then(Value::as_f64) {
            return amount.is_finite().then_some(ParsedUiAmount {
                amount,
                raw_amount: ui_amount
                    .get("amount")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
                decimals: ui_amount
                    .get("decimals")
                    .and_then(Value::as_u64)
                    .and_then(|value| u8::try_from(value).ok()),
            });
        }
        let raw = ui_amount.get("amount").and_then(Value::as_str)?;
        let decimals = ui_amount.get("decimals").and_then(Value::as_u64)?;
        if decimals > 18 {
            return None;
        }
        let parsed_raw = raw.parse::<f64>().ok()?;
        let amount = parsed_raw / 10f64.powi(decimals as i32);
        amount.is_finite().then(|| ParsedUiAmount {
            amount,
            raw_amount: Some(raw.to_string()),
            decimals: u8::try_from(decimals).ok(),
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
}
