use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::ExactSwapAmounts;
use std::collections::{HashMap, HashSet};
use yellowstone_grpc_proto::prelude::{
    subscribe_update, CommitmentLevel, CompiledInstruction, InnerInstruction,
    Message as SolMessage, SubscribeRequest, SubscribeRequestFilterTransactions, SubscribeUpdate,
    SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo, TransactionStatusMeta,
    UiTokenAmount,
};

use super::{
    normalize_program_ids_or_fallback, HeliusWsSource, RawSwapObservation, YellowstoneParsedUpdate,
    YellowstoneRuntimeConfig, SOL_MINT,
};

#[derive(Debug, Clone)]
pub(super) struct ParsedUiAmount {
    pub(super) amount: f64,
    pub(super) raw_amount: Option<String>,
    pub(super) decimals: Option<u8>,
}

#[derive(Debug, Clone, Default)]
struct MintDelta {
    amount_delta: f64,
    raw_delta: Option<i128>,
    decimals: Option<u8>,
}

impl MintDelta {
    fn apply_sub(&mut self, amount: &ParsedUiAmount) {
        self.amount_delta -= amount.amount;
        self.apply_raw_delta(amount, -1);
    }

    fn apply_add(&mut self, amount: &ParsedUiAmount) {
        self.amount_delta += amount.amount;
        self.apply_raw_delta(amount, 1);
    }

    fn apply_raw_delta(&mut self, amount: &ParsedUiAmount, sign: i8) {
        let Some(raw_amount) = amount.raw_amount.as_deref() else {
            self.raw_delta = None;
            self.decimals = None;
            return;
        };
        let Some(decimals) = amount.decimals else {
            self.raw_delta = None;
            self.decimals = None;
            return;
        };
        let Some(parsed_raw) = raw_amount.parse::<i128>().ok() else {
            self.raw_delta = None;
            self.decimals = None;
            return;
        };
        match self.decimals {
            Some(existing) if existing != decimals => {
                self.raw_delta = None;
                self.decimals = None;
            }
            Some(_) => {
                if let Some(current) = self.raw_delta {
                    self.raw_delta = current.checked_add(parsed_raw * i128::from(sign));
                }
            }
            None => {
                self.decimals = Some(decimals);
                self.raw_delta = Some(parsed_raw * i128::from(sign));
            }
        }
    }

    fn candidate(&self) -> ParsedUiAmount {
        ParsedUiAmount {
            amount: self.amount_delta.abs(),
            raw_amount: self.raw_delta.map(|value| value.abs().to_string()),
            decimals: self.decimals,
        }
    }
}

pub(super) fn build_yellowstone_subscribe_request(
    runtime_config: &YellowstoneRuntimeConfig,
) -> SubscribeRequest {
    let mut transactions = HashMap::new();
    transactions.insert(
        "copybot-swaps".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            signature: None,
            account_include: runtime_config
                .interested_program_ids
                .iter()
                .cloned()
                .collect(),
            account_exclude: Vec::new(),
            account_required: Vec::new(),
        },
    );

    SubscribeRequest {
        accounts: HashMap::new(),
        slots: HashMap::new(),
        transactions,
        transactions_status: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta: HashMap::new(),
        entry: HashMap::new(),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        accounts_data_slice: Vec::new(),
        ping: None,
        from_slot: None,
    }
}

pub(super) fn parse_yellowstone_update(
    update: SubscribeUpdate,
    runtime_config: &YellowstoneRuntimeConfig,
) -> Result<Option<YellowstoneParsedUpdate>> {
    let created_at = update.created_at.clone();
    let Some(update_oneof) = update.update_oneof else {
        return Ok(None);
    };
    match update_oneof {
        subscribe_update::UpdateOneof::Transaction(transaction_update) => {
            parse_yellowstone_transaction_update(transaction_update, created_at, runtime_config)
                .map(|raw| raw.map(YellowstoneParsedUpdate::Observation))
        }
        subscribe_update::UpdateOneof::Ping(_) => Ok(Some(YellowstoneParsedUpdate::Ping)),
        _ => Ok(None),
    }
}

fn parse_yellowstone_transaction_update(
    tx_update: SubscribeUpdateTransaction,
    created_at: Option<yellowstone_grpc_proto::prost_types::Timestamp>,
    runtime_config: &YellowstoneRuntimeConfig,
) -> Result<Option<RawSwapObservation>> {
    if tx_update.slot == 0 {
        return Err(anyhow!("missing slot in yellowstone update"));
    }
    let Some(tx_info) = tx_update.transaction else {
        return Err(anyhow!("missing status in yellowstone update"));
    };
    if tx_info.is_vote {
        return Ok(None);
    }

    let Some(meta) = tx_info.meta.as_ref() else {
        return Err(anyhow!("missing status in yellowstone update"));
    };
    if tx_meta_has_error(meta) {
        return Ok(None);
    }

    let Some(transaction) = tx_info.transaction.as_ref() else {
        return Err(anyhow!("missing signer in yellowstone update"));
    };
    let Some(message) = transaction.message.as_ref() else {
        return Err(anyhow!("missing signer in yellowstone update"));
    };

    let account_keys = proto_account_keys(message, meta);
    if account_keys.is_empty() {
        return Err(anyhow!("missing signer in yellowstone update"));
    }

    let signer_index = 0;
    let signer = account_keys.get(signer_index).cloned().unwrap_or_default();
    if signer.is_empty() {
        return Err(anyhow!("missing signer in yellowstone update"));
    }

    let program_ids = match normalize_program_ids_or_fallback(
        extract_program_ids_from_proto(message, meta, &account_keys),
        &runtime_config.interested_program_ids,
        runtime_config.telemetry.as_ref(),
        "missing program ids in yellowstone update",
    )? {
        Some(value) => value,
        None => return Ok(None),
    };

    let (token_in, amount_in, token_out, amount_out) =
        match infer_swap_from_proto_balances(meta, signer_index, &signer) {
            Some(value) => value,
            None => return Ok(None),
        };
    if !amount_in.amount.is_finite()
        || !amount_out.amount.is_finite()
        || amount_in.amount <= 0.0
        || amount_out.amount <= 0.0
    {
        return Ok(None);
    }

    let signature = decode_signature_from_proto(&tx_info)
        .ok_or_else(|| anyhow!("missing transaction signature in yellowstone update"))?;
    let logs = meta.log_messages.clone();
    let dex_hint = HeliusWsSource::detect_dex_hint(
        &program_ids,
        &logs,
        &runtime_config.raydium_program_ids,
        &runtime_config.pumpswap_program_ids,
    );

    let ts_utc = created_at
        .as_ref()
        .and_then(|timestamp| {
            if timestamp.nanos < 0 || timestamp.nanos >= 1_000_000_000 {
                return None;
            }
            DateTime::<Utc>::from_timestamp(timestamp.seconds, timestamp.nanos as u32)
        })
        .unwrap_or_else(Utc::now);

    Ok(Some(RawSwapObservation {
        signature,
        slot: tx_update.slot,
        signer,
        token_in,
        token_out,
        amount_in: amount_in.amount,
        amount_out: amount_out.amount,
        exact_amounts: build_exact_swap_amounts(&amount_in, &amount_out),
        program_ids: program_ids.into_iter().collect(),
        dex_hint,
        ts_utc,
    }))
}

fn tx_meta_has_error(meta: &TransactionStatusMeta) -> bool {
    meta.err.as_ref().is_some_and(|err| !err.err.is_empty())
}

fn decode_signature_from_proto(tx_info: &SubscribeUpdateTransactionInfo) -> Option<String> {
    if !tx_info.signature.is_empty() {
        return Some(bs58::encode(&tx_info.signature).into_string());
    }
    tx_info
        .transaction
        .as_ref()
        .and_then(|tx| tx.signatures.first())
        .map(|sig| bs58::encode(sig).into_string())
}

fn proto_account_keys(message: &SolMessage, meta: &TransactionStatusMeta) -> Vec<String> {
    let mut out = message
        .account_keys
        .iter()
        .map(|raw| bs58::encode(raw).into_string())
        .collect::<Vec<_>>();
    out.extend(
        meta.loaded_writable_addresses
            .iter()
            .map(|raw| bs58::encode(raw).into_string()),
    );
    out.extend(
        meta.loaded_readonly_addresses
            .iter()
            .map(|raw| bs58::encode(raw).into_string()),
    );
    out
}

fn extract_program_ids_from_proto(
    message: &SolMessage,
    meta: &TransactionStatusMeta,
    account_keys: &[String],
) -> HashSet<String> {
    let mut set = HashSet::new();
    for instruction in &message.instructions {
        if let Some(program) =
            decode_program_id_from_compiled_instruction(instruction, account_keys)
        {
            set.insert(program);
        }
    }
    for group in &meta.inner_instructions {
        for instruction in &group.instructions {
            if let Some(program) =
                decode_program_id_from_inner_instruction(instruction, account_keys)
            {
                set.insert(program);
            }
        }
    }
    for log in &meta.log_messages {
        if let Some(program_id) = HeliusWsSource::extract_program_id_from_log(log) {
            set.insert(program_id);
        }
    }
    set
}

fn decode_program_id_from_compiled_instruction(
    instruction: &CompiledInstruction,
    account_keys: &[String],
) -> Option<String> {
    account_keys
        .get(instruction.program_id_index as usize)
        .cloned()
}

fn decode_program_id_from_inner_instruction(
    instruction: &InnerInstruction,
    account_keys: &[String],
) -> Option<String> {
    account_keys
        .get(instruction.program_id_index as usize)
        .cloned()
}

pub(super) fn infer_swap_from_proto_balances(
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
    token_in_candidates
        .sort_by(|a, b| b.1.amount.partial_cmp(&a.1.amount).unwrap_or(std::cmp::Ordering::Equal));
    token_out_candidates
        .sort_by(|a, b| b.1.amount.partial_cmp(&a.1.amount).unwrap_or(std::cmp::Ordering::Equal));

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

fn parse_proto_ui_amount(ui_amount: Option<&UiTokenAmount>) -> Option<ParsedUiAmount> {
    let ui_amount = ui_amount?;
    if !ui_amount.ui_amount_string.is_empty() {
        let parsed = ui_amount.ui_amount_string.parse::<f64>().ok()?;
        return parsed.is_finite().then_some(ParsedUiAmount {
            amount: parsed,
            raw_amount: (!ui_amount.amount.is_empty()).then(|| ui_amount.amount.clone()),
            decimals: Some(ui_amount.decimals as u8),
        });
    }
    if !ui_amount.amount.is_empty() {
        let raw = ui_amount.amount.parse::<f64>().ok()?;
        let normalized = raw / 10f64.powi(ui_amount.decimals as i32);
        return normalized.is_finite().then_some(ParsedUiAmount {
            amount: normalized,
            raw_amount: Some(ui_amount.amount.clone()),
            decimals: Some(ui_amount.decimals as u8),
        });
    }
    if ui_amount.ui_amount.is_finite() {
        return Some(ParsedUiAmount {
            amount: ui_amount.ui_amount,
            raw_amount: None,
            decimals: Some(ui_amount.decimals as u8),
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
        raw_amount: Some(delta.abs().to_string()),
        decimals: Some(9),
    })
}

fn build_exact_swap_amounts(
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

#[cfg(test)]
mod tests {
    use super::parse_proto_ui_amount;
    use yellowstone_grpc_proto::prelude::UiTokenAmount;

    #[test]
    fn parse_proto_ui_amount_rejects_non_finite_strings() {
        let amount = UiTokenAmount {
            ui_amount: 0.0,
            decimals: 6,
            amount: String::new(),
            ui_amount_string: "NaN".to_string(),
        };

        assert!(
            parse_proto_ui_amount(Some(&amount)).is_none(),
            "NaN ui_amount_string must be rejected"
        );
    }

    #[test]
    fn parse_proto_ui_amount_rejects_non_finite_raw_amounts() {
        let amount = UiTokenAmount {
            ui_amount: 0.0,
            decimals: 6,
            amount: "NaN".to_string(),
            ui_amount_string: String::new(),
        };

        assert!(
            parse_proto_ui_amount(Some(&amount)).is_none(),
            "NaN raw amount must be rejected"
        );
    }

    #[test]
    fn parse_proto_ui_amount_rejects_non_finite_ui_amount_field() {
        let amount = UiTokenAmount {
            ui_amount: f64::INFINITY,
            decimals: 6,
            amount: String::new(),
            ui_amount_string: String::new(),
        };

        assert!(
            parse_proto_ui_amount(Some(&amount)).is_none(),
            "non-finite ui_amount field must be rejected"
        );
    }
}
