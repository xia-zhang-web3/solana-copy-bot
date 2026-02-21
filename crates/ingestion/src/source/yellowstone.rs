use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
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
    if amount_in <= 0.0 || amount_out <= 0.0 {
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
        amount_in,
        amount_out,
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
) -> Option<(String, f64, String, f64)> {
    const TOKEN_EPS: f64 = 1e-12;
    const SOL_EPS: f64 = 1e-8;
    let mut mint_deltas: HashMap<String, f64> = HashMap::new();

    for item in &meta.pre_token_balances {
        if item.owner == signer {
            let Some(amount) = parse_proto_ui_amount(item.ui_token_amount.as_ref()) else {
                continue;
            };
            *mint_deltas.entry(item.mint.clone()).or_default() -= amount;
        }
    }
    for item in &meta.post_token_balances {
        if item.owner == signer {
            let Some(amount) = parse_proto_ui_amount(item.ui_token_amount.as_ref()) else {
                continue;
            };
            *mint_deltas.entry(item.mint.clone()).or_default() += amount;
        }
    }

    let mut token_in_candidates = Vec::new();
    let mut token_out_candidates = Vec::new();
    for (mint, delta) in &mint_deltas {
        if *delta < -TOKEN_EPS {
            token_in_candidates.push((mint.clone(), delta.abs()));
        } else if *delta > TOKEN_EPS {
            token_out_candidates.push((mint.clone(), *delta));
        }
    }
    token_in_candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    token_out_candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    let sol_token_delta = mint_deltas.get(SOL_MINT).copied().unwrap_or(0.0);
    if sol_token_delta < -TOKEN_EPS {
        if let Some((out_mint, out_amt)) =
            HeliusWsSource::dominant_non_sol_leg(&token_out_candidates)
        {
            return Some((
                SOL_MINT.to_string(),
                sol_token_delta.abs(),
                out_mint,
                out_amt,
            ));
        }
    }
    if sol_token_delta > TOKEN_EPS {
        if let Some((in_mint, in_amt)) = HeliusWsSource::dominant_non_sol_leg(&token_in_candidates)
        {
            return Some((in_mint, in_amt, SOL_MINT.to_string(), sol_token_delta));
        }
    }

    let sol_delta = signer_sol_delta_from_proto(meta, signer_index).unwrap_or(0.0);
    if sol_delta < -SOL_EPS {
        if let Some((out_mint, out_amt)) =
            HeliusWsSource::dominant_non_sol_leg(&token_out_candidates)
        {
            return Some((SOL_MINT.to_string(), sol_delta.abs(), out_mint, out_amt));
        }
    }
    if sol_delta > SOL_EPS {
        if let Some((in_mint, in_amt)) = HeliusWsSource::dominant_non_sol_leg(&token_in_candidates)
        {
            return Some((in_mint, in_amt, SOL_MINT.to_string(), sol_delta));
        }
    }

    if sol_delta.abs() <= SOL_EPS && sol_token_delta.abs() <= TOKEN_EPS {
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

fn parse_proto_ui_amount(ui_amount: Option<&UiTokenAmount>) -> Option<f64> {
    let ui_amount = ui_amount?;
    if !ui_amount.ui_amount_string.is_empty() {
        return ui_amount.ui_amount_string.parse::<f64>().ok();
    }
    if !ui_amount.amount.is_empty() {
        let raw = ui_amount.amount.parse::<f64>().ok()?;
        return Some(raw / 10f64.powi(ui_amount.decimals as i32));
    }
    if ui_amount.ui_amount.is_finite() {
        return Some(ui_amount.ui_amount);
    }
    None
}

fn signer_sol_delta_from_proto(meta: &TransactionStatusMeta, signer_index: usize) -> Option<f64> {
    let pre_sol = *meta.pre_balances.get(signer_index)? as f64 / 1_000_000_000.0;
    let post_sol = *meta.post_balances.get(signer_index)? as f64 / 1_000_000_000.0;
    Some(post_sol - pre_sol)
}
