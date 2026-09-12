use anyhow::{anyhow, Result};
use copybot_core_types::ExactSwapAmounts;
use std::collections::HashSet;
use yellowstone_grpc_proto::prelude::SubscribeUpdateTransaction;

use super::yellowstone_proto::{
    build_exact_swap_amounts, decode_signature_from_proto, extract_program_ids_from_proto,
    infer_swap_from_proto_balances_with_attribution, proto_account_keys, tx_meta_has_error,
};
use super::HeliusWsSource;

/// Checked swap operands only. No containing block/fork binding or event time
/// is proven by this type; token_in/token_out retain the decoded direction.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct YellowstoneSwapFacts {
    pub(super) signature: String,
    pub(super) slot: u64,
    pub(super) signer: String,
    pub(super) token_in: String,
    pub(super) token_out: String,
    pub(super) amount_in: f64,
    pub(super) amount_out: f64,
    pub(super) exact_amounts: Option<ExactSwapAmounts>,
    pub(super) program_ids: Vec<String>,
    pub(super) dex_hint: String,
}

pub(super) struct DecodedYellowstoneSwap {
    pub(super) facts: Result<Option<YellowstoneSwapFacts>>,
    pub(super) used_program_fallback: bool,
}

/// Pure extraction: no clock, I/O, telemetry mutation or message timestamp.
/// The caller applies the returned fallback effect, including on refusal/error.
pub(super) fn decode_yellowstone_swap_facts(
    tx_update: &SubscribeUpdateTransaction,
    interested_program_ids: &HashSet<String>,
    raydium_program_ids: &HashSet<String>,
    pumpswap_program_ids: &HashSet<String>,
) -> DecodedYellowstoneSwap {
    let mut used_program_fallback = false;
    let facts = decode(
        tx_update,
        interested_program_ids,
        raydium_program_ids,
        pumpswap_program_ids,
        &mut used_program_fallback,
    );
    DecodedYellowstoneSwap {
        facts,
        used_program_fallback,
    }
}

fn decode(
    tx_update: &SubscribeUpdateTransaction,
    interested_program_ids: &HashSet<String>,
    raydium_program_ids: &HashSet<String>,
    pumpswap_program_ids: &HashSet<String>,
    used_program_fallback: &mut bool,
) -> Result<Option<YellowstoneSwapFacts>> {
    if tx_update.slot == 0 {
        return Err(anyhow!("missing slot in yellowstone update"));
    }
    let Some(tx_info) = tx_update.transaction.as_ref() else {
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

    let mut program_ids = extract_program_ids_from_proto(message, meta, &account_keys);
    // Preserve the existing program fallback policy, returning its telemetry
    // effect separately even when a later predicate refuses or errors.
    if program_ids.is_empty() {
        if interested_program_ids.is_empty() {
            return Err(anyhow!("missing program ids in yellowstone update"));
        }
        *used_program_fallback = true;
        program_ids.extend(interested_program_ids.iter().cloned());
    } else if !program_ids
        .iter()
        .any(|id| interested_program_ids.contains(id))
    {
        return Ok(None);
    }

    let (token_in, amount_in, token_out, amount_out) =
        match infer_swap_from_proto_balances_with_attribution(meta, signer_index, &signer, || {
            super::native_attribution::proto::infer(message, meta, &signer, pumpswap_program_ids)
        }) {
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

    let signature = decode_signature_from_proto(tx_info)
        .ok_or_else(|| anyhow!("missing transaction signature in yellowstone update"))?;
    let logs = meta.log_messages.clone();
    let dex_hint = HeliusWsSource::detect_dex_hint(
        &program_ids,
        &logs,
        raydium_program_ids,
        pumpswap_program_ids,
    );

    if super::pumpswap_instruction::requires_pumpswap_instruction(
        &program_ids,
        &dex_hint,
        pumpswap_program_ids,
    ) && !super::pumpswap_instruction::proto_has_supported_swap(
        message,
        meta,
        pumpswap_program_ids,
    ) {
        return Ok(None);
    }

    Ok(Some(YellowstoneSwapFacts {
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
    }))
}
