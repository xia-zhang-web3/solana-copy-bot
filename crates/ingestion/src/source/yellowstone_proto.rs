use copybot_core_types::ExactSwapAmounts;
use std::collections::{HashMap, HashSet};
use yellowstone_grpc_proto::prelude::{
    CompiledInstruction, InnerInstruction, Message as SolMessage, SubscribeUpdateTransactionInfo,
    TransactionStatusMeta, UiTokenAmount,
};

use super::{HeliusWsSource, SOL_MINT};

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
    exact_unavailable: bool,
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
        if self.exact_unavailable {
            return;
        }
        let Some(raw_amount) = amount.raw_amount.as_deref() else {
            self.invalidate_exact();
            return;
        };
        let Some(decimals) = amount.decimals else {
            self.invalidate_exact();
            return;
        };
        let Some(parsed_raw) = raw_amount.parse::<u64>().ok() else {
            self.invalidate_exact();
            return;
        };
        let parsed_raw = i128::from(parsed_raw);
        let Some(signed_raw) = parsed_raw.checked_mul(i128::from(sign)) else {
            self.invalidate_exact();
            return;
        };
        match self.decimals {
            Some(existing) if existing != decimals => {
                self.invalidate_exact();
            }
            Some(_) => {
                if let Some(current) = self.raw_delta {
                    match current.checked_add(signed_raw) {
                        Some(next) => self.raw_delta = Some(next),
                        None => self.invalidate_exact(),
                    }
                } else {
                    self.invalidate_exact();
                }
            }
            None => {
                self.decimals = Some(decimals);
                self.raw_delta = Some(signed_raw);
            }
        }
    }

    fn invalidate_exact(&mut self) {
        self.raw_delta = None;
        self.decimals = None;
        self.exact_unavailable = true;
    }

    fn candidate(&self) -> ParsedUiAmount {
        ParsedUiAmount {
            amount: self.amount_delta.abs(),
            raw_amount: self.raw_delta.map(|value| value.abs().to_string()),
            decimals: self.decimals,
        }
    }
}

pub(super) fn tx_meta_has_error(meta: &TransactionStatusMeta) -> bool {
    meta.err.as_ref().is_some_and(|err| !err.err.is_empty())
}

pub(super) fn decode_signature_from_proto(
    tx_info: &SubscribeUpdateTransactionInfo,
) -> Option<String> {
    if !tx_info.signature.is_empty() {
        return Some(bs58::encode(&tx_info.signature).into_string());
    }
    tx_info
        .transaction
        .as_ref()
        .and_then(|tx| tx.signatures.first())
        .map(|sig| bs58::encode(sig).into_string())
}

pub(super) fn proto_account_keys(
    message: &SolMessage,
    meta: &TransactionStatusMeta,
) -> Vec<String> {
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

pub(super) fn extract_program_ids_from_proto(
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

include!("yellowstone_proto_balances.rs");
