//! Association asserted by one supplied provider message, never canonicality,
//! finality or cryptographic inclusion. No subscriptions or runtime consumers.
use chrono::{DateTime, Utc};
use prost::Message;
use std::collections::HashSet;
use yellowstone_grpc_proto::prelude::{
    SubscribeUpdateBlock, SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
};

use super::yellowstone_facts::{decode_yellowstone_swap_facts, YellowstoneSwapFacts};

#[path = "yellowstone_block_bounds.rs"]
pub(super) mod bounds;
#[cfg(test)]
pub(super) use bounds::{MAX_INFO_BYTES, MAX_INFO_ITEMS};

/// Explicit local work/coverage limit, not a Solana protocol maximum.
pub(super) const MAX_BLOCK_TRANSACTIONS: usize = 4096;

/// Only the provider's assertion in this message. Two hashes at the same slot
/// remain distinct assertions; this API supplies no fork choice or dedup policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ProviderContainingBlock {
    pub(super) slot: u64,
    pub(super) blockhash: String,
    pub(super) signature: [u8; 64],
    pub(super) transaction_index: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProviderBlockTime {
    AvailableBlockTime(DateTime<Utc>),
    Missing,
    OutOfRange(i64),
}

#[derive(Debug)]
pub(super) struct AssociatedSwap {
    pub(super) facts: YellowstoneSwapFacts,
    pub(super) provider_assertion: ProviderContainingBlock,
    pub(super) block_time: ProviderBlockTime,
    pub(super) used_program_fallback: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum InfoSide {
    Expected,
    Selected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum AssociationRefusal {
    MissingExpectedInfo,
    InvalidSignatureLength(usize),
    ZeroSlot,
    SlotMismatch,
    InvalidBlockhash,
    TooManyTransactions(usize),
    InfoCardinalityExceeded(InfoSide),
    InfoTooLarge(InfoSide),
    /// Not found in this potentially filtered message, not in the entire block.
    NotFoundInMessage,
    DuplicateSignature,
    InfoMismatch,
    NoCheckedSwapFacts,
    FactsError(String),
}

/// Borrows already decoded protobufs. Requires exactly one matching signature
/// and full equality of all known Info fields, then reuses decoder79 unchanged.
/// Block time is separate: Missing/OutOfRange still preserves checked facts,
/// and even AvailableBlockTime makes no chain/event-time claim.
///
/// Limits apply after transport/deserialization allocations. Only signatures of
/// other transactions are scanned; block accounts/rewards/entries are untouched.
/// The bounded re-encodings plus explicit float-bit comparison cover every field;
/// they do not recover unknown fields discarded during protobuf decoding.
pub(super) fn associate_yellowstone_transaction(
    expected: &SubscribeUpdateTransaction,
    block: &SubscribeUpdateBlock,
    interested_program_ids: &HashSet<String>,
    raydium_program_ids: &HashSet<String>,
    pumpswap_program_ids: &HashSet<String>,
) -> Result<AssociatedSwap, AssociationRefusal> {
    use AssociationRefusal as Refusal;
    // Cheap cardinality/identity checks precede scanning, encoding or decoding.
    if block.transactions.len() > MAX_BLOCK_TRANSACTIONS {
        return Err(Refusal::TooManyTransactions(block.transactions.len()));
    }
    let info = expected
        .transaction
        .as_ref()
        .ok_or(Refusal::MissingExpectedInfo)?;
    let signature: [u8; 64] = info
        .signature
        .as_slice()
        .try_into()
        .map_err(|_| Refusal::InvalidSignatureLength(info.signature.len()))?;
    if expected.slot == 0 || block.slot == 0 {
        return Err(Refusal::ZeroSlot);
    }
    if expected.slot != block.slot {
        return Err(Refusal::SlotMismatch);
    }
    if !valid_blockhash(&block.blockhash) {
        return Err(Refusal::InvalidBlockhash);
    }
    bounds::check_info(info, InfoSide::Expected)?;
    let mut selected = None;
    for candidate in &block.transactions {
        if candidate.signature == info.signature {
            if selected.is_some() {
                return Err(Refusal::DuplicateSignature);
            }
            selected = Some(candidate);
        }
    }
    let selected = selected.ok_or(Refusal::NotFoundInMessage)?;
    bounds::check_info(selected, InfoSide::Selected)?;
    // Re-encoding uses at most 2 * MAX_INFO_BYTES; no block clone or reconstruction.
    if info.encode_to_vec() != selected.encode_to_vec() || !same_float_bits(info, selected) {
        return Err(Refusal::InfoMismatch);
    }
    let decoded = decode_yellowstone_swap_facts(
        expected,
        interested_program_ids,
        raydium_program_ids,
        pumpswap_program_ids,
    );
    let facts = decoded
        .facts
        .map_err(|error| Refusal::FactsError(format!("{error:#}")))?
        .ok_or(Refusal::NoCheckedSwapFacts)?;
    let block_time = match block.block_time.as_ref() {
        None => ProviderBlockTime::Missing,
        Some(time) => match DateTime::<Utc>::from_timestamp(time.timestamp, 0) {
            Some(time) => ProviderBlockTime::AvailableBlockTime(time),
            None => ProviderBlockTime::OutOfRange(time.timestamp),
        },
    };
    Ok(AssociatedSwap {
        facts,
        provider_assertion: ProviderContainingBlock {
            slot: expected.slot,
            blockhash: block.blockhash.clone(),
            signature,
            transaction_index: info.index,
        },
        block_time,
        used_program_fallback: decoded.used_program_fallback,
    })
}

pub(super) fn valid_blockhash(hash: &str) -> bool {
    if !(32..=44).contains(&hash.len()) {
        return false;
    }
    let mut bytes = [0u8; 32];
    matches!(bs58::decode(hash).onto(&mut bytes), Ok(32))
        && bs58::encode(bytes).into_string() == hash
}

// Prost omits both +0.0 and -0.0 when encoding a default double. Compare the
// bits of every double in pinned proto12 as well, including NaN payloads.
pub(super) fn same_float_bits(
    a: &SubscribeUpdateTransactionInfo,
    b: &SubscribeUpdateTransactionInfo,
) -> bool {
    let bits = |info: &SubscribeUpdateTransactionInfo| {
        info.meta
            .iter()
            .flat_map(|meta| {
                meta.pre_token_balances
                    .iter()
                    .chain(&meta.post_token_balances)
            })
            .map(|row| {
                row.ui_token_amount
                    .as_ref()
                    .map(|amount| amount.ui_amount.to_bits())
            })
            .collect::<Vec<_>>()
    };
    bits(a) == bits(b)
}
