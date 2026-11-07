//! Adapter executes before scoped capture/business parsing/filtering/deduplication.
use super::yellowstone_facts::DecodeMiss;
use super::yellowstone_proto::decode_signature_from_proto;
use super::{FetchedObservation, RawSwapObservation};
use crate::transaction_membership::Capture;
use crate::transaction_membership::{disposition, enabled, Disposition};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use yellowstone_grpc_proto::prelude::{subscribe_update::UpdateOneof, SubscribeUpdate};

pub(super) fn transaction_outcome(
    tx: &yellowstone_grpc_proto::prelude::SubscribeUpdateTransaction,
    outcome: Disposition,
) {
    if !enabled() {
        return;
    }
    if let Some(signature) = tx
        .transaction
        .as_ref()
        .and_then(decode_signature_from_proto)
    {
        disposition(&signature, tx.slot, outcome);
    }
}

pub(super) fn refused(
    tx: &yellowstone_grpc_proto::prelude::SubscribeUpdateTransaction,
    miss: Option<DecodeMiss>,
) {
    transaction_outcome(
        tx,
        match miss {
            Some(DecodeMiss::Vote) => Disposition::ParseVote,
            Some(DecodeMiss::Failed) => Disposition::ParseFailed,
            Some(DecodeMiss::UninterestedProgram) => Disposition::ParseProgram,
            Some(DecodeMiss::NoAttributedSwap) => Disposition::ParseNoSwap,
            Some(DecodeMiss::InvalidAmount) => Disposition::ParseAmount,
            Some(DecodeMiss::UnsupportedPumpSwap) => Disposition::ParseUnsupportedPumpSwap,
            Some(DecodeMiss::UnsupportedMessageConfig) => Disposition::ParseUnsupportedMessage,
            _ => Disposition::ParseOther,
        },
    );
}

pub(super) fn signature_is_seen(
    seen: &HashMap<String, Instant>,
    raw: &RawSwapObservation,
    ttl: Duration,
    now: Instant,
) -> bool {
    let duplicate = super::core::is_seen_signature(seen, &raw.signature, ttl, now);
    if duplicate {
        disposition(&raw.signature, raw.slot, Disposition::SourceDedup);
    }
    duplicate
}

pub(super) fn output_discarded(item: &FetchedObservation, reason: super::queue::QueueDiscard) {
    let outcome = match reason {
        super::queue::QueueDiscard::Replaced => Disposition::OutputReplaced,
        super::queue::QueueDiscard::Closed => Disposition::OutputClosed,
    };
    disposition(&item.raw.signature, item.raw.slot, outcome);
}

pub(crate) fn decoded(capture: &Capture, update: &SubscribeUpdate) {
    let Some(UpdateOneof::Transaction(tx)) = update.update_oneof.as_ref() else {
        return;
    };
    let Some(info) = tx.transaction.as_ref() else {
        capture.gap();
        return;
    };
    let raw = if info.signature.is_empty() {
        info.transaction
            .as_ref()
            .and_then(|t| t.signatures.first())
            .map(Vec::as_slice)
    } else {
        Some(info.signature.as_slice())
    };
    if raw.is_none_or(|s| s.len() != 64) {
        capture.gap();
        return;
    }
    if let Some(signature) = decode_signature_from_proto(info) {
        capture.decoded(&signature, tx.slot);
    } else {
        capture.gap();
    }
}

#[cfg(test)]
#[path = "../source_tests/transaction_membership_boundary.rs"]
mod tests;
