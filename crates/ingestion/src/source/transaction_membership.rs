//! Adapter executes before scoped capture/business parsing/filtering/deduplication.
use super::yellowstone_proto::decode_signature_from_proto;
use crate::transaction_membership::Capture;
use yellowstone_grpc_proto::prelude::{subscribe_update::UpdateOneof, SubscribeUpdate};

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
