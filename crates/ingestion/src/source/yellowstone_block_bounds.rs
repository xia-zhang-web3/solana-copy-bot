use prost::Message;
use yellowstone_grpc_proto::prelude::SubscribeUpdateTransactionInfo;

use super::{AssociationRefusal, InfoSide};

/// Local coverage/work bounds, not protocol validity rules. Meta may be larger
/// than transaction wire bytes. 64 KiB covers small swaps and bounded logs; the
/// caller receives an explicit refusal for larger provider Info objects.
pub(in crate::source) const MAX_INFO_BYTES: usize = 64 * 1024;
/// Cap each repeated field, including nested inner-instruction lists, before
/// encoded_len walks it. The nested traversal is at most 256 * 256 entries.
pub(in crate::source) const MAX_INFO_ITEMS: usize = 256;

pub(in crate::source) fn check_info(
    info: &SubscribeUpdateTransactionInfo,
    side: InfoSide,
) -> Result<(), AssociationRefusal> {
    if !cardinality_ok(info) {
        return Err(AssociationRefusal::InfoCardinalityExceeded(side));
    }
    // This is the re-encoded size of an already decoded object, not original
    // wire bytes, nor a defense against transport/deserialization allocation.
    if info.encoded_len() > MAX_INFO_BYTES {
        return Err(AssociationRefusal::InfoTooLarge(side));
    }
    Ok(())
}

fn cardinality_ok(info: &SubscribeUpdateTransactionInfo) -> bool {
    if let Some(tx) = &info.transaction {
        if tx.signatures.len() > MAX_INFO_ITEMS {
            return false;
        }
        if let Some(message) = &tx.message {
            if [
                message.account_keys.len(),
                message.instructions.len(),
                message.address_table_lookups.len(),
            ]
            .into_iter()
            .any(|n| n > MAX_INFO_ITEMS)
            {
                return false;
            }
        }
    }
    if let Some(meta) = &info.meta {
        if [
            meta.pre_balances.len(),
            meta.post_balances.len(),
            meta.inner_instructions.len(),
            meta.log_messages.len(),
            meta.pre_token_balances.len(),
            meta.post_token_balances.len(),
            meta.rewards.len(),
            meta.loaded_writable_addresses.len(),
            meta.loaded_readonly_addresses.len(),
        ]
        .into_iter()
        .any(|n| n > MAX_INFO_ITEMS)
        {
            return false;
        }
        if meta
            .inner_instructions
            .iter()
            .any(|inner| inner.instructions.len() > MAX_INFO_ITEMS)
        {
            return false;
        }
    }
    true
}
