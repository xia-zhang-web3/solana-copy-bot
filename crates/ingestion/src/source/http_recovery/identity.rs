//! Common decoded transaction identity. Wire encodings remain independent
//! evidence. Every known Info field participates, with explicit IEEE float bits.
use copybot_core_types::association_delivery::InfoIdentity;
use prost::Message;
use yellowstone_grpc_proto::prelude::{
    SubscribeUpdateBlock, SubscribeUpdateTransactionInfo, TokenBalance, TransactionStatusMeta,
};

fn tokens_equal(a: &[TokenBalance], b: &[TokenBalance]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|(a, b)| {
            a.account_index == b.account_index
                && a.mint == b.mint
                && a.owner == b.owner
                && a.program_id == b.program_id
                && match (&a.ui_token_amount, &b.ui_token_amount) {
                    (Some(a), Some(b)) => {
                        a.ui_amount.to_bits() == b.ui_amount.to_bits()
                            && a.amount == b.amount
                            && a.decimals == b.decimals
                            && a.ui_amount_string == b.ui_amount_string
                    }
                    (None, None) => true,
                    _ => false,
                }
        })
}
fn meta_equal(a: &TransactionStatusMeta, b: &TransactionStatusMeta) -> bool {
    a.err == b.err
        && a.fee == b.fee
        && a.pre_balances == b.pre_balances
        && a.post_balances == b.post_balances
        && a.inner_instructions == b.inner_instructions
        && a.inner_instructions_none == b.inner_instructions_none
        && a.log_messages == b.log_messages
        && a.log_messages_none == b.log_messages_none
        && tokens_equal(&a.pre_token_balances, &b.pre_token_balances)
        && tokens_equal(&a.post_token_balances, &b.post_token_balances)
        && a.rewards == b.rewards
        && a.loaded_writable_addresses == b.loaded_writable_addresses
        && a.loaded_readonly_addresses == b.loaded_readonly_addresses
        && a.return_data == b.return_data
        && a.return_data_none == b.return_data_none
        && a.compute_units_consumed == b.compute_units_consumed
        && a.cost_units == b.cost_units
}
pub(crate) fn info_equal(
    a: &SubscribeUpdateTransactionInfo,
    b: &SubscribeUpdateTransactionInfo,
) -> bool {
    a.signature == b.signature
        && a.index == b.index
        && a.is_vote == b.is_vote
        && a.transaction == b.transaction
        && match (&a.meta, &b.meta) {
            (Some(a), Some(b)) => meta_equal(a, b),
            (None, None) => true,
            _ => false,
        }
}
pub(crate) fn info_equivalent(a: &InfoIdentity, b: &InfoIdentity) -> bool {
    if a.float_bits != b.float_bits {
        return false;
    }
    match (
        SubscribeUpdateTransactionInfo::decode(a.encoded.as_slice()),
        SubscribeUpdateTransactionInfo::decode(b.encoded.as_slice()),
    ) {
        (Ok(a), Ok(b)) => info_equal(&a, &b),
        _ => false,
    }
}
pub(crate) fn block_equivalent(a: &SubscribeUpdateBlock, b: &SubscribeUpdateBlock) -> bool {
    // HTTP cannot report gRPC accounts/entries or their stream coverage counts.
    // Compare full transaction and parent/block evidence, never pretend those
    // coverage fields were fetched. The original gRPC/RPC inputs stay retained.
    let rewards = |block: &SubscribeUpdateBlock| block.rewards.clone().unwrap_or_default();
    a.slot == b.slot
        && a.parent_slot == b.parent_slot
        && a.blockhash == b.blockhash
        && a.parent_blockhash == b.parent_blockhash
        && a.block_time == b.block_time
        && a.block_height == b.block_height
        && rewards(a) == rewards(b)
        && a.executed_transaction_count == b.executed_transaction_count
        && a.transactions.len() == b.transactions.len()
        && match (
            super::index_identity::execution_order(a),
            super::index_identity::execution_order(b),
        ) {
            (Ok(a), Ok(b)) => a
                .into_iter()
                .zip(b)
                .all(|(a, b)| info_equal(a.unwrap(), b.unwrap())),
            _ => false,
        }
}
