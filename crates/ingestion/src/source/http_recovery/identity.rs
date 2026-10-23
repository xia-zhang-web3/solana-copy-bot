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
fn meta_fields_equal(a: &TransactionStatusMeta, b: &TransactionStatusMeta) -> bool {
    a.err == b.err
        && a.fee == b.fee
        && a.pre_balances == b.pre_balances
        && a.post_balances == b.post_balances
        && a.inner_instructions == b.inner_instructions
        && a.inner_instructions_none == b.inner_instructions_none
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
fn meta_equal(a: &TransactionStatusMeta, b: &TransactionStatusMeta) -> bool {
    a.log_messages == b.log_messages && meta_fields_equal(a, b)
}
fn anchor_info_compatible(
    a: &SubscribeUpdateTransactionInfo,
    b: &SubscribeUpdateTransactionInfo,
) -> bool {
    a.signature == b.signature
        && a.index == b.index
        && a.is_vote == b.is_vote
        && a.transaction == b.transaction
        && match (&a.meta, &b.meta) {
            (Some(a), Some(b)) => {
                a.err.is_none()
                    && !a.log_messages_none
                    && meta_fields_equal(a, b)
                    && super::terminal_tail::equivalent(&a.log_messages, &b.log_messages)
            }
            _ => false,
        }
}
pub(super) fn same_execution(
    a: &SubscribeUpdateTransactionInfo,
    b: &SubscribeUpdateTransactionInfo,
) -> bool {
    a.signature == b.signature
        && a.index == b.index
        && a.is_vote == b.is_vote
        && a.transaction == b.transaction
        && match (&a.meta, &b.meta) {
            (Some(a), Some(b)) => meta_fields_equal(a, b),
            _ => false,
        }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AnchorComparison {
    Exact,
    SuccessfulTerminalUnwind,
    Mismatch,
}
impl AnchorComparison {
    pub(crate) fn admitted(self) -> bool {
        self != Self::Mismatch
    }
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Exact => "EXACT",
            Self::SuccessfulTerminalUnwind => "SUCCESSFUL_TERMINAL_UNWIND",
            Self::Mismatch => "MISMATCH",
        }
    }
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
pub(super) fn headers_equal(a: &SubscribeUpdateBlock, b: &SubscribeUpdateBlock) -> bool {
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
}
pub(crate) fn block_comparison(
    a: &SubscribeUpdateBlock,
    b: &SubscribeUpdateBlock,
) -> AnchorComparison {
    let headers_equal = headers_equal(a, b);
    if !headers_equal {
        return AnchorComparison::Mismatch;
    }
    let (Ok(a), Ok(b)) = (
        super::index_identity::execution_order(a),
        super::index_identity::execution_order(b),
    ) else {
        return AnchorComparison::Mismatch;
    };
    let mut result = AnchorComparison::Exact;
    for (a, b) in a.into_iter().zip(b) {
        let (Some(a), Some(b)) = (a, b) else {
            return AnchorComparison::Mismatch;
        };
        if !info_equal(a, b) {
            if !anchor_info_compatible(a, b) {
                return AnchorComparison::Mismatch;
            }
            result = AnchorComparison::SuccessfulTerminalUnwind;
        }
    }
    result
}
pub(crate) fn block_equivalent(a: &SubscribeUpdateBlock, b: &SubscribeUpdateBlock) -> bool {
    block_comparison(a, b).admitted()
}
