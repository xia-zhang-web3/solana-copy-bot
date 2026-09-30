//! Borrowed execution order. Delivery vectors and their float witnesses stay
//! untouched. At the existing 4096-transaction limit, the two maps' reference
//! storage uses <=64 KiB (plus their two Vec headers).
use yellowstone_grpc_proto::prelude::{SubscribeUpdateBlock, SubscribeUpdateTransactionInfo};

pub(super) struct Invalid {
    pub path: String,
    pub reason: &'static str,
    pub position: Option<usize>,
}

pub(super) fn execution_order(
    block: &SubscribeUpdateBlock,
) -> Result<Vec<Option<&SubscribeUpdateTransactionInfo>>, Invalid> {
    let count = block.transactions.len();
    if block.executed_transaction_count != count as u64
        || count > super::super::yellowstone_block_association::MAX_BLOCK_TRANSACTIONS
    {
        return Err(Invalid {
            path: "transactions.length".into(),
            reason: "executed_count_or_existing_transaction_bound",
            position: None,
        });
    }
    let mut order = vec![None; count];
    for (position, info) in block.transactions.iter().enumerate() {
        let invalid = |reason, field| Invalid {
            path: format!("transactions.delivery[{position}].{field}"),
            reason,
            position: Some(position),
        };
        let index = usize::try_from(info.index)
            .ok()
            .filter(|index| *index < count)
            .ok_or_else(|| invalid("execution_index_out_of_range", "index"))?;
        if order[index].is_some() {
            return Err(invalid("duplicate_execution_index", "index"));
        }
        let first = info.transaction.as_ref().and_then(|t| t.signatures.first());
        if info.signature.len() != 64 || first != Some(&info.signature) {
            return Err(invalid("first_signature_binding", "signature_binding"));
        }
        order[index] = Some(info);
    }
    // Count equality and unique in-range indices prove dense coverage. No
    // sorting, cloned blocks, duplicate overwrites or omitted transactions.
    Ok(order)
}
