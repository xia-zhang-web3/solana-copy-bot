use std::collections::HashMap;

use yellowstone_grpc_proto::prelude::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterTransactions,
};

use super::YellowstoneRuntimeConfig;

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
