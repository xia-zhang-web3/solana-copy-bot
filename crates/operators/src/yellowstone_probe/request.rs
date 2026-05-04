use std::collections::HashMap;

use yellowstone_grpc_proto::prelude::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterBlocksMeta,
    SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions,
};

use super::config::ProbeMode;

pub(crate) fn build_subscribe_request(mode: ProbeMode, program_ids: &[String]) -> SubscribeRequest {
    match mode {
        ProbeMode::TransactionFilter => build_transaction_filter_subscribe_request(program_ids),
        ProbeMode::SlotsOnly => build_slots_only_subscribe_request(),
        ProbeMode::BlocksMeta => build_blocks_meta_subscribe_request(),
        ProbeMode::EmptyThenSend => build_empty_subscribe_request(),
    }
}

fn build_transaction_filter_subscribe_request(program_ids: &[String]) -> SubscribeRequest {
    let mut transactions = HashMap::new();
    transactions.insert(
        "copybot-swaps".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            signature: None,
            account_include: program_ids.to_vec(),
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

fn build_slots_only_subscribe_request() -> SubscribeRequest {
    let mut slots = HashMap::new();
    slots.insert(
        "copybot-slots-probe".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: Some(true),
            interslot_updates: Some(false),
        },
    );

    SubscribeRequest {
        accounts: HashMap::new(),
        slots,
        transactions: HashMap::new(),
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

fn build_blocks_meta_subscribe_request() -> SubscribeRequest {
    let mut blocks_meta = HashMap::new();
    blocks_meta.insert(
        "copybot-blocks-meta-probe".to_string(),
        SubscribeRequestFilterBlocksMeta {},
    );

    SubscribeRequest {
        accounts: HashMap::new(),
        slots: HashMap::new(),
        transactions: HashMap::new(),
        transactions_status: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta,
        entry: HashMap::new(),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        accounts_data_slice: Vec::new(),
        ping: None,
        from_slot: None,
    }
}

fn build_empty_subscribe_request() -> SubscribeRequest {
    SubscribeRequest {
        accounts: HashMap::new(),
        slots: HashMap::new(),
        transactions: HashMap::new(),
        transactions_status: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta: HashMap::new(),
        entry: HashMap::new(),
        commitment: None,
        accounts_data_slice: Vec::new(),
        ping: None,
        from_slot: None,
    }
}
