//! Field mutations prove the new visitor explains every existing compared field.
use super::super::identity_difference;
use super::*;
use yellowstone_grpc_proto::prelude::*;

pub(super) fn fixture() -> SubscribeUpdateBlock {
    let mut grpc = block::parse(10, &raw_block()).unwrap();
    let tx = grpc.transactions[0].transaction.as_mut().unwrap();
    let message = tx.message.as_mut().unwrap();
    message.instructions[0].data = vec![1];
    message
        .address_table_lookups
        .push(MessageAddressTableLookup {
            account_key: vec![1],
            writable_indexes: vec![1],
            readonly_indexes: vec![1],
        });
    message.config = Some(TransactionConfig {
        priority_fee: Some(1),
        compute_unit_limit: Some(1),
        loaded_accounts_data_size_limit: Some(1),
        heap_size: Some(1),
    });
    let meta = grpc.transactions[0].meta.as_mut().unwrap();
    meta.err = Some(TransactionError { err: vec![1] });
    meta.post_token_balances = meta.pre_token_balances.clone();
    meta.inner_instructions[0].instructions[0].data = vec![1];
    meta.loaded_writable_addresses = vec![vec![1]];
    meta.loaded_readonly_addresses = vec![vec![1]];
    grpc
}

#[test]
fn anchor_diagnostic_visitor_header_and_transaction_field_mutations() {
    let grpc = fixture();
    macro_rules! check {
        ($path:literal,$edit:expr) => {{
            let mut http = grpc.clone();
            let edit: fn(&mut SubscribeUpdateBlock) = $edit;
            edit(&mut http);
            assert!(
                !identity::block_equivalent(&grpc, &http),
                "{} mutation not compared",
                $path
            );
            let d = identity_difference::first(&grpc, &http)
                .expect("false predicate must have a first difference");
            assert_eq!(d.path, $path);
            assert_ne!(d.grpc, d.http);
        }};
    }
    check!("slot", |b| b.slot += 1);
    check!("parent_slot", |b| b.parent_slot += 1);
    check!("blockhash", |b| b.blockhash.push_str("x"));
    check!("parent_blockhash", |b| b.parent_blockhash.push_str("x"));
    check!("block_time.timestamp", |b| b
        .block_time
        .as_mut()
        .unwrap()
        .timestamp += 1);
    check!("block_time.presence", |b| b.block_time = None);
    check!("block_height.block_height", |b| b
        .block_height
        .as_mut()
        .unwrap()
        .block_height += 1);
    check!("block_height.presence", |b| b.block_height = None);
    check!("rewards.rewards.length", |b| b
        .rewards
        .as_mut()
        .unwrap()
        .rewards
        .clear());
    check!("rewards.num_partitions.num_partitions", |b| b
        .rewards
        .as_mut()
        .unwrap()
        .num_partitions
        .as_mut()
        .unwrap()
        .num_partitions +=
        1);
    check!("rewards.num_partitions.presence", |b| b
        .rewards
        .as_mut()
        .unwrap()
        .num_partitions =
        None);
    check!("executed_transaction_count", |b| b
        .executed_transaction_count +=
        1);
    check!("transactions.length", |b| b.transactions.clear());
    check!("transactions[0].signature[0]", |b| b.transactions[0]
        .signature[0] += 1);
    check!("transactions[0].index", |b| b.transactions[0].index += 1);
    check!("transactions[0].is_vote", |b| b.transactions[0].is_vote =
        !b.transactions[0].is_vote);
    check!("transactions[0].transaction.presence", |b| b.transactions
        [0]
    .transaction =
        None);
    check!("transactions[0].meta.presence", |b| b.transactions[0]
        .meta = None);
    check!("transactions[0].transaction.signatures[0][0]", |b| b
        .transactions[0]
        .transaction
        .as_mut()
        .unwrap()
        .signatures[0][0] +=
        1);
    check!("transactions[0].transaction.message.presence", |b| b
        .transactions[0]
        .transaction
        .as_mut()
        .unwrap()
        .message =
        None);
    check!(
        "transactions[0].transaction.message.header.num_required_signatures",
        |b| b.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap()
            .header
            .as_mut()
            .unwrap()
            .num_required_signatures += 1
    );
    check!(
        "transactions[0].transaction.message.header.num_readonly_signed_accounts",
        |b| b.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap()
            .header
            .as_mut()
            .unwrap()
            .num_readonly_signed_accounts += 1
    );
    check!(
        "transactions[0].transaction.message.header.num_readonly_unsigned_accounts",
        |b| b.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap()
            .header
            .as_mut()
            .unwrap()
            .num_readonly_unsigned_accounts += 1
    );
    check!("transactions[0].transaction.message.header.presence", |b| {
        b.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap()
            .header = None
    });
    check!(
        "transactions[0].transaction.message.account_keys[0][0]",
        |b| b.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap()
            .account_keys[0][0] += 1
    );
    check!(
        "transactions[0].transaction.message.recent_blockhash[0]",
        |b| b.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap()
            .recent_blockhash[0] += 1
    );
    check!("transactions[0].transaction.message.versioned", |b| b
        .transactions[0]
        .transaction
        .as_mut()
        .unwrap()
        .message
        .as_mut()
        .unwrap()
        .versioned =
        !b.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap()
            .versioned);
    check!(
        "transactions[0].transaction.message.instructions[0].program_id_index",
        |b| b.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap()
            .instructions[0]
            .program_id_index += 1
    );
    check!(
        "transactions[0].transaction.message.instructions[0].accounts[0]",
        |b| b.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap()
            .instructions[0]
            .accounts[0] += 1
    );
    check!(
        "transactions[0].transaction.message.instructions[0].data.length",
        |b| b.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap()
            .instructions[0]
            .data
            .clear()
    );
    check!(
        "transactions[0].transaction.message.address_table_lookups[0].account_key[0]",
        |b| b.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap()
            .address_table_lookups[0]
            .account_key[0] += 1
    );
    check!(
        "transactions[0].transaction.message.address_table_lookups[0].writable_indexes[0]",
        |b| b.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap()
            .address_table_lookups[0]
            .writable_indexes[0] += 1
    );
    check!(
        "transactions[0].transaction.message.address_table_lookups[0].readonly_indexes[0]",
        |b| b.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap()
            .address_table_lookups[0]
            .readonly_indexes[0] += 1
    );
    check!("transactions[0].transaction.message.config.presence", |b| {
        b.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap()
            .config = None
    });
    check!(
        "transactions[0].transaction.message.config.priority_fee.presence",
        |b| b.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap()
            .config
            .as_mut()
            .unwrap()
            .priority_fee = None
    );
    check!(
        "transactions[0].transaction.message.config.compute_unit_limit.presence",
        |b| b.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap()
            .config
            .as_mut()
            .unwrap()
            .compute_unit_limit = None
    );
    check!(
        "transactions[0].transaction.message.config.loaded_accounts_data_size_limit.presence",
        |b| b.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap()
            .config
            .as_mut()
            .unwrap()
            .loaded_accounts_data_size_limit = None
    );
    check!(
        "transactions[0].transaction.message.config.heap_size.presence",
        |b| b.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap()
            .config
            .as_mut()
            .unwrap()
            .heap_size = None
    );
    assert!(identity::block_equivalent(&grpc, &grpc));
    assert!(identity_difference::first(&grpc, &grpc).is_none());
}
