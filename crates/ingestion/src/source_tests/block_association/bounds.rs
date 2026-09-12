use super::*;
use yellowstone_grpc_proto::prelude::InnerInstructions;

#[test]
fn block_cardinality_n_passes_n_plus_one_refuses_before_other_work() -> Result<()> {
    let (expected, mut block) = pair(false, false)?;
    let other = SubscribeUpdateTransactionInfo {
        signature: vec![8; 64],
        ..Default::default()
    };
    block
        .transactions
        .resize(MAX_BLOCK_TRANSACTIONS, other.clone());
    assert!(observe("transactions-n", &expected, &block).is_ok());
    block.transactions.push(other);
    refused(
        "transactions-n-plus-one",
        &expected,
        &block,
        Refusal::TooManyTransactions(MAX_BLOCK_TRANSACTIONS + 1),
    );
    let mut invalid = expected.clone();
    tx_mut(&mut invalid).transaction = None;
    refused(
        "transactions-limit-before-missing-info",
        &invalid,
        &block,
        Refusal::TooManyTransactions(MAX_BLOCK_TRANSACTIONS + 1),
    );
    Ok(())
}

fn padded(mut info: SubscribeUpdateTransactionInfo, size: usize) -> SubscribeUpdateTransactionInfo {
    info.meta.as_mut().unwrap().log_messages = vec![String::new()];
    let mut n = size - info.encoded_len();
    loop {
        info.meta.as_mut().unwrap().log_messages[0] = "x".repeat(n);
        let actual = info.encoded_len();
        if actual == size {
            return info;
        }
        if actual > size {
            n -= actual - size;
        } else {
            n += size - actual;
        }
    }
}

#[test]
fn expected_selected_info_encoded_size_n_and_n_plus_one() -> Result<()> {
    let (mut expected, mut block) = pair(false, false)?;
    let at_limit = padded(block.transactions[0].clone(), MAX_INFO_BYTES);
    tx_mut(&mut expected).transaction = Some(at_limit.clone());
    block.transactions[0] = at_limit;
    assert!(observe("info-bytes-n", &expected, &block).is_ok());
    let over = padded(block.transactions[0].clone(), MAX_INFO_BYTES + 1);
    block.transactions[0] = over.clone();
    refused(
        "selected-info-bytes-n-plus-one",
        &expected,
        &block,
        Refusal::InfoTooLarge(InfoSide::Selected),
    );
    tx_mut(&mut expected).transaction = Some(over);
    refused(
        "expected-info-bytes-n-plus-one",
        &expected,
        &block,
        Refusal::InfoTooLarge(InfoSide::Expected),
    );
    Ok(())
}

#[test]
fn repeated_items_n_and_n_plus_one_are_bounded_before_encoding() -> Result<()> {
    let (mut expected, mut block) = pair(false, false)?;
    let mut info = block.transactions[0].clone();
    info.meta.as_mut().unwrap().log_messages = vec![String::new(); MAX_INFO_ITEMS];
    tx_mut(&mut expected).transaction = Some(info.clone());
    block.transactions[0] = info.clone();
    assert!(observe("info-items-n", &expected, &block).is_ok());
    info.meta.as_mut().unwrap().log_messages.push(String::new());
    block.transactions[0] = info.clone();
    refused(
        "selected-info-items-n-plus-one",
        &expected,
        &block,
        Refusal::InfoCardinalityExceeded(InfoSide::Selected),
    );
    tx_mut(&mut expected).transaction = Some(info);
    refused(
        "expected-info-items-n-plus-one",
        &expected,
        &block,
        Refusal::InfoCardinalityExceeded(InfoSide::Expected),
    );
    Ok(())
}

#[test]
fn every_repeated_info_collection_has_a_cardinality_guard() -> Result<()> {
    type Mutation = (&'static str, fn(&mut SubscribeUpdateTransactionInfo));
    let n = MAX_INFO_ITEMS + 1;
    let mutations: Vec<Mutation> = vec![
        ("signatures", |i| {
            i.transaction
                .as_mut()
                .unwrap()
                .signatures
                .resize_with(MAX_INFO_ITEMS + 1, Default::default)
        }),
        ("keys", |i| {
            i.transaction
                .as_mut()
                .unwrap()
                .message
                .as_mut()
                .unwrap()
                .account_keys
                .resize_with(MAX_INFO_ITEMS + 1, Default::default)
        }),
        ("instructions", |i| {
            i.transaction
                .as_mut()
                .unwrap()
                .message
                .as_mut()
                .unwrap()
                .instructions
                .resize_with(MAX_INFO_ITEMS + 1, Default::default)
        }),
        ("lookups", |i| {
            i.transaction
                .as_mut()
                .unwrap()
                .message
                .as_mut()
                .unwrap()
                .address_table_lookups
                .resize_with(MAX_INFO_ITEMS + 1, Default::default)
        }),
        ("pre-balances", |i| {
            i.meta
                .as_mut()
                .unwrap()
                .pre_balances
                .resize(MAX_INFO_ITEMS + 1, 0)
        }),
        ("post-balances", |i| {
            i.meta
                .as_mut()
                .unwrap()
                .post_balances
                .resize(MAX_INFO_ITEMS + 1, 0)
        }),
        ("inner-groups", |i| {
            i.meta
                .as_mut()
                .unwrap()
                .inner_instructions
                .resize_with(MAX_INFO_ITEMS + 1, Default::default)
        }),
        ("logs", |i| {
            i.meta
                .as_mut()
                .unwrap()
                .log_messages
                .resize_with(MAX_INFO_ITEMS + 1, Default::default)
        }),
        ("pre-token", |i| {
            i.meta
                .as_mut()
                .unwrap()
                .pre_token_balances
                .resize_with(MAX_INFO_ITEMS + 1, Default::default)
        }),
        ("post-token", |i| {
            i.meta
                .as_mut()
                .unwrap()
                .post_token_balances
                .resize_with(MAX_INFO_ITEMS + 1, Default::default)
        }),
        ("rewards", |i| {
            i.meta
                .as_mut()
                .unwrap()
                .rewards
                .resize_with(MAX_INFO_ITEMS + 1, Default::default)
        }),
        ("writable", |i| {
            i.meta
                .as_mut()
                .unwrap()
                .loaded_writable_addresses
                .resize_with(MAX_INFO_ITEMS + 1, Default::default)
        }),
        ("readonly", |i| {
            i.meta
                .as_mut()
                .unwrap()
                .loaded_readonly_addresses
                .resize_with(MAX_INFO_ITEMS + 1, Default::default)
        }),
        ("inner-instructions", |i| {
            i.meta.as_mut().unwrap().inner_instructions = vec![InnerInstructions {
                index: 0,
                instructions: vec![Default::default(); MAX_INFO_ITEMS + 1],
            }]
        }),
    ];
    let (expected, original) = pair(false, false)?;
    for (name, mutate) in mutations {
        let mut block = original.clone();
        mutate(&mut block.transactions[0]);
        refused(
            &format!("collection-{name}-{n}"),
            &expected,
            &block,
            Refusal::InfoCardinalityExceeded(InfoSide::Selected),
        );
    }
    Ok(())
}
