//! Metadata mutations describe the unchanged full runtime predicate.
use super::super::identity_difference;
use super::*;

#[test]
fn anchor_diagnostic_visitor_meta_and_reward_field_mutations() {
    let grpc = super::anchor_difference_tests::fixture();
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
    check!("transactions[0].meta.err.presence", |b| b.transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .err = None);
    check!("transactions[0].meta.err.err[0]", |b| b.transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .err
        .as_mut()
        .unwrap()
        .err[0] += 1);
    check!("transactions[0].meta.fee", |b| b.transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .fee += 1);
    check!("transactions[0].meta.pre_balances[0]", |b| b
        .transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .pre_balances[0] +=
        1);
    check!("transactions[0].meta.post_balances[0]", |b| b
        .transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .post_balances[0] +=
        1);
    check!("transactions[0].meta.inner_instructions_none", |b| b
        .transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .inner_instructions_none =
        !b.transactions[0]
            .meta
            .as_mut()
            .unwrap()
            .inner_instructions_none);
    check!("transactions[0].meta.log_messages_none", |b| b
        .transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .log_messages_none =
        !b.transactions[0].meta.as_mut().unwrap().log_messages_none);
    check!("transactions[0].meta.return_data_none", |b| b
        .transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .return_data_none =
        !b.transactions[0].meta.as_mut().unwrap().return_data_none);
    check!("transactions[0].meta.inner_instructions[0].index", |b| b
        .transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .inner_instructions[0]
        .index +=
        1);
    check!("transactions[0].meta.log_messages[0]", |b| b.transactions
        [0]
    .meta
    .as_mut()
    .unwrap()
    .log_messages[0]
        .push_str("x"));
    check!(
        "transactions[0].meta.inner_instructions[0].instructions[0].program_id_index",
        |b| b.transactions[0].meta.as_mut().unwrap().inner_instructions[0].instructions[0]
            .program_id_index += 1
    );
    check!(
        "transactions[0].meta.inner_instructions[0].instructions[0].accounts[0]",
        |b| b.transactions[0].meta.as_mut().unwrap().inner_instructions[0].instructions[0]
            .accounts[0] += 1
    );
    check!(
        "transactions[0].meta.inner_instructions[0].instructions[0].data[0]",
        |b| b.transactions[0].meta.as_mut().unwrap().inner_instructions[0].instructions[0].data
            [0] += 1
    );
    check!(
        "transactions[0].meta.inner_instructions[0].instructions[0].stack_height.presence",
        |b| b.transactions[0].meta.as_mut().unwrap().inner_instructions[0].instructions[0]
            .stack_height = None
    );
    check!(
        "transactions[0].meta.pre_token_balances[0].account_index",
        |b| b.transactions[0].meta.as_mut().unwrap().pre_token_balances[0].account_index += 1
    );
    check!("transactions[0].meta.pre_token_balances[0].mint", |b| b
        .transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .pre_token_balances[0]
        .mint
        .push_str("x"));
    check!("transactions[0].meta.pre_token_balances[0].owner", |b| b
        .transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .pre_token_balances[0]
        .owner
        .push_str("x"));
    check!(
        "transactions[0].meta.pre_token_balances[0].program_id",
        |b| b.transactions[0].meta.as_mut().unwrap().pre_token_balances[0]
            .program_id
            .push_str("x")
    );
    check!(
        "transactions[0].meta.pre_token_balances[0].ui_token_amount.presence",
        |b| b.transactions[0].meta.as_mut().unwrap().pre_token_balances[0].ui_token_amount = None
    );
    check!(
        "transactions[0].meta.pre_token_balances[0].ui_token_amount.ui_amount",
        |b| b.transactions[0].meta.as_mut().unwrap().pre_token_balances[0]
            .ui_token_amount
            .as_mut()
            .unwrap()
            .ui_amount += 1.0
    );
    check!(
        "transactions[0].meta.pre_token_balances[0].ui_token_amount.amount",
        |b| b.transactions[0].meta.as_mut().unwrap().pre_token_balances[0]
            .ui_token_amount
            .as_mut()
            .unwrap()
            .amount
            .push_str("x")
    );
    check!(
        "transactions[0].meta.pre_token_balances[0].ui_token_amount.decimals",
        |b| b.transactions[0].meta.as_mut().unwrap().pre_token_balances[0]
            .ui_token_amount
            .as_mut()
            .unwrap()
            .decimals += 1
    );
    check!(
        "transactions[0].meta.pre_token_balances[0].ui_token_amount.ui_amount_string",
        |b| b.transactions[0].meta.as_mut().unwrap().pre_token_balances[0]
            .ui_token_amount
            .as_mut()
            .unwrap()
            .ui_amount_string
            .push_str("x")
    );
    check!(
        "transactions[0].meta.post_token_balances[0].account_index",
        |b| b.transactions[0].meta.as_mut().unwrap().post_token_balances[0].account_index += 1
    );
    check!("transactions[0].meta.post_token_balances[0].mint", |b| b
        .transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .post_token_balances[0]
        .mint
        .push_str("x"));
    check!("transactions[0].meta.post_token_balances[0].owner", |b| b
        .transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .post_token_balances[0]
        .owner
        .push_str("x"));
    check!(
        "transactions[0].meta.post_token_balances[0].program_id",
        |b| b.transactions[0].meta.as_mut().unwrap().post_token_balances[0]
            .program_id
            .push_str("x")
    );
    check!(
        "transactions[0].meta.post_token_balances[0].ui_token_amount.presence",
        |b| b.transactions[0].meta.as_mut().unwrap().post_token_balances[0].ui_token_amount = None
    );
    check!(
        "transactions[0].meta.post_token_balances[0].ui_token_amount.ui_amount",
        |b| b.transactions[0].meta.as_mut().unwrap().post_token_balances[0]
            .ui_token_amount
            .as_mut()
            .unwrap()
            .ui_amount += 1.0
    );
    check!(
        "transactions[0].meta.post_token_balances[0].ui_token_amount.amount",
        |b| b.transactions[0].meta.as_mut().unwrap().post_token_balances[0]
            .ui_token_amount
            .as_mut()
            .unwrap()
            .amount
            .push_str("x")
    );
    check!(
        "transactions[0].meta.post_token_balances[0].ui_token_amount.decimals",
        |b| b.transactions[0].meta.as_mut().unwrap().post_token_balances[0]
            .ui_token_amount
            .as_mut()
            .unwrap()
            .decimals += 1
    );
    check!(
        "transactions[0].meta.post_token_balances[0].ui_token_amount.ui_amount_string",
        |b| b.transactions[0].meta.as_mut().unwrap().post_token_balances[0]
            .ui_token_amount
            .as_mut()
            .unwrap()
            .ui_amount_string
            .push_str("x")
    );
    check!("rewards.rewards[0].pubkey", |b| b
        .rewards
        .as_mut()
        .unwrap()
        .rewards[0]
        .pubkey
        .push_str("x"));
    check!("rewards.rewards[0].lamports", |b| b
        .rewards
        .as_mut()
        .unwrap()
        .rewards[0]
        .lamports += 1);
    check!("rewards.rewards[0].post_balance", |b| b
        .rewards
        .as_mut()
        .unwrap()
        .rewards[0]
        .post_balance +=
        1);
    check!("rewards.rewards[0].reward_type", |b| b
        .rewards
        .as_mut()
        .unwrap()
        .rewards[0]
        .reward_type += 1);
    check!("rewards.rewards[0].commission", |b| b
        .rewards
        .as_mut()
        .unwrap()
        .rewards[0]
        .commission
        .push_str("x"));
    check!("rewards.rewards[0].commission_bps", |b| b
        .rewards
        .as_mut()
        .unwrap()
        .rewards[0]
        .commission_bps
        .push_str("x"));
    check!("transactions[0].meta.rewards[0].pubkey", |b| b.transactions
        [0]
    .meta
    .as_mut()
    .unwrap()
    .rewards[0]
        .pubkey
        .push_str("x"));
    check!("transactions[0].meta.rewards[0].lamports", |b| b
        .transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .rewards[0]
        .lamports +=
        1);
    check!("transactions[0].meta.rewards[0].post_balance", |b| b
        .transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .rewards[0]
        .post_balance +=
        1);
    check!("transactions[0].meta.rewards[0].reward_type", |b| b
        .transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .rewards[0]
        .reward_type +=
        1);
    check!("transactions[0].meta.rewards[0].commission", |b| b
        .transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .rewards[0]
        .commission
        .push_str("x"));
    check!("transactions[0].meta.rewards[0].commission_bps", |b| b
        .transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .rewards[0]
        .commission_bps
        .push_str("x"));
    check!(
        "transactions[0].meta.loaded_writable_addresses[0][0]",
        |b| b.transactions[0]
            .meta
            .as_mut()
            .unwrap()
            .loaded_writable_addresses[0][0] += 1
    );
    check!(
        "transactions[0].meta.loaded_readonly_addresses[0][0]",
        |b| b.transactions[0]
            .meta
            .as_mut()
            .unwrap()
            .loaded_readonly_addresses[0][0] += 1
    );
    check!("transactions[0].meta.return_data.program_id[0]", |b| b
        .transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .return_data
        .as_mut()
        .unwrap()
        .program_id[0] +=
        1);
    check!("transactions[0].meta.return_data.data[0]", |b| b
        .transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .return_data
        .as_mut()
        .unwrap()
        .data[0] +=
        1);
    check!("transactions[0].meta.return_data.presence", |b| b
        .transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .return_data =
        None);
    check!(
        "transactions[0].meta.compute_units_consumed.presence",
        |b| b.transactions[0]
            .meta
            .as_mut()
            .unwrap()
            .compute_units_consumed = None
    );
    check!("transactions[0].meta.cost_units.presence", |b| b
        .transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .cost_units =
        None);
    assert!(identity::block_equivalent(&grpc, &grpc));
    assert!(identity_difference::first(&grpc, &grpc).is_none());
}
