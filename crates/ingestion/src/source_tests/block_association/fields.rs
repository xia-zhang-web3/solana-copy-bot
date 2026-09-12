use super::*;
use yellowstone_grpc_proto::prelude::{InnerInstructions, ReturnData, TransactionError};

type Mutation = (&'static str, fn(&mut SubscribeUpdateTransactionInfo));

fn mutations() -> Vec<Mutation> {
    vec![
        ("index", |i| i.index += 1),
        ("vote", |i| i.is_vote = !i.is_vote),
        ("transaction-absent", |i| i.transaction = None),
        ("transaction-signatures", |i| {
            i.transaction.as_mut().unwrap().signatures.push(vec![2; 64])
        }),
        ("message-absent", |i| {
            i.transaction.as_mut().unwrap().message = None
        }),
        ("header", |i| {
            i.transaction
                .as_mut()
                .unwrap()
                .message
                .as_mut()
                .unwrap()
                .header
                .get_or_insert_with(Default::default)
                .num_required_signatures += 1
        }),
        ("keys", |i| {
            i.transaction
                .as_mut()
                .unwrap()
                .message
                .as_mut()
                .unwrap()
                .account_keys[0][0] ^= 1
        }),
        ("recent-blockhash", |i| {
            i.transaction
                .as_mut()
                .unwrap()
                .message
                .as_mut()
                .unwrap()
                .recent_blockhash = vec![82; 32]
        }),
        ("instructions", |i| {
            i.transaction
                .as_mut()
                .unwrap()
                .message
                .as_mut()
                .unwrap()
                .instructions
                .push(Default::default())
        }),
        ("instruction-data", |i| {
            i.transaction
                .as_mut()
                .unwrap()
                .message
                .as_mut()
                .unwrap()
                .instructions[0]
                .data
                .push(1)
        }),
        ("instruction-accounts", |i| {
            i.transaction
                .as_mut()
                .unwrap()
                .message
                .as_mut()
                .unwrap()
                .instructions[0]
                .accounts
                .push(1)
        }),
        ("instruction-program", |i| {
            i.transaction
                .as_mut()
                .unwrap()
                .message
                .as_mut()
                .unwrap()
                .instructions[0]
                .program_id_index += 1
        }),
        ("versioned", |i| {
            let m = i.transaction.as_mut().unwrap().message.as_mut().unwrap();
            m.versioned = !m.versioned;
        }),
        ("lookups", |i| {
            i.transaction
                .as_mut()
                .unwrap()
                .message
                .as_mut()
                .unwrap()
                .address_table_lookups
                .push(Default::default())
        }),
        ("meta-absent", |i| i.meta = None),
        ("error", |i| {
            i.meta.as_mut().unwrap().err = Some(TransactionError { err: vec![1] })
        }),
        ("fee", |i| i.meta.as_mut().unwrap().fee += 1),
        ("pre-balances", |i| {
            i.meta.as_mut().unwrap().pre_balances.push(1)
        }),
        ("post-balances", |i| {
            i.meta.as_mut().unwrap().post_balances.push(1)
        }),
        ("inner", |i| {
            i.meta
                .as_mut()
                .unwrap()
                .inner_instructions
                .push(InnerInstructions {
                    index: 1,
                    instructions: vec![],
                })
        }),
        ("inner-none", |i| {
            let m = i.meta.as_mut().unwrap();
            m.inner_instructions_none = !m.inner_instructions_none;
        }),
        ("logs", |i| {
            i.meta
                .as_mut()
                .unwrap()
                .log_messages
                .push("different".into())
        }),
        ("logs-none", |i| {
            let m = i.meta.as_mut().unwrap();
            m.log_messages_none = !m.log_messages_none;
        }),
        ("pre-token", |i| {
            i.meta
                .as_mut()
                .unwrap()
                .pre_token_balances
                .push(Default::default())
        }),
        ("post-token", |i| {
            i.meta
                .as_mut()
                .unwrap()
                .post_token_balances
                .push(Default::default())
        }),
        ("raw-amount", |i| {
            i.meta.as_mut().unwrap().pre_token_balances[0]
                .ui_token_amount
                .as_mut()
                .unwrap()
                .amount
                .push('1')
        }),
        ("decimals", |i| {
            i.meta.as_mut().unwrap().pre_token_balances[0]
                .ui_token_amount
                .as_mut()
                .unwrap()
                .decimals += 1
        }),
        ("ui-float", |i| {
            i.meta.as_mut().unwrap().pre_token_balances[0]
                .ui_token_amount
                .as_mut()
                .unwrap()
                .ui_amount += 1.0
        }),
        ("ui-string", |i| {
            i.meta.as_mut().unwrap().pre_token_balances[0]
                .ui_token_amount
                .as_mut()
                .unwrap()
                .ui_amount_string
                .push('1')
        }),
        ("row-index", |i| {
            i.meta.as_mut().unwrap().pre_token_balances[0].account_index += 1
        }),
        ("row-owner", |i| {
            i.meta.as_mut().unwrap().pre_token_balances[0]
                .owner
                .push('1')
        }),
        ("row-mint", |i| {
            i.meta.as_mut().unwrap().pre_token_balances[0]
                .mint
                .push('1')
        }),
        ("row-program", |i| {
            i.meta.as_mut().unwrap().pre_token_balances[0]
                .program_id
                .push('1')
        }),
        ("rewards", |i| {
            i.meta.as_mut().unwrap().rewards.push(Default::default())
        }),
        ("loaded-writable", |i| {
            i.meta
                .as_mut()
                .unwrap()
                .loaded_writable_addresses
                .push(vec![1; 32])
        }),
        ("loaded-readonly", |i| {
            i.meta
                .as_mut()
                .unwrap()
                .loaded_readonly_addresses
                .push(vec![1; 32])
        }),
        ("return-data", |i| {
            i.meta.as_mut().unwrap().return_data = Some(ReturnData {
                program_id: vec![1; 32],
                data: vec![2],
            })
        }),
        ("return-data-none", |i| {
            let m = i.meta.as_mut().unwrap();
            m.return_data_none = !m.return_data_none;
        }),
        ("compute-units", |i| {
            i.meta.as_mut().unwrap().compute_units_consumed = Some(0)
        }),
        ("cost-units", |i| {
            i.meta.as_mut().unwrap().cost_units = Some(0)
        }),
    ]
}

#[test]
fn same_signature_requires_full_transaction_message_meta_equality() -> Result<()> {
    let (expected, original) = pair(false, false)?;
    for (name, mutate) in mutations() {
        let mut block = original.clone();
        mutate(&mut block.transactions[0]);
        assert_eq!(
            block.transactions[0].signature,
            original.transactions[0].signature
        );
        refused(
            &format!("info-{name}"),
            &expected,
            &block,
            Refusal::InfoMismatch,
        );
    }
    Ok(())
}

#[test]
fn signed_zero_bits_in_decoded_info_cannot_hide_behind_prost_default() -> Result<()> {
    let (mut expected, mut block) = pair(false, false)?;
    tx_mut(&mut expected)
        .transaction
        .as_mut()
        .unwrap()
        .meta
        .as_mut()
        .unwrap()
        .pre_token_balances[0]
        .ui_token_amount
        .as_mut()
        .unwrap()
        .ui_amount = 0.0;
    block.transactions[0] = transaction(&expected).transaction.as_ref().unwrap().clone();
    block.transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .pre_token_balances[0]
        .ui_token_amount
        .as_mut()
        .unwrap()
        .ui_amount = -0.0;
    // This control deliberately exercises borrowed decoded objects: prost's
    // re-encoding discards the sign of default zero. Capture both actual bits.
    assert_eq!(
        transaction(&expected)
            .transaction
            .as_ref()
            .unwrap()
            .encode_to_vec(),
        block.transactions[0].encode_to_vec()
    );
    let result = associate(&expected, &block);
    if let Ok(dir) = std::env::var("B80_CAPTURE_DIR") {
        let v = json!({"expected_ui_bits":0.0f64.to_bits(),"selected_ui_bits":(-0.0f64).to_bits(),
            "outcome":result_json(&result),"input_kind":"decoded protobuf; re-encoding normalizes signed zero"});
        std::fs::write(
            std::path::Path::new(&dir).join("signed-zero-decoded.json"),
            serde_json::to_vec_pretty(&v)?,
        )?;
    }
    assert_eq!(result.unwrap_err(), Refusal::InfoMismatch);
    Ok(())
}

#[test]
fn existing_program_policy_is_applied_after_exact_info_match() -> Result<()> {
    let (expected, block) = pair(false, false)?;
    let empty = std::collections::HashSet::new();
    let outcome =
        associate_yellowstone_transaction(transaction(&expected), &block, &empty, &empty, &empty);
    if let Ok(dir) = std::env::var("B80_CAPTURE_DIR") {
        std::fs::write(
            std::path::Path::new(&dir).join("empty-policy.json"),
            serde_json::to_vec_pretty(&result_json(&outcome))?,
        )?;
    }
    assert_eq!(outcome.unwrap_err(), Refusal::NoCheckedSwapFacts);
    Ok(())
}
