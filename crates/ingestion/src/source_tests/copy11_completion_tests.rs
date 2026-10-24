//! Exact COPY11 plus explicit LOCAL gap variants of the same saved execution.
use super::anchor_tail_pair::load;
use crate::source::http_recovery::{complete_anchor::eligible, identity};
use yellowstone_grpc_proto::prelude::{SubscribeUpdateBlock, SubscribeUpdateTransactionInfo};

#[test]
fn completion_gap_preserves_available_records_and_execution_identity() {
    use yellowstone_grpc_proto::prelude::{
        CompiledInstruction, Message, Transaction, TransactionStatusMeta,
    };
    let program = bs58::encode([7; 32]).into_string();
    let logs = vec![
        format!("Program {program} invoke [1]"),
        "Program log: payload".into(),
        format!("Program {program} consumed 1 of 2 compute units"),
        format!("Program {program} success"),
    ];
    let http = SubscribeUpdateBlock {
        slot: 7,
        parent_slot: 6,
        blockhash: "child".into(),
        parent_blockhash: "parent".into(),
        executed_transaction_count: 1,
        transactions: vec![SubscribeUpdateTransactionInfo {
            signature: vec![9; 64],
            transaction: Some(Transaction {
                signatures: vec![vec![9; 64]],
                message: Some(Message {
                    account_keys: vec![vec![7; 32]],
                    instructions: vec![CompiledInstruction {
                        program_id_index: 0,
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            }),
            meta: Some(TransactionStatusMeta {
                log_messages: logs.clone(),
                ..Default::default()
            }),
            ..Default::default()
        }],
        ..Default::default()
    };
    for begin in 0..logs.len() {
        for end in begin + 1..=logs.len() {
            let mut live = http.clone();
            live.transactions[0].meta.as_mut().unwrap().log_messages = logs[..begin]
                .iter()
                .cloned()
                .chain(["Log truncated".into()])
                .chain(logs[end..].iter().cloned())
                .collect();
            assert!(eligible(&http, &live), "gap {begin}..{end}");
            let mut conflict = http.clone();
            conflict.transactions[0].meta.as_mut().unwrap().fee += 1;
            assert!(
                !eligible(&conflict, &live),
                "identity conflict {begin}..{end}"
            );
            if begin > 0 {
                live.transactions[0].meta.as_mut().unwrap().log_messages[0].push('x');
                assert!(!eligible(&http, &live), "available prefix {begin}..{end}");
            } else if end < logs.len() {
                live.transactions[0]
                    .meta
                    .as_mut()
                    .unwrap()
                    .log_messages
                    .last_mut()
                    .unwrap()
                    .push('x');
                assert!(!eligible(&http, &live), "available suffix {begin}..{end}");
            }
        }
    }
}

fn changed(block: &mut SubscribeUpdateBlock) -> &mut SubscribeUpdateTransactionInfo {
    block
        .transactions
        .iter_mut()
        .find(|i| i.index == 228)
        .unwrap()
}

#[test]
#[ignore = "pinned COPY11 seven-file pair; offline only"]
fn copy11_saved_completion_contract_and_refusals() {
    let pair = load();
    assert_eq!(pair.grpc.slot, 460843623);
    assert_eq!(pair.grpc.transactions.len(), 229);
    assert!(!identity::block_equivalent(&pair.http, &pair.grpc));
    let mut live = pair.grpc.clone();
    let logs = &changed(&mut live).meta.as_ref().unwrap().log_messages;
    assert_eq!(logs.len(), 137);
    assert_eq!(logs[135], "Log truncated");
    assert!(logs[136].ends_with(" success"));
    assert!(
        eligible(&pair.http, &pair.grpc),
        "exact COPY11 must select full HTTP"
    );

    let mut full = pair.http.clone();
    let full_logs = changed(&mut full)
        .meta
        .as_ref()
        .unwrap()
        .log_messages
        .clone();
    // One rule for missing controls, payload, invocations and terminal suffixes.
    // These are labeled LOCAL variants, not extra observed provider records.
    for (begin, end) in [
        (0, 1),
        (0, 137),
        (1, 3),
        (47, 105),
        (132, 136),
        (134, 136),
        (135, 136),
        (136, 137),
    ] {
        let mut g = pair.grpc.clone();
        changed(&mut g).meta.as_mut().unwrap().log_messages = full_logs[..begin]
            .iter()
            .cloned()
            .chain(["Log truncated".into()])
            .chain(full_logs[end..].iter().cloned())
            .collect();
        assert!(
            eligible(&pair.http, &g),
            "LOCAL contiguous gap {begin}..{end}"
        );
    }

    let mut count = 0;
    let mut refuse = |label: &str, h: SubscribeUpdateBlock, g: SubscribeUpdateBlock| {
        assert!(!eligible(&h, &g), "completion accepted {label}");
        count += 1;
    };
    for label in [
        "wrong_program",
        "CU_overflow",
        "missing_success",
        "extra_payload",
        "prefix",
        "suffix",
        "fee",
        "hash",
        "HTTP_marker",
        "unknown_control",
    ] {
        let mut h = pair.http.clone();
        let meta = changed(&mut h).meta.as_mut().unwrap();
        match label {
            "wrong_program" => {
                meta.log_messages[135] =
                    "Program 11111111111111111111111111111111 consumed 1 of 2 compute units".into()
            }
            "CU_overflow" => {
                meta.log_messages[135] =
                    meta.log_messages[135].replace("48923 of 115595", "115596 of 115595")
            }
            "missing_success" => {
                meta.log_messages.pop();
            }
            "extra_payload" => meta.log_messages.push("Program log: extra".into()),
            "prefix" => meta.log_messages[134].push('x'),
            "suffix" => meta.log_messages[136].push('x'),
            "fee" => meta.fee += 1,
            "hash" => h.blockhash.push('x'),
            "HTTP_marker" => meta.log_messages[135] = "Log truncated".into(),
            "unknown_control" => meta.log_messages[135] = "Program bad control".into(),
            _ => unreachable!(),
        }
        refuse(label, h, pair.grpc.clone());
    }
    for label in [
        "extra_marker",
        "non_bare_marker",
        "wrong_success",
        "reordered_available",
        "missing_marker",
        "empty_gap",
    ] {
        let mut g = pair.grpc.clone();
        let logs = &mut changed(&mut g).meta.as_mut().unwrap().log_messages;
        match label {
            "extra_marker" => logs[134] = "Log truncated".into(),
            "non_bare_marker" => logs[135].push('x'),
            "wrong_success" => logs[136].push('x'),
            "reordered_available" => logs.swap(132, 134),
            "missing_marker" => {
                logs.remove(135);
            }
            "empty_gap" => {
                logs.insert(135, full_logs[135].clone());
            }
            _ => unreachable!(),
        }
        refuse(label, pair.http.clone(), g);
    }
    assert_eq!(count, 16);
    eprintln!("COPY11_COMPLETION_CONTRACT actual_transactions=229 actual_pair=PASS LOCAL_gap_variants=8 refusals={count} generic_identity=MISMATCH");
}
