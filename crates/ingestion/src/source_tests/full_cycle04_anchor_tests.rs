//! Actual full-cycle04 pair through the current-fetch callback; offline only.
use super::anchor_complete_replay::local_server;
use crate::source::http_recovery::{complete_anchor::eligible, identity, ConfirmedHttpRecovery};
use crate::source::tests::{anchor_tail_consumers::projection, anchor_tail_pair::load};
use serde_json::json;
use std::{collections::BTreeMap, time::Duration};
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;

fn info(
    block: &mut SubscribeUpdateBlock,
    index: u64,
) -> &mut yellowstone_grpc_proto::prelude::SubscribeUpdateTransactionInfo {
    block
        .transactions
        .iter_mut()
        .find(|i| i.index == index)
        .unwrap()
}

#[tokio::test]
#[ignore = "pinned full-cycle04 seven-file pair; loopback only"]
async fn full_cycle04_saved_pair_current_callback() {
    let pair = load();
    assert_eq!(pair.grpc.slot, 464521751);
    assert_eq!(pair.grpc.transactions.len(), 363);
    assert!(!identity::block_equivalent(&pair.http, &pair.grpc));
    let mut isolated = vec![];
    for index in [102, 123] {
        let mut live = pair.grpc.clone();
        for h in &pair.http.transactions {
            if h.index != index {
                info(&mut live, h.index).meta.as_mut().unwrap().log_messages =
                    h.meta.as_ref().unwrap().log_messages.clone();
            }
        }
        assert!(!identity::block_equivalent(&pair.http, &live));
        isolated.push(json!({"index":index,"eligible":eligible(&pair.http,&live)}));
    }
    let server = local_server(
        BTreeMap::from([(pair.grpc.slot, pair.raw_http.clone())]),
        vec![],
    )
    .await;
    let client =
        ConfirmedHttpRecovery::new(&server.http, None, 4, 8 << 20, Duration::from_secs(10))
            .unwrap();
    let (selection, error) = match client
        .current_anchor(pair.grpc.slot, &pair.grpc, None, "cycle04:0".into())
        .await
    {
        Ok(choice) => (Some(choice), None),
        Err(error) => (None, Some(format!("{error:#}"))),
    };
    let witness = json!({"slot":pair.grpc.slot,"raw_pair_transactions":363,
        "isolated_variants":isolated,"callback_error":error,
        "generic_identity_equal":false,"local_getBlock_requests":server.calls.lock().unwrap().len(),
        "real_provider_requests":0,"raw_inputs_unchanged":true});
    if let Ok(path) = std::env::var("COPYBOT_CYCLE04_OUTPUT") {
        std::fs::write(path, serde_json::to_vec_pretty(&witness).unwrap()).unwrap();
    }
    eprintln!("FULL_CYCLE04_EXACT_PAIR {witness}");
    let selected = selection
        .expect("exact real same-execution pair must select current HTTP")
        .for_context("cycle04:0")
        .unwrap();
    assert_eq!(selected.block(), &pair.http);
    assert!(isolated.iter().all(|v| v["eligible"] == true));
    for h in &pair.http.transactions {
        let chosen = selected
            .block()
            .transactions
            .iter()
            .find(|i| i.index == h.index)
            .unwrap();
        assert_eq!(
            projection(chosen, pair.http.slot),
            projection(h, pair.http.slot)
        );
    }
    let failed = pair
        .http
        .transactions
        .iter()
        .find(|i| i.index == 102)
        .unwrap();
    let incoming = pair
        .grpc
        .transactions
        .iter()
        .find(|i| i.index == 102)
        .unwrap();
    assert!(incoming.meta.as_ref().unwrap().err.is_some());
    assert_eq!(
        failed.meta.as_ref().unwrap().err,
        incoming.meta.as_ref().unwrap().err
    );
    assert_ne!(
        projection(failed, pair.http.slot)["facts"]["status"],
        "checked"
    );
    assert!(!identity::block_equivalent(selected.block(), &pair.grpc));
}

#[tokio::test]
#[ignore = "pinned full-cycle04 pair; causal LOCAL refusals"]
async fn full_cycle04_saved_pair_refusals_and_context() {
    let pair = load();
    assert!(eligible(&pair.http, &pair.grpc));
    for mode in [
        "known_prefix",
        "known_close",
        "fee",
        "err",
        "signature",
        "index",
        "parent",
        "HTTP_marker",
        "incomplete",
        "no_open_frame",
    ] {
        let mut h = pair.http.clone();
        let mut g = pair.grpc.clone();
        match mode {
            "known_prefix" => info(&mut g, 123).meta.as_mut().unwrap().log_messages[158].push('x'),
            "known_close" => info(&mut g, 123).meta.as_mut().unwrap().log_messages[160].push('x'),
            "fee" => info(&mut h, 123).meta.as_mut().unwrap().fee += 1,
            "err" => info(&mut h, 102).meta.as_mut().unwrap().err = None,
            "signature" => info(&mut h, 123).signature[0] ^= 1,
            "index" => info(&mut h, 123).index = 102,
            "parent" => h.parent_slot -= 1,
            "HTTP_marker" => info(&mut h, 123)
                .meta
                .as_mut()
                .unwrap()
                .log_messages
                .push("Log truncated".into()),
            "incomplete" => {
                info(&mut h, 123).meta.as_mut().unwrap().log_messages.pop();
            }
            "no_open_frame" => {
                info(&mut g, 123).meta.as_mut().unwrap().log_messages = vec![
                    "Log truncated".into(),
                    "Program TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb success".into(),
                ]
            }
            _ => unreachable!(),
        }
        assert!(!eligible(&h, &g), "accepted {mode}");
    }
    let server = local_server(
        BTreeMap::from([(pair.grpc.slot, pair.raw_http.clone())]),
        vec![],
    )
    .await;
    let client =
        ConfirmedHttpRecovery::new(&server.http, None, 4, 8 << 20, Duration::from_secs(10))
            .unwrap();
    let current = client
        .current_anchor(pair.grpc.slot, &pair.grpc, None, "cycle04:0".into())
        .await
        .unwrap();
    assert_eq!(
        current.for_context("cycle04:1").err().unwrap().to_string(),
        "http_anchor_stale_association_context"
    );
}

fn modeled() -> SubscribeUpdateBlock {
    use yellowstone_grpc_proto::prelude::*;
    let p = bs58::encode([7; 32]).into_string();
    let q = bs58::encode([8; 32]).into_string();
    SubscribeUpdateBlock {
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
                    account_keys: vec![vec![7; 32], vec![8; 32]],
                    instructions: vec![
                        CompiledInstruction {
                            program_id_index: 0,
                            ..Default::default()
                        },
                        CompiledInstruction {
                            program_id_index: 1,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            meta: Some(TransactionStatusMeta {
                log_messages: vec![
                    format!("Program {p} invoke [1]"),
                    "Program log: known".into(),
                    format!("Program {p} consumed 1 of 2 compute units"),
                    format!("Program {p} success"),
                    format!("Program {p} invoke [1]"),
                    "Program log: later same program".into(),
                    format!("Program {p} success"),
                    format!("Program {q} invoke [1]"),
                    format!("Program {q} success"),
                ],
                ..Default::default()
            }),
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[test]
fn full_cycle04_retained_close_is_bound_to_observed_invocation() {
    let h = modeled();
    let mut g = h.clone();
    let logs = &h.transactions[0].meta.as_ref().unwrap().log_messages;
    g.transactions[0].meta.as_mut().unwrap().log_messages = vec![
        logs[0].clone(),
        logs[1].clone(),
        "Log truncated".into(),
        logs[3].clone(),
    ];
    assert!(
        eligible(&h, &g),
        "later identical program closes must not make the observed frame ambiguous"
    );
    assert!(!identity::info_equal(
        &h.transactions[0],
        &g.transactions[0]
    ));
    for mode in [
        "no_observed_frame",
        "known_payload",
        "known_close",
        "second_marker",
        "HTTP_unclosed",
        "status_trace_conflict",
    ] {
        let mut h = h.clone();
        let mut g = g.clone();
        let gm = g.transactions[0].meta.as_mut().unwrap();
        match mode {
            "no_observed_frame" => gm.log_messages = vec!["Log truncated".into(), logs[3].clone()],
            "known_payload" => gm.log_messages[1].push('x'),
            "known_close" => gm.log_messages[3].push('x'),
            "second_marker" => gm.log_messages.push("Log truncated".into()),
            "HTTP_unclosed" => {
                h.transactions[0].meta.as_mut().unwrap().log_messages.pop();
            }
            "status_trace_conflict" => {
                let error =
                    Some(yellowstone_grpc_proto::prelude::TransactionError { err: vec![1] });
                gm.err = error.clone();
                h.transactions[0].meta.as_mut().unwrap().err = error;
            }
            _ => unreachable!(),
        }
        assert!(!eligible(&h, &g), "accepted {mode}");
    }
}

#[test]
fn full_cycle04_failed_completion_preserves_failed_execution() {
    use yellowstone_grpc_proto::prelude::TransactionError;
    let mut h = modeled();
    let hm = h.transactions[0].meta.as_mut().unwrap();
    hm.log_messages.truncate(4);
    hm.log_messages[3] =
        hm.log_messages[3].replace("success", "failed: custom program error: 0x1774");
    hm.err = Some(TransactionError { err: vec![1] });
    let mut g = h.clone();
    g.transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .log_messages
        .truncate(2);
    g.transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .log_messages
        .push("Log truncated".into());
    assert!(eligible(&h, &g));
    assert_eq!(
        h.transactions[0].meta.as_ref().unwrap().err,
        g.transactions[0].meta.as_ref().unwrap().err
    );
    h.transactions[0].meta.as_mut().unwrap().err = None;
    assert!(!eligible(&h, &g));
}
