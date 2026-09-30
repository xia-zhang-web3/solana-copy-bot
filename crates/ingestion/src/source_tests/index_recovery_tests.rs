//! Execution-index correction only; the original vectors remain evidence.
use super::{identity, identity_difference};
use yellowstone_grpc_proto::prelude::*;

fn block() -> SubscribeUpdateBlock {
    SubscribeUpdateBlock {
        slot: 10,
        executed_transaction_count: 3,
        transactions: (0..3)
            .map(|index| {
                let signature = vec![index as u8 + 1; 64];
                SubscribeUpdateTransactionInfo {
                    signature: signature.clone(),
                    index,
                    transaction: Some(Transaction {
                        signatures: vec![signature],
                        message: Some(Message {
                            config: Some(TransactionConfig {
                                priority_fee: Some(0),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                    }),
                    meta: Some(TransactionStatusMeta {
                        fee: index,
                        pre_token_balances: vec![TokenBalance {
                            ui_token_amount: Some(UiTokenAmount {
                                ui_amount: -0.0,
                                ..Default::default()
                            }),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                    ..Default::default()
                }
            })
            .collect(),
        ..Default::default()
    }
}

#[test]
fn index_recovery_permutation_aligns_all_fields_without_mutating_vectors() {
    let http = block();
    let mut grpc = http.clone();
    grpc.transactions.rotate_right(1);
    let original = grpc.clone();
    assert!(identity::block_equivalent(&grpc, &http));
    assert!(identity_difference::first(&grpc, &http).is_none());
    assert_eq!(grpc, original);
    assert_eq!(
        grpc.transactions
            .iter()
            .map(|i| i.index)
            .collect::<Vec<_>>(),
        vec![2, 0, 1]
    );
    let mut changed = http.clone();
    changed.transactions[2].meta.as_mut().unwrap().fee += 1;
    assert!(!identity::block_equivalent(&grpc, &changed));
    let difference = identity_difference::first(&grpc, &changed).unwrap();
    assert_eq!(difference.path, "transactions[2].meta.fee");
    assert_eq!(difference.grpc, 2);
    assert_eq!(difference.http, 3);
}

#[test]
fn index_recovery_invalid_both_side_integrity_is_fail_closed_and_diagnosed() {
    let valid = block();
    let cases: Vec<(&str, fn(&mut SubscribeUpdateBlock))> = vec![
        ("transactions.delivery[1].index", |b| {
            b.transactions[1].index = 0
        }),
        ("transactions.delivery[0].index", |b| {
            b.transactions[0].index = u64::MAX
        }),
        ("transactions.length", |b| {
            b.transactions.pop();
        }),
        ("executed_transaction_count", |b| {
            b.executed_transaction_count += 1
        }),
        ("transactions.delivery[0].signature_binding", |b| {
            b.transactions[0].signature[0] ^= 1
        }),
        ("transactions.delivery[0].signature_binding", |b| {
            b.transactions[0]
                .transaction
                .as_mut()
                .unwrap()
                .signatures
                .clear();
        }),
        ("transactions.delivery[0].signature_binding", |b| {
            b.transactions[0].transaction = None;
        }),
        ("transactions.delivery[0].signature_binding", |b| {
            b.transactions[0].signature.pop();
            b.transactions[0].transaction.as_mut().unwrap().signatures[0].pop();
        }),
    ];
    for (path, edit) in cases {
        let mut invalid = valid.clone();
        edit(&mut invalid);
        for (grpc, http) in [(&invalid, &valid), (&valid, &invalid), (&invalid, &invalid)] {
            assert!(!identity::block_equivalent(grpc, http), "{path}");
            let difference = identity_difference::first(grpc, http).unwrap();
            // Equal malformed counts are rejected by local count validation,
            // while differing header counts keep the original first path.
            assert_eq!(
                difference.path,
                if path == "executed_transaction_count" && grpc == http {
                    "transactions.length"
                } else {
                    path
                }
            );
            assert!(!difference.grpc.is_null() && !difference.http.is_null());
        }
    }
    let mut other = valid.clone();
    other.transactions[1].signature[0] = 9;
    other.transactions[1]
        .transaction
        .as_mut()
        .unwrap()
        .signatures[0][0] = 9;
    assert!(!identity::block_equivalent(&valid, &other));
    assert_eq!(
        identity_difference::first(&valid, &other).unwrap().path,
        "transactions[1].signature[0]"
    );
}

#[test]
fn index_recovery_non_order_predicates_and_signed_zero_remain_exact() {
    let mut grpc = block();
    grpc.transactions.rotate_right(1);
    let http = block();
    let cases: Vec<(&str, fn(&mut SubscribeUpdateBlock))> = vec![
        ("parent_blockhash", |b| b.parent_blockhash.push('x')),
        ("rewards.rewards.length", |b| {
            b.rewards = Some(Rewards {
                rewards: vec![Reward::default()],
                ..Default::default()
            })
        }),
        (
            "transactions[1].transaction.message.config.priority_fee.presence",
            |b| {
                b.transactions[1]
                    .transaction
                    .as_mut()
                    .unwrap()
                    .message
                    .as_mut()
                    .unwrap()
                    .config
                    .as_mut()
                    .unwrap()
                    .priority_fee = None;
            },
        ),
        ("transactions[1].meta.presence", |b| {
            b.transactions[1].meta = None
        }),
        (
            "transactions[1].meta.pre_token_balances[0].ui_token_amount.ui_amount",
            |b| {
                b.transactions[1].meta.as_mut().unwrap().pre_token_balances[0]
                    .ui_token_amount
                    .as_mut()
                    .unwrap()
                    .ui_amount = 0.0;
            },
        ),
    ];
    for (path, edit) in cases {
        let mut changed = http.clone();
        edit(&mut changed);
        assert!(!identity::block_equivalent(&grpc, &changed));
        let difference = identity_difference::first(&grpc, &changed).unwrap();
        assert_eq!(difference.path, path);
        if path.ends_with("ui_amount") {
            assert_eq!(difference.grpc["bits"], "0x8000000000000000");
            assert_eq!(difference.http["bits"], "0x0000000000000000");
        }
    }
    // Preserve the established None -> default Rewards translation.
    let mut explicit = http.clone();
    explicit.rewards = Some(Rewards::default());
    assert!(identity::block_equivalent(&grpc, &explicit));
}
