use super::*;
use std::io::Write;
use yellowstone_grpc_proto::prelude::SubscribeUpdatePing;

fn envelope(
    block: SubscribeUpdateBlock,
    created_at: Option<yellowstone_grpc_proto::prost_types::Timestamp>,
) -> SubscribeUpdate {
    SubscribeUpdate {
        update_oneof: Some(subscribe_update::UpdateOneof::Block(block)),
        created_at,
        ..Default::default()
    }
}
fn emit(
    root: &Path,
    name: &str,
    messages: Vec<SubscribeUpdate>,
    expected: &[&str],
    cases: &mut Vec<Value>,
) {
    let dir = root.join(name);
    std::fs::create_dir(&dir).unwrap();
    let mut inputs = vec![];
    for (i, msg) in messages.into_iter().enumerate() {
        let bytes = msg.encode_to_vec();
        let file = format!("{:06}.pb", i + 1);
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(dir.join(&file))
            .unwrap()
            .write_all(&bytes)
            .unwrap();
        inputs.push(json!({"file":file,"bytes":bytes.len(),
            "created_at":msg.created_at.map(|t| json!({"seconds":t.seconds,"nanos":t.nanos}))}));
    }
    write_json(&dir.join("inputs.json"), &json!(inputs));
    cases.push(json!({"name":name,"expected":expected}));
}
fn base() -> (SubscribeUpdate, SubscribeUpdateBlock) {
    pair(false, true).unwrap()
}
pub(super) fn generate(root: &Path) {
    std::fs::create_dir(root).unwrap();
    let mut cases = vec![];
    for sell in [false, true] {
        for native in [false, true] {
            for block_first in [false, true] {
                let (tx, block) = pair(sell, native).unwrap();
                let b = envelope(block, tx.created_at);
                let name = format!(
                    "{}-{}-{}",
                    if sell { "sell" } else { "buy" },
                    if native { "native" } else { "wsol" },
                    if block_first {
                        "block-first"
                    } else {
                        "tx-first"
                    }
                );
                let mut msgs = vec![SubscribeUpdate {
                    update_oneof: Some(subscribe_update::UpdateOneof::Ping(SubscribeUpdatePing {})),
                    ..Default::default()
                }];
                if block_first {
                    msgs.extend([b, tx]);
                } else {
                    msgs.extend([tx, b]);
                }
                emit(root, &name, msgs, &["matched"], &mut cases);
            }
        }
    }
    for (name, time) in [
        ("created-missing", None),
        (
            "created-invalid",
            Some(yellowstone_grpc_proto::prost_types::Timestamp {
                seconds: 1788868800,
                nanos: -1,
            }),
        ),
    ] {
        let (mut tx, b) = base();
        tx.created_at = time;
        emit(
            root,
            name,
            vec![tx, envelope(b, time)],
            &["matched"],
            &mut cases,
        );
    }
    for (name, time) in [
        ("block-time-missing", None),
        (
            "block-time-invalid",
            Some(UnixTimestamp {
                timestamp: i64::MAX,
            }),
        ),
        ("block-time-zero", Some(UnixTimestamp { timestamp: 0 })),
    ] {
        let (tx, mut b) = base();
        b.block_time = time;
        emit(
            root,
            name,
            vec![tx.clone(), envelope(b, tx.created_at)],
            &["matched"],
            &mut cases,
        );
    }
    for name in [
        "info-mismatch",
        "filtered-absence",
        "duplicate-identical",
        "duplicate-conflicting",
    ] {
        let (tx, mut b) = base();
        match name {
            "info-mismatch" => b.transactions[0].index += 1,
            "filtered-absence" => b.transactions.clear(),
            "duplicate-identical" => b.transactions.push(b.transactions[0].clone()),
            _ => {
                let mut other = b.transactions[0].clone();
                other.index += 1;
                b.transactions.push(other);
            }
        }
        emit(
            root,
            name,
            vec![tx.clone(), envelope(b, tx.created_at)],
            &[if name == "filtered-absence" {
                "unmatched"
            } else {
                "refused"
            }],
            &mut cases,
        );
    }
    let (tx, b) = base();
    emit(
        root,
        "repeated-signature",
        vec![tx.clone(), envelope(b.clone(), tx.created_at), tx.clone()],
        &["matched", "matched"],
        &mut cases,
    );
    let mut fork = b.clone();
    fork.blockhash = bs58::encode([81u8; 32]).into_string();
    emit(
        root,
        "conflicting-blockhash",
        vec![
            tx.clone(),
            envelope(b, tx.created_at),
            envelope(fork, tx.created_at),
        ],
        &["unknown"],
        &mut cases,
    );
    let (mut a, _) = base();
    tx_mut(&mut a).transaction.as_mut().unwrap().signature = vec![99; 64];
    let (b, block) = pair(true, false).unwrap();
    emit(
        root,
        "unmatched-a-healthy-b",
        vec![a, b.clone(), envelope(block, b.created_at)],
        &["unmatched", "matched"],
        &mut cases,
    );
    let (mut bad, _) = base();
    tx_mut(&mut bad).transaction.as_mut().unwrap().meta = None;
    let (b, block) = pair(true, true).unwrap();
    emit(
        root,
        "non-swap-denominator",
        vec![bad, b.clone(), envelope(block, b.created_at)],
        &["not_checked_swap", "matched"],
        &mut cases,
    );
    emit(root, "missing-block", vec![tx], &["unmatched"], &mut cases);
    write_json(&root.join("scenarios.json"), &json!(cases));
}
