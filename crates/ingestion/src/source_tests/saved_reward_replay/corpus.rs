//! Opt-in private evidence: exact sealed RPC bodies, never copied into Git.
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::Path};
use yellowstone_grpc_proto::prelude::*;

pub(super) const FIRST: u64 = 451_591_982;
pub(super) const LAST: u64 = 451_591_985;
const META_SEALS: [(u8, &str); 5] = [
    (
        1,
        "d89bc4c891a74dd671eb7f4a7de1be41e3092d11858e525727b787358dd2b54e",
    ),
    (
        2,
        "7db6a0c6c94d92aca5dd20c768c3c2426254999f56094e4a0b80e6155a87d8bf",
    ),
    (
        3,
        "bd7b5edcb7b1f7b8402e3a4e6a2361a1bf78138fb7cd07eb5e5161fa24aa1a8e",
    ),
    (
        4,
        "d8281f588e10e40e635d03cfb74708622eb535c6c6329d5434038e65f81c3437",
    ),
    (
        5,
        "8c5cb271410984e741594bcb8cd6acba196be5ece5700e6b117fbb6577cb0f82",
    ),
];
pub(super) struct Corpus {
    pub list: Vec<u8>,
    pub bodies: BTreeMap<u64, Vec<u8>>,
}
fn sha(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub(super) fn load(path: &Path) -> Corpus {
    let mut corpus = Corpus {
        list: vec![],
        bodies: BTreeMap::new(),
    };
    for (index, seal) in META_SEALS {
        let base = format!("response-{index:06}");
        let metadata = std::fs::read(path.join(format!("{base}.meta.json"))).unwrap();
        assert_eq!(sha(&metadata), seal, "private metadata seal changed");
        let metadata: Value = serde_json::from_slice(&metadata).unwrap();
        let body = std::fs::read(path.join(format!("{base}.json"))).unwrap();
        assert_eq!(sha(&body), metadata["response_sha256"].as_str().unwrap());
        assert_eq!(body.len() as u64, metadata["bytes"].as_u64().unwrap());
        assert_eq!(metadata["status"], 200);
        let response: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(response["id"], metadata["request"]["id"]);
        if metadata["method"] == "getBlocks" {
            corpus.list = body;
        } else {
            assert_eq!(metadata["method"], "getBlock");
            assert_eq!(metadata["request"]["params"][1]["rewards"], true);
            assert!(corpus
                .bodies
                .insert(metadata["request"]["params"][0].as_u64().unwrap(), body)
                .is_none());
        }
    }
    assert_eq!(
        corpus.bodies.keys().copied().collect::<Vec<_>>(),
        (FIRST..=LAST).collect::<Vec<_>>()
    );
    corpus
}
pub(super) fn result(body: &[u8]) -> Value {
    serde_json::from_slice::<Value>(body).unwrap()["result"].clone()
}
fn reward_facts(raw: &Value, rewards: &[Reward]) {
    let rows = raw.as_array().map(Vec::as_slice).unwrap_or(&[]);
    assert_eq!(rows.len(), rewards.len());
    for (row, reward) in rows.iter().zip(rewards) {
        assert_eq!(reward.pubkey, row["pubkey"].as_str().unwrap());
        assert_eq!(reward.lamports, row["lamports"].as_i64().unwrap());
        assert_eq!(reward.post_balance, row["postBalance"].as_u64().unwrap());
        for (name, actual) in [
            ("commission", &reward.commission),
            ("commissionBps", &reward.commission_bps),
        ] {
            let expected = row[name]
                .as_u64()
                .map(|v| v.to_string())
                .unwrap_or_default();
            assert_eq!(*actual, expected);
        }
    }
}
pub(super) fn financial_facts(raw: &Value, parsed: &SubscribeUpdateBlock) {
    let rows = raw["transactions"].as_array().unwrap();
    assert_eq!(parsed.executed_transaction_count as usize, rows.len());
    assert_eq!(parsed.transactions.len(), rows.len());
    reward_facts(&raw["rewards"], &parsed.rewards.as_ref().unwrap().rewards);
    assert!(parsed
        .rewards
        .as_ref()
        .unwrap()
        .rewards
        .iter()
        .all(|r| r.reward_type == RewardType::Fee as i32));
    for (row, tx) in rows.iter().zip(&parsed.transactions) {
        let meta = tx.meta.as_ref().unwrap();
        assert_eq!(meta.fee, row["meta"]["fee"].as_u64().unwrap());
        for (name, actual) in [
            ("preBalances", &meta.pre_balances),
            ("postBalances", &meta.post_balances),
        ] {
            let expected: Vec<_> = row["meta"][name]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_u64().unwrap())
                .collect();
            assert_eq!(*actual, expected);
        }
        reward_facts(&row["meta"]["rewards"], &meta.rewards);
        for (name, actual) in [
            ("preTokenBalances", &meta.pre_token_balances),
            ("postTokenBalances", &meta.post_token_balances),
        ] {
            let rows = row["meta"][name]
                .as_array()
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            assert_eq!(rows.len(), actual.len());
            for (raw, parsed) in rows.iter().zip(actual) {
                let amount = parsed.ui_token_amount.as_ref().unwrap();
                assert_eq!(
                    amount.amount,
                    raw["uiTokenAmount"]["amount"].as_str().unwrap()
                );
                assert_eq!(
                    amount.decimals as u64,
                    raw["uiTokenAmount"]["decimals"].as_u64().unwrap()
                );
            }
        }
    }
}
