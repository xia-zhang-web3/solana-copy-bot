//! Byte-pinned LOCAL pair; IEEE witnesses use delivery position before execution mapping.
use crate::source::http_recovery::normalize_confirmed_http_block;
use prost::Message;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fs, path::PathBuf};
use yellowstone_grpc_proto::prelude::{SubscribeUpdateBlock, SubscribeUpdateTransactionInfo};

pub(super) struct Pair {
    pub(super) grpc: SubscribeUpdateBlock,
    pub(super) http: SubscribeUpdateBlock,
    pub(super) manifest: Value,
    pub(super) raw_http: Vec<u8>,
    pub(super) root: PathBuf,
}

fn bits(block: &SubscribeUpdateBlock) -> BTreeMap<(u32, u8, u32), u64> {
    let mut out = BTreeMap::new();
    for (position, info) in block.transactions.iter().enumerate() {
        if let Some(meta) = &info.meta {
            for (side, rows) in [
                (0u8, &meta.pre_token_balances),
                (1, &meta.post_token_balances),
            ] {
                for (row, token) in rows.iter().enumerate() {
                    if let Some(amount) = &token.ui_token_amount {
                        assert!(out
                            .insert(
                                (position as u32, side, row as u32),
                                amount.ui_amount.to_bits()
                            )
                            .is_none());
                    }
                }
            }
        }
    }
    out
}

fn sidecar(bytes: &[u8]) -> BTreeMap<(u32, u8, u32), u64> {
    assert_eq!(bytes.len() % 17, 0);
    let mut out = BTreeMap::new();
    for record in bytes.chunks_exact(17) {
        let position = u32::from_le_bytes(record[0..4].try_into().unwrap());
        let side = record[4];
        let row = u32::from_le_bytes(record[5..9].try_into().unwrap());
        let value = u64::from_le_bytes(record[9..17].try_into().unwrap());
        assert!(side <= 1);
        assert!(out.insert((position, side, row), value).is_none());
    }
    out
}

fn restore(block: &mut SubscribeUpdateBlock, authoritative: &BTreeMap<(u32, u8, u32), u64>) {
    assert_eq!(
        bits(block).keys().collect::<Vec<_>>(),
        authoritative.keys().collect::<Vec<_>>()
    );
    for (&(position, side, row), &value) in authoritative {
        let meta = block.transactions[position as usize].meta.as_mut().unwrap();
        let rows = if side == 0 {
            &mut meta.pre_token_balances
        } else {
            &mut meta.post_token_balances
        };
        rows[row as usize]
            .ui_token_amount
            .as_mut()
            .unwrap()
            .ui_amount = f64::from_bits(value);
    }
    assert_eq!(&bits(block), authoritative);
}

pub(super) fn execution_order(
    block: &SubscribeUpdateBlock,
) -> Vec<&SubscribeUpdateTransactionInfo> {
    assert_eq!(
        block.executed_transaction_count as usize,
        block.transactions.len()
    );
    let mut order = BTreeMap::new();
    for info in &block.transactions {
        assert_eq!(
            info.transaction.as_ref().unwrap().signatures.first(),
            Some(&info.signature)
        );
        assert!(order.insert(info.index, info).is_none());
    }
    assert!(order
        .keys()
        .enumerate()
        .all(|(i, index)| *index == i as u64));
    order.into_values().collect()
}

pub(super) fn load() -> Pair {
    let root = PathBuf::from(std::env::var("COPYBOT_ANCHOR_PAIR_DIR").unwrap());
    let manifest: Value =
        serde_json::from_slice(&fs::read(root.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["complete"], true);
    let read = |name: &str| {
        let bytes = fs::read(root.join(name)).unwrap();
        assert!(bytes.len() <= 16 << 20);
        assert_eq!(
            bytes.len() as u64,
            manifest["files"][name]["bytes"].as_u64().unwrap()
        );
        assert_eq!(
            format!("{:x}", Sha256::digest(&bytes)),
            manifest["files"][name]["sha256"].as_str().unwrap()
        );
        bytes
    };
    let mut grpc = SubscribeUpdateBlock::decode(read("grpc_typed.pb").as_slice()).unwrap();
    restore(&mut grpc, &sidecar(&read("grpc_ui_amount_bits.bin")));
    let raw = read("http_response.json");
    assert_eq!(read("http_attempt_1.json"), raw);
    let response: Value = serde_json::from_slice(&raw).unwrap();
    let http =
        normalize_confirmed_http_block(manifest["slot"].as_u64().unwrap(), &response["result"])
            .unwrap();
    let http_bits = sidecar(&read("http_ui_amount_bits.bin"));
    // The real JSON normalizer must reproduce these bits itself; no HTTP repair/retag.
    assert_eq!(bits(&http), http_bits);
    let mut saved_http =
        SubscribeUpdateBlock::decode(read("http_normalized.pb").as_slice()).unwrap();
    restore(&mut saved_http, &http_bits);
    assert_eq!(http, saved_http);
    assert_eq!(grpc.slot, http.slot);
    assert_eq!(
        grpc.transactions.len() as u64,
        manifest["transactions"].as_u64().unwrap()
    );
    Pair {
        grpc,
        http,
        manifest,
        raw_http: raw,
        root,
    }
}
