//! Preserved31 historical layout through the real RPC normalizer and gRPC decoder.
//! Counterfactual mutations are mocks. This is never a fresh native admission.
use crate::source::{
    http_recovery::normalize_confirmed_http_block, yellowstone_facts::decode_yellowstone_swap_facts,
};
use serde_json::{json, Value};
use std::collections::HashSet;
use yellowstone_grpc_proto::prelude::*;
const AMM: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../storage-core/tests/fixtures/canary31-source-sell-raw.json"
    ))
    .unwrap()
}
fn proto(t: &Value) -> SubscribeUpdateTransaction {
    let slot = t["slot"].as_u64().unwrap();
    let block = json!({"blockhash":bs58::encode([2;32]).into_string(),"previousBlockhash":bs58::encode([3;32]).into_string(),
        "parentSlot":slot-1,"blockHeight":slot,"blockTime":t["blockTime"],"transactions":[t],"rewards":[]});
    let b = normalize_confirmed_http_block(slot, &block).unwrap();
    SubscribeUpdateTransaction {
        slot,
        transaction: Some(b.transactions[0].clone()),
        ..Default::default()
    }
}
fn decode(
    t: &SubscribeUpdateTransaction,
) -> Option<crate::source::yellowstone_facts::YellowstoneSwapFacts> {
    let ray = HashSet::from([AMM.into()]);
    decode_yellowstone_swap_facts(t, &ray, &ray, &HashSet::new())
        .facts
        .unwrap()
}
#[test]
fn controlled_jupiter_saved31_sell_is_exact_and_retains_historical_slot() {
    let t = fixture();
    let p = proto(&t);
    let f = decode(&p).expect("new closed SELL grammar");
    assert_eq!(f.slot, 455_694_767);
    assert_eq!(f.signature, t["transaction"]["signatures"][0]);
    assert_eq!(f.token_in, MINT);
    assert_eq!(f.token_out, "So11111111111111111111111111111111111111112");
    let e = f.exact_amounts.unwrap();
    assert_eq!(e.amount_in_raw, "1091636");
    assert_eq!(e.amount_out_raw, "9948805");
    assert_eq!(e.amount_in_decimals, 6);
    assert_eq!(e.amount_out_decimals, 9);
}
#[test]
fn controlled_jupiter_saved31_buy_binds_swap_event_to_program_and_exact_amounts() {
    let t: Value = serde_json::from_str(include_str!(
        "../../../storage-core/tests/fixtures/canary31-source-buy-raw.json"
    ))
    .unwrap();
    let f = decode(&proto(&t)).expect("actual BUY event carries AMM program, not pool address");
    assert_eq!(f.slot, 455_694_684);
    assert_eq!(f.token_out, MINT);
    let e = f.exact_amounts.unwrap();
    assert_eq!(e.amount_in_raw, "10000000");
    assert_eq!(e.amount_out_raw, "1091636");
}
#[test]
fn controlled_jupiter_source_sell_missing_or_conflicting_witness_is_terminal() {
    let original = proto(&fixture());
    let mut p = original.clone();
    p.transaction
        .as_mut()
        .unwrap()
        .meta
        .as_mut()
        .unwrap()
        .inner_instructions
        .remove(0);
    assert!(decode(&p).is_none());
    let mut p = original.clone();
    p.transaction
        .as_mut()
        .unwrap()
        .transaction
        .as_mut()
        .unwrap()
        .message
        .as_mut()
        .unwrap()
        .instructions[4]
        .accounts[1] = 2;
    assert!(decode(&p).is_none());
    let mut p = original.clone();
    p.transaction
        .as_mut()
        .unwrap()
        .meta
        .as_mut()
        .unwrap()
        .inner_instructions[1]
        .instructions[1]
        .data[1] ^= 1;
    assert!(decode(&p).is_none());
    let mut p = original.clone();
    p.transaction
        .as_mut()
        .unwrap()
        .transaction
        .as_mut()
        .unwrap()
        .message
        .as_mut()
        .unwrap()
        .header
        .as_mut()
        .unwrap()
        .num_required_signatures = 2;
    assert!(decode(&p).is_none());
    let mut p = original;
    p.transaction
        .as_mut()
        .unwrap()
        .meta
        .as_mut()
        .unwrap()
        .post_balances[0] += 1;
    assert!(decode(&p).is_none());
}
