use super::jupiter_raydium_fixture as f;
use crate::source::yellowstone_facts::{
    decode_yellowstone_swap_facts, DecodeMiss, YellowstoneSwapFacts,
};
use std::collections::HashSet;
use yellowstone_grpc_proto::prelude::*;

fn decode(tx: &SubscribeUpdateTransaction) -> Option<YellowstoneSwapFacts> {
    let ray = HashSet::from([f::AMM.to_owned()]);
    decode_yellowstone_swap_facts(tx, &ray, &ray, &HashSet::new())
        .facts
        .unwrap()
}
fn info(tx: &mut SubscribeUpdateTransaction) -> &mut SubscribeUpdateTransactionInfo {
    tx.transaction.as_mut().unwrap()
}
fn message(tx: &mut SubscribeUpdateTransaction) -> &mut Message {
    info(tx)
        .transaction
        .as_mut()
        .unwrap()
        .message
        .as_mut()
        .unwrap()
}
fn meta(tx: &mut SubscribeUpdateTransaction) -> &mut TransactionStatusMeta {
    info(tx).meta.as_mut().unwrap()
}
fn route(tx: &mut SubscribeUpdateTransaction) -> &mut InnerInstructions {
    meta(tx)
        .inner_instructions
        .iter_mut()
        .find(|g| g.index == f::ROUTE_INDEX as u32)
        .unwrap()
}
fn known(tx: &SubscribeUpdateTransaction, input: u64) {
    let trade = decode(tx).expect("complete model custody should be exact");
    assert_eq!(trade.token_in, f::SOL);
    assert_eq!(trade.token_out, f::MINT);
    assert_eq!(trade.dex_hint, "raydium");
    let exact = trade.exact_amounts.expect("confirmed custody raw operands");
    assert_eq!(exact.amount_in_raw, input.to_string());
    assert_eq!(exact.amount_in_decimals, 9);
    assert_eq!(exact.amount_out_raw, f::OUTPUT.to_string());
    assert_eq!(exact.amount_out_decimals, 9);
}

#[test]
fn jupiter_raydium_complete_wrap_cpi_and_target_creation_are_exact() {
    for fresh in [false, true] {
        for event in [false, true] {
            for tag in [7, 105] {
                known(&f::buy(fresh, event, tag, f::INPUT), f::INPUT);
            }
        }
    }
    // A leader's exact custody is not capped at the follower's financial limit.
    known(&f::buy(true, true, 7, 20_000_000), 20_000_000);
}
#[test]
fn jupiter_raydium_wrong_wallet_temp_owner_raw_vault_and_token2022_refuse() {
    let original = f::buy(true, true, 7, f::INPUT);
    let mut tx = original.clone();
    message(&mut tx).account_keys[0] = vec![74; 32];
    assert!(decode(&tx).is_none());
    let mut tx = original.clone();
    meta(&mut tx).inner_instructions[0].instructions[3].data[1] ^= 1;
    assert!(decode(&tx).is_none());
    let mut tx = original.clone();
    route(&mut tx).instructions[1].data[1] ^= 1;
    assert!(decode(&tx).is_none());
    let mut tx = original.clone();
    route(&mut tx).instructions[1].accounts[1] = 14;
    assert!(decode(&tx).is_none());
    let mut tx = original.clone();
    meta(&mut tx).post_token_balances[1].owner = f::MINT.into();
    assert!(decode(&tx).is_none());
    let mut tx = original;
    meta(&mut tx).post_token_balances[0].program_id =
        "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb".into();
    assert!(decode(&tx).is_none());
}
#[test]
fn jupiter_raydium_missing_witness_never_turns_native_debit_or_fee_into_swap() {
    let original = f::buy(true, true, 7, f::INPUT);
    let mut tx = original.clone();
    meta(&mut tx).inner_instructions[0].instructions.remove(0);
    assert!(decode(&tx).is_none());
    let mut tx = original.clone();
    meta(&mut tx).inner_instructions[1].instructions.pop();
    assert!(decode(&tx).is_none());
    let mut tx = original.clone();
    meta(&mut tx)
        .inner_instructions
        .retain(|g| g.index == f::ROUTE_INDEX as u32);
    assert!(decode(&tx).is_none());
    let mut tx = original.clone();
    meta(&mut tx).inner_instructions.clear();
    assert!(decode(&tx).is_none());
    let mut tx = original;
    meta(&mut tx).post_balances[0] = 999_981_000;
    meta(&mut tx).inner_instructions_none = true;
    assert!(decode(&tx).is_none());
}
#[test]
fn jupiter_raydium_other_routes_events_and_message_config_refuse() {
    let original = f::buy(true, true, 7, f::INPUT);
    let mut tx = original.clone();
    message(&mut tx).instructions[f::ROUTE_INDEX].data[12] = 8;
    assert!(decode(&tx).is_none());
    let mut tx = original.clone();
    let duplicate = message(&mut tx).instructions[f::ROUTE_INDEX].clone();
    message(&mut tx).instructions.push(duplicate);
    assert!(decode(&tx).is_none());
    let mut tx = original.clone();
    let duplicate = route(&mut tx).instructions[0].clone();
    route(&mut tx).instructions.push(duplicate);
    assert!(decode(&tx).is_none());
    let mut tx = original.clone();
    route(&mut tx).instructions[3].data[120] ^= 1;
    assert!(decode(&tx).is_none());
    let mut tx = original.clone();
    route(&mut tx).instructions[3].accounts[0] = 11;
    assert!(decode(&tx).is_none());
    for versioned in [false, true] {
        let mut tx = original.clone();
        message(&mut tx).versioned = versioned;
        message(&mut tx).config = Some(TransactionConfig::default());
        let ray = HashSet::from([f::AMM.to_owned()]);
        let rejected = decode_yellowstone_swap_facts(&tx, &ray, &ray, &HashSet::new());
        assert!(rejected.facts.unwrap().is_none());
        assert_eq!(rejected.miss, Some(DecodeMiss::UnsupportedMessageConfig));
    }
}
