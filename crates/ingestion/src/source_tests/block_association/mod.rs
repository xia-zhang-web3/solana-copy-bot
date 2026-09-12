use super::{cases, config, transaction};
use crate::source::yellowstone_block_association::{
    associate_yellowstone_transaction, AssociatedSwap, AssociationRefusal as Refusal, InfoSide,
    ProviderBlockTime as Time, MAX_BLOCK_TRANSACTIONS, MAX_INFO_BYTES, MAX_INFO_ITEMS,
};
use crate::source::yellowstone_facts::{decode_yellowstone_swap_facts, YellowstoneSwapFacts};
use crate::source::YellowstoneGrpcSource;
use anyhow::Result;
use prost::Message;
use serde_json::{json, Value};
use yellowstone_grpc_proto::prelude::{
    subscribe_update, SubscribeUpdate, SubscribeUpdateBlock, SubscribeUpdateTransaction,
    SubscribeUpdateTransactionInfo, UnixTimestamp,
};

mod bounds;
mod delivery_tests;
mod fields;
mod identity;
mod parent_tests;
mod time;

fn tx_mut(update: &mut SubscribeUpdate) -> &mut SubscribeUpdateTransaction {
    match update.update_oneof.as_mut().unwrap() {
        subscribe_update::UpdateOneof::Transaction(tx) => tx,
        _ => panic!("transaction fixture required"),
    }
}

fn pair(sell: bool, native: bool) -> Result<(SubscribeUpdate, SubscribeUpdateBlock)> {
    let mut expected = cases::base(sell, native);
    expected.created_at = cases::times()[0].1;
    let expected = super::replay(&format!("{sell}-{native}-healthy"), expected)?;
    let tx = transaction(&expected);
    let block = SubscribeUpdateBlock {
        slot: tx.slot,
        blockhash: bs58::encode([80u8; 32]).into_string(),
        block_time: Some(UnixTimestamp {
            timestamp: 1788868700,
        }),
        transactions: vec![tx.transaction.as_ref().unwrap().clone()],
        executed_transaction_count: 100_000,
        parent_slot: tx.slot - 1,
        parent_blockhash: bs58::encode([79u8; 32]).into_string(),
        ..Default::default()
    };
    Ok((expected, block))
}

fn facts_json(f: &YellowstoneSwapFacts) -> Value {
    json!({"signature":f.signature,"slot":f.slot,"signer":f.signer,
        "token_in":f.token_in,"token_out":f.token_out,"amount_in":f.amount_in,
        "amount_out":f.amount_out,"amount_in_bits":f.amount_in.to_bits(),
        "amount_out_bits":f.amount_out.to_bits(),"exact_amounts":f.exact_amounts,
        "program_ids":f.program_ids,"dex_hint":f.dex_hint})
}

fn canonical(mut value: Value) -> Value {
    value["program_ids"]
        .as_array_mut()
        .unwrap()
        .sort_by_key(Value::to_string);
    value
}

fn result_json(result: &std::result::Result<AssociatedSwap, Refusal>) -> Value {
    match result {
        Err(error) => json!({"refusal":format!("{error:?}")}),
        Ok(value) => json!({"facts":facts_json(&value.facts),
            "provider_assertion":{"slot":value.provider_assertion.slot,
                "blockhash":value.provider_assertion.blockhash,
                "signature":bs58::encode(value.provider_assertion.signature).into_string(),
                "transaction_index":value.provider_assertion.transaction_index},
            "provider_block_time":format!("{:?}",value.block_time),
            "used_program_fallback":value.used_program_fallback,
            "canonicality":"unproven","finality":"unproven","cryptographic_inclusion":"unproven"}),
    }
}

fn associate(
    expected: &SubscribeUpdate,
    block: &SubscribeUpdateBlock,
) -> std::result::Result<AssociatedSwap, Refusal> {
    let source = YellowstoneGrpcSource::new(&config()).unwrap();
    let c = &source.runtime_config;
    let before = c.telemetry.parse_fallback_by_reason.lock().unwrap().clone();
    let result = associate_yellowstone_transaction(
        transaction(expected),
        block,
        &c.interested_program_ids,
        &c.raydium_program_ids,
        &c.pumpswap_program_ids,
    );
    assert_eq!(
        *c.telemetry.parse_fallback_by_reason.lock().unwrap(),
        before
    );
    result
}

// Capture actual protobuf envelopes before asserting the outcome. Both clocks
// live outside the two borrowed transaction/block inputs accepted by the API.
fn observe(
    name: &str,
    expected: &SubscribeUpdate,
    block: &SubscribeUpdateBlock,
) -> std::result::Result<AssociatedSwap, Refusal> {
    let block_envelope = SubscribeUpdate {
        created_at: expected.created_at,
        update_oneof: Some(subscribe_update::UpdateOneof::Block(block.clone())),
        ..Default::default()
    };
    let expected_bytes = expected.encode_to_vec();
    let block_bytes = block_envelope.encode_to_vec();
    let expected_decoded = SubscribeUpdate::decode(expected_bytes.as_slice()).unwrap();
    let block_decoded = SubscribeUpdate::decode(block_bytes.as_slice()).unwrap();
    assert_eq!(expected_decoded, *expected);
    assert_eq!(block_decoded, block_envelope);
    let Some(subscribe_update::UpdateOneof::Block(block)) = &block_decoded.update_oneof else {
        panic!()
    };
    let result = associate(&expected_decoded, block);
    if let Ok(dir) = std::env::var("B80_CAPTURE_DIR") {
        let dir = std::path::Path::new(&dir);
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join(format!("{name}.expected.pb")), expected_bytes).unwrap();
        std::fs::write(dir.join(format!("{name}.block.pb")), block_bytes).unwrap();
        std::fs::write(
            dir.join(format!("{name}.json")),
            serde_json::to_vec_pretty(&result_json(&result)).unwrap(),
        )
        .unwrap();
    }
    result
}

fn refused(name: &str, expected: &SubscribeUpdate, block: &SubscribeUpdateBlock, reason: Refusal) {
    assert_eq!(
        observe(name, expected, block).unwrap_err(),
        reason,
        "{name}"
    );
}

mod capture_replay;

mod streaming;
