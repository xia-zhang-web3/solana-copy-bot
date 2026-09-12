use super::{config, fixtures, inventory, SwapParser, PUMP, RAY};
use crate::source::{
    yellowstone, RawSwapObservation, YellowstoneGrpcSource, YellowstoneParsedUpdate,
};
use anyhow::Result;
use prost::Message;
use serde_json::{json, Value};
use yellowstone_grpc_proto::prelude::{
    subscribe_update, SubscribeUpdate, SubscribeUpdateTransaction,
};

mod boundary;
mod cases;
mod damage;

fn transaction(update: &SubscribeUpdate) -> &SubscribeUpdateTransaction {
    match update.update_oneof.as_ref().unwrap() {
        subscribe_update::UpdateOneof::Transaction(tx) => tx,
        _ => panic!("transaction required"),
    }
}

fn raw_fields(raw: &RawSwapObservation) -> Value {
    json!({"signature":raw.signature,"slot":raw.slot,"signer":raw.signer,
        "token_in":raw.token_in,"token_out":raw.token_out,
        "amount_in":raw.amount_in,"amount_out":raw.amount_out,
        "amount_in_bits":raw.amount_in.to_bits(),"amount_out_bits":raw.amount_out.to_bits(),
        "exact_amounts":raw.exact_amounts,"program_ids":raw.program_ids,
        "dex_hint":raw.dex_hint,"ts_utc":raw.ts_utc})
}

fn legacy(update: SubscribeUpdate, config: &crate::source::YellowstoneRuntimeConfig) -> Value {
    match yellowstone::parse_yellowstone_update(update, config) {
        Err(error) => json!({"error":format!("{error:#}")}),
        Ok(None) => json!({"none":true}),
        Ok(Some(YellowstoneParsedUpdate::Ping)) => json!({"ping":true}),
        Ok(Some(YellowstoneParsedUpdate::Observation(raw))) => {
            let fields = raw_fields(&raw);
            let event = SwapParser::new(vec![RAY.into()], vec![PUMP.into()]).parse(raw);
            json!({"raw":fields,"event":event})
        }
    }
}

fn capture(name: &str, update: &SubscribeUpdate, result: &Value) -> Result<()> {
    if let Ok(dir) = std::env::var("B79_CAPTURE_DIR") {
        let dir = std::path::Path::new(&dir);
        std::fs::create_dir_all(dir)?;
        std::fs::write(dir.join(format!("{name}.pb")), update.encode_to_vec())?;
        std::fs::write(
            dir.join(format!("{name}.json")),
            serde_json::to_vec_pretty(result)?,
        )?;
    }
    Ok(())
}

fn replay(name: &str, generated: SubscribeUpdate) -> Result<SubscribeUpdate> {
    let bytes = match std::env::var("B79_REPLAY_DIR") {
        Ok(dir) => std::fs::read(std::path::Path::new(&dir).join(format!("{name}.pb")))?,
        Err(_) => generated.encode_to_vec(),
    };
    assert_eq!(bytes, generated.encode_to_vec(), "changed protobuf: {name}");
    let decoded = SubscribeUpdate::decode(bytes.as_slice())?;
    assert_eq!(decoded, generated);
    assert_eq!(decoded.encode_to_vec(), bytes);
    Ok(decoded)
}

#[test]
fn legacy_corpus_captures_full_outputs() -> Result<()> {
    for case in cases::corpus() {
        let mut source = YellowstoneGrpcSource::new(&config())?;
        let config = std::sync::Arc::get_mut(&mut source.runtime_config).unwrap();
        if case.empty_interest {
            config.interested_program_ids.clear();
        }
        let update = replay(&case.name, case.update)?;
        let mut result = legacy(update.clone(), config);
        result["fallback_counts"] =
            json!(*config.telemetry.parse_fallback_by_reason.lock().unwrap());
        capture(&case.name, &update, &result)?;
        if case.refused {
            assert!(result.get("raw").is_none(), "{}", case.name);
        }
    }
    Ok(())
}

#[path = "../block_association/mod.rs"]
mod block_association;

mod delivery_baseline_tests;
