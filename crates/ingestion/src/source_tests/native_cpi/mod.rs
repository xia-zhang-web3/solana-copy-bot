use super::{config, parse, proto, PUMP, RAY};
use crate::parser::SwapParser;
use crate::source::{yellowstone, YellowstoneGrpcSource, YellowstoneParsedUpdate};
use anyhow::Result;
use prost::Message;
use serde_json::{json, Value};
use yellowstone_grpc_proto::prelude::{subscribe_update, SubscribeUpdate};

#[path = "../known_time/http.rs"]
mod http;

fn scenario(name: &str) -> Result<()> {
    let evidence =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/source_tests/native_cpi");
    let output = std::env::var("B55_CAPTURE_DIR")
        .ok()
        .map(std::path::PathBuf::from);
    if let Some(output) = &output {
        std::fs::create_dir_all(output)?;
    }
    let mut mismatches = Vec::new();
    for side in ["buy", "sell"] {
        let case = format!("b54-{name}-{side}");
        let input: Value = serde_json::from_slice(&std::fs::read(
            evidence.join(format!("fixtures/{case}.json")),
        )?)?;
        let mut events = Vec::new();
        for provider in ["yellowstone", "rpc_backfill", "helius_fetch"] {
            let mut wire = None;
            let mut decoded_facts = Value::Null;
            let (raw, http) = if provider == "yellowstone" {
                let mut adapter = input.clone();
                let absent = adapter["result"]["meta"].get("innerInstructions").is_none();
                if absent {
                    adapter["result"]["meta"]["innerInstructions"] = json!([]);
                }
                let mut update = proto::update(&adapter);
                if let Some(subscribe_update::UpdateOneof::Transaction(tx)) =
                    &mut update.update_oneof
                {
                    tx.transaction
                        .as_mut()
                        .unwrap()
                        .meta
                        .as_mut()
                        .unwrap()
                        .inner_instructions_none = absent;
                }
                let bytes = update.encode_to_vec();
                let decoded = SubscribeUpdate::decode(bytes.as_slice())?;
                assert_eq!(decoded, update);
                if let Some(subscribe_update::UpdateOneof::Transaction(tx)) = &decoded.update_oneof
                {
                    let info = tx.transaction.as_ref().unwrap();
                    let meta = info.meta.as_ref().unwrap();
                    assert_eq!(meta.inner_instructions_none, absent);
                    assert_eq!(meta.pre_balances.len(), meta.post_balances.len());
                    decoded_facts = json!({"inner_none":meta.inner_instructions_none,
                        "inner_groups":meta.inner_instructions.len(),"fee":meta.fee,
                        "signer_count":info.transaction.as_ref().unwrap().message.as_ref().unwrap().header.as_ref().unwrap().num_required_signatures,
                        "exact_roundtrip_bytes":decoded.encode_to_vec()==bytes});
                }
                let source = YellowstoneGrpcSource::new(&config())?;
                let raw =
                    match yellowstone::parse_yellowstone_update(decoded, &source.runtime_config)? {
                        Some(YellowstoneParsedUpdate::Observation(raw)) => Some(raw),
                        None => None,
                        _ => panic!("unexpected ping"),
                    };
                wire = Some(bytes);
                (raw, Value::Null)
            } else if provider == "helius_fetch" {
                http::fetch(&input, &config())?
            } else {
                (parse(&input, provider)?, Value::Null)
            };
            let raw_present = raw.is_some();
            let event =
                raw.and_then(|raw| SwapParser::new(vec![RAY.into()], vec![PUMP.into()]).parse(raw));
            let record = json!({"case":case,"provider":provider,"input":input,"raw_present":raw_present,
                "event":event,"http":http,"protobuf_decoded_facts":decoded_facts});
            if let Some(output) = &output {
                std::fs::write(
                    output.join(format!("{case}-{provider}.json")),
                    serde_json::to_vec_pretty(&record)?,
                )?;
                if let Some(bytes) = wire {
                    std::fs::write(output.join(format!("{case}-{provider}.pb")), bytes)?;
                }
            }
            let ev = record["event"].clone();
            if name == "incomplete" {
                if raw_present || !ev.is_null() {
                    mismatches.push(format!("{case}-{provider}: incomplete emitted"));
                }
                events.push(ev);
                continue;
            }
            assert!(raw_present && !ev.is_null(), "{record}");
            assert_eq!(ev["signature"], input["signature"]);
            assert_eq!(ev["wallet"], input["roles"]["user"]);
            assert_eq!(ev["slot"], input["result"]["slot"]);
            let time =
                chrono::DateTime::from_timestamp(input["result"]["blockTime"].as_i64().unwrap(), 0)
                    .unwrap();
            assert_eq!(ev["ts_utc"], json!(time));
            let token = if side == "buy" {
                "token_out"
            } else {
                "token_in"
            };
            assert_eq!(ev[token], input["roles"]["quote_mint"]);
            let amount = if side == "buy" {
                "amount_in"
            } else {
                "amount_out"
            };
            let expected = input["audit"]["swap_sol_raw"].as_u64().unwrap() as f64 / 1e9;
            if (ev[amount].as_f64().unwrap() - expected).abs() >= 1e-12 {
                mismatches.push(format!(
                    "{case}-{provider}: got {} expected {expected}",
                    ev[amount]
                ));
            }
            if ev["exact_amounts"].is_null() {
                mismatches.push(format!("{case}-{provider}: no exact legs"));
            }
            assert_eq!(
                ev[if side == "buy" {
                    "amount_out"
                } else {
                    "amount_in"
                }],
                10.0
            );
            events.push(ev);
        }
        assert_eq!(events[0], events[1]);
        assert_eq!(events[1], events[2]);
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    Ok(())
}
macro_rules! probe {
    ($name:ident) => {
        #[test]
        fn $name() -> Result<()> {
            scenario(stringify!($name))
        }
    };
}
probe!(persistent);
probe!(temporary);
probe!(fee_only);
probe!(payment);
probe!(rent);
probe!(incomplete);

mod controls;
mod harness;
mod parsed;
mod refusals;

mod ata_binding_controls;
mod root_routing_review_tests;

mod ata_amounts;
mod ata_fixture;

mod ata_controls;

mod ata_depth_controls;
mod root_top_depth_review;

mod root_b62_existing_target_missing_row;
mod target_rows_controls;
mod target_rows_fixture;
