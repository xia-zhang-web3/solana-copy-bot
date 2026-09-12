use super::{config, http, parse, proto, PUMP, RAY};
use crate::parser::SwapParser;
use crate::source::{yellowstone, YellowstoneGrpcSource, YellowstoneParsedUpdate};
use anyhow::Result;
use prost::Message;
use serde_json::{json, Value};
use yellowstone_grpc_proto::prelude::{subscribe_update, SubscribeUpdate};

pub(super) fn fixture(name: &str, side: &str) -> Value {
    let p = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/source_tests/native_cpi/fixtures")
        .join(format!("b54-{name}-{side}.json"));
    serde_json::from_slice(&std::fs::read(p).unwrap()).unwrap()
}

pub(super) fn capture(label: &str, input: &Value, provider: &str) -> Result<Value> {
    if provider == "yellowstone" {
        let mut adapter = input.clone();
        let absent = !adapter["result"]["meta"]["innerInstructions"].is_array();
        if absent {
            adapter["result"]["meta"]["innerInstructions"] = json!([]);
        }
        let mut update = proto::update(&adapter);
        if let Some(subscribe_update::UpdateOneof::Transaction(tx)) = &mut update.update_oneof {
            tx.transaction
                .as_mut()
                .unwrap()
                .meta
                .as_mut()
                .unwrap()
                .inner_instructions_none = absent;
        }
        return capture_proto(label, input, update);
    }
    let (raw, http) = if provider == "helius_fetch" {
        http::fetch(input, &config())?
    } else {
        (parse(input, provider)?, Value::Null)
    };
    let event = raw.and_then(|r| SwapParser::new(vec![RAY.into()], vec![PUMP.into()]).parse(r));
    save(label, provider, input, json!(event), http, None)
}

pub(super) fn capture_proto(label: &str, input: &Value, update: SubscribeUpdate) -> Result<Value> {
    let wire = update.encode_to_vec();
    let decoded = SubscribeUpdate::decode(wire.as_slice())?;
    assert_eq!(decoded, update);
    assert_eq!(decoded.encode_to_vec(), wire);
    let source = YellowstoneGrpcSource::new(&config())?;
    let raw = match yellowstone::parse_yellowstone_update(decoded, &source.runtime_config)? {
        Some(YellowstoneParsedUpdate::Observation(r)) => Some(r),
        None => None,
        _ => panic!("unexpected ping"),
    };
    let event = raw.and_then(|r| SwapParser::new(vec![RAY.into()], vec![PUMP.into()]).parse(r));
    save(
        label,
        "yellowstone",
        input,
        json!(event),
        Value::Null,
        Some(wire),
    )
}

fn save(
    label: &str,
    provider: &str,
    input: &Value,
    event: Value,
    http: Value,
    wire: Option<Vec<u8>>,
) -> Result<Value> {
    if let Ok(dir) = std::env::var("B55_CAPTURE_DIR") {
        let p = std::path::Path::new(&dir);
        std::fs::create_dir_all(p)?;
        let record = json!({"case":label,"provider":provider,"input":input,"event":event,"raw_present":!event.is_null(),"http":http,
            "protobuf_exact_encode_decode":wire.is_some()});
        std::fs::write(
            p.join(format!("{label}-{provider}.json")),
            serde_json::to_vec_pretty(&record)?,
        )?;
        if let Some(wire) = wire {
            std::fs::write(p.join(format!("{label}-{provider}.pb")), wire)?;
        }
    }
    Ok(event)
}

pub(super) fn known(event: &Value, buy: bool) {
    assert!(!event.is_null(), "expected Known");
    assert_eq!(
        event[if buy { "amount_in" } else { "amount_out" }],
        if buy { 1.0 } else { 1.2 }
    );
    assert_eq!(event[if buy { "amount_out" } else { "amount_in" }], 10.0);
    let exact = &event["exact_amounts"];
    assert_eq!(
        exact[if buy {
            "amount_in_raw"
        } else {
            "amount_out_raw"
        }],
        if buy { "1000000000" } else { "1200000000" }
    );
    assert_eq!(
        exact[if buy {
            "amount_out_raw"
        } else {
            "amount_in_raw"
        }],
        "10000000"
    );
    assert_eq!(
        exact[if buy {
            "amount_in_decimals"
        } else {
            "amount_out_decimals"
        }],
        9
    );
    assert_eq!(
        exact[if buy {
            "amount_out_decimals"
        } else {
            "amount_in_decimals"
        }],
        6
    );
}
