use super::{config, parse, PUMP, RAY};
use crate::parser::SwapParser;
use anyhow::Result;
use prost::Message;
use serde_json::{json, Value};

const SOL: &str = "So11111111111111111111111111111111111111112";
const PROVIDERS: [&str; 3] = ["yellowstone", "rpc_backfill", "helius_fetch"];

mod controls;
mod fixtures;
mod matrix;

#[path = "../owned_amount/mod.rs"]
mod owned_amount;

fn original_gift() -> Value {
    serde_json::from_str(include_str!("fixtures/g5_dominant_gift.json")).unwrap()
}

fn check(f: &Value, provider: &str, expected: bool) -> Result<Value> {
    let (raw, request) = if provider == "helius_fetch" {
        let (raw, request) = super::http::fetch(f, &config())?;
        (raw, Some(request))
    } else {
        (parse(f, provider)?, None)
    };
    let raw_present = raw.is_some();
    let event = raw.and_then(|raw| SwapParser::new(vec![RAY.into()], vec![PUMP.into()]).parse(raw));
    let record = json!({"case":f["case"],"provider":provider,"input":f,"raw_present":raw_present,"event":event,
        "http_request":request,"http_handler_joined":provider=="helius_fetch"});
    if let Ok(dir) = std::env::var("B45_CAPTURE_DIR") {
        let dir = std::path::Path::new(&dir);
        std::fs::create_dir_all(dir)?;
        std::fs::write(
            dir.join(format!("{}-{provider}.json", f["case"].as_str().unwrap())),
            serde_json::to_vec_pretty(&record)?,
        )?;
        if provider == "yellowstone" {
            std::fs::write(
                dir.join(format!("{}.pb", f["case"].as_str().unwrap())),
                super::proto::update(f).encode_to_vec(),
            )?;
        }
    }
    assert_eq!(raw_present, expected, "{} {provider}: {record}", f["case"]);
    assert_eq!(record["event"].is_null(), !expected);
    Ok(record)
}

#[test]
fn f2_gift_yellowstone_refused() -> Result<()> {
    check(&original_gift(), "yellowstone", false)?;
    Ok(())
}
#[test]
fn f2_gift_backfill_refused() -> Result<()> {
    check(&original_gift(), "rpc_backfill", false)?;
    Ok(())
}
#[test]
fn f2_gift_helius_refused() -> Result<()> {
    check(&original_gift(), "helius_fetch", false)?;
    Ok(())
}
