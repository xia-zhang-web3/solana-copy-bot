use crate::parser::SwapParser;
use crate::source::{
    rpc_backfill, yellowstone, IngestionConfig, RawSwapObservation, YellowstoneGrpcSource,
    YellowstoneParsedUpdate,
};
use anyhow::Result;
use prost::Message;
use serde_json::{json, Value};

mod cases;
mod http;
mod proto;
mod resolution;

const PUMP: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const RAY: &str = "675kPX9MHTjS2zt1qfr1NYHuzeVoWJg29uhPcaX4cF5";

fn fixture(case: &str) -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/source_tests/pumpswap/fixtures")
        .join(format!("{case}.json"));
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

fn config() -> IngestionConfig {
    let mut c = IngestionConfig::default();
    c.yellowstone_grpc_url = "http://127.0.0.1:1".into();
    c.yellowstone_x_token = "synthetic-not-a-credential".into();
    c.yellowstone_program_ids = vec![PUMP.into(), RAY.into()];
    c.subscribe_program_ids = c.yellowstone_program_ids.clone();
    c.pumpswap_program_ids = vec![PUMP.into()];
    c.raydium_program_ids = vec![RAY.into()];
    c
}

fn parse(f: &Value, provider: &str) -> Result<Option<RawSwapObservation>> {
    parse_with_config(f, provider, &config())
}

fn parse_with_config(
    f: &Value,
    provider: &str,
    c: &IngestionConfig,
) -> Result<Option<RawSwapObservation>> {
    let source = YellowstoneGrpcSource::new(c)?;
    match provider {
        "yellowstone" => {
            let wire = proto::update(f).encode_to_vec();
            let decoded =
                yellowstone_grpc_proto::prelude::SubscribeUpdate::decode(wire.as_slice())?;
            Ok(
                match yellowstone::parse_yellowstone_update(decoded, &source.runtime_config)? {
                    Some(YellowstoneParsedUpdate::Observation(raw)) => Some(raw),
                    None => None,
                    _ => panic!("unexpected ping"),
                },
            )
        }
        "rpc_backfill" => rpc_backfill::raw_observation_from_transaction_result(
            f["signature"].as_str().unwrap(),
            9999,
            &f["result"],
            &source.runtime_config.interested_program_ids,
            &source.runtime_config.raydium_program_ids,
            &source.runtime_config.pumpswap_program_ids,
        ),
        "helius_fetch" => Ok(http::fetch(f, c)?.0),
        _ => panic!("unknown provider"),
    }
}

fn assert_case(f: &Value, provider: &str, expected: bool) -> Result<()> {
    assert_config_case(f, provider, expected, &config())
}

fn assert_config_case(
    f: &Value,
    provider: &str,
    expected: bool,
    c: &IngestionConfig,
) -> Result<()> {
    let raw = parse_with_config(f, provider, c)?;
    let raw_present = raw.is_some();
    let event = raw.and_then(|raw| SwapParser::new(vec![RAY.into()], vec![PUMP.into()]).parse(raw));
    if let Ok(dir) = std::env::var("B43_CAPTURE_DIR") {
        let dir = std::path::Path::new(&dir);
        std::fs::create_dir_all(dir)?;
        let record = json!({"case":f["case"],"provider":provider,"input":f,"raw_present":raw_present,"event":event});
        std::fs::write(
            dir.join(format!("{}-{provider}.json", f["case"].as_str().unwrap())),
            serde_json::to_vec_pretty(&record)?,
        )?;
    }
    assert_eq!(
        raw_present, expected,
        "{} {provider}: raw refused before SwapParser",
        f["case"]
    );
    assert_eq!(event.is_some(), expected, "{} {provider}", f["case"]);
    Ok(())
}

macro_rules! refusal {
    ($name:ident, $case:literal, $provider:literal) => {
        #[test]
        fn $name() -> Result<()> {
            assert_case(&fixture($case), $provider, false)
        }
    };
}
refusal!(f1_buy_yellowstone, "g4_extend_gift_buy", "yellowstone");
refusal!(f1_buy_backfill, "g4_extend_gift_buy", "rpc_backfill");
refusal!(f1_buy_helius, "g4_extend_gift_buy", "helius_fetch");
refusal!(f1_sell_yellowstone, "g4_extend_payment_sell", "yellowstone");
refusal!(f1_sell_backfill, "g4_extend_payment_sell", "rpc_backfill");
refusal!(f1_sell_helius, "g4_extend_payment_sell", "helius_fetch");

#[test]
fn maintenance_and_account_only_refused_at_all_boundaries() -> Result<()> {
    for case in ["g4_extend_only", "g3_gift_0", "g3_gift_500000000"] {
        for provider in ["yellowstone", "rpc_backfill", "helius_fetch"] {
            assert_case(&fixture(case), provider, false)?;
        }
    }
    Ok(())
}

#[path = "../mint_uniqueness/mod.rs"]
mod mint_uniqueness;

#[path = "../native_cpi/mod.rs"]
mod native_cpi;
