//! Exact replay of used11 inputs. No transport, execution or provider requests.
use super::source_selection_rpc as rpc;
use crate::source::yellowstone_facts::decode_yellowstone_swap_facts;
use crate::source::{native_attribution, HeliusWsSource, SOL_MINT};
use serde_json::{json, Value};
use std::collections::HashSet;
mod causal;
mod parity;
#[path = "../target_ata/mod.rs"]
mod target_ata;
const RAY: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const PUMP: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TARGETS: [usize; 5] = [2, 6, 7, 9, 11];
fn fixture(wallet: usize) -> Value {
    corpus(&format!("wallet-{wallet:02}-01"))
}
fn corpus(label: &str) -> Value {
    let p = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(format!("src/source_tests/ammv4/fixtures/{label}.json"));
    let input: Value = serde_json::from_slice(&std::fs::read(p).unwrap()).unwrap();
    input[0]["result"].clone()
}
fn keys(r: &Value) -> Vec<String> {
    r["transaction"]["message"]["accountKeys"]
        .as_array()
        .unwrap()
        .iter()
        .chain(r["meta"]["loadedAddresses"]["writable"].as_array().unwrap())
        .chain(r["meta"]["loadedAddresses"]["readonly"].as_array().unwrap())
        .map(|k| k.as_str().unwrap().to_owned())
        .collect()
}
fn parent(r: &Value) -> usize {
    let k = keys(r);
    r["transaction"]["message"]["instructions"]
        .as_array()
        .unwrap()
        .iter()
        .position(|ix| k[ix["programIdIndex"].as_u64().unwrap() as usize] == RAY)
        .unwrap()
}
fn accounts(r: &Value) -> Vec<usize> {
    r["transaction"]["message"]["instructions"][parent(r)]["accounts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i.as_u64().unwrap() as usize)
        .collect()
}
fn group(r: &mut Value) -> &mut Vec<Value> {
    let p = parent(r);
    r["meta"]["innerInstructions"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|g| g["index"] == p)
        .unwrap()["instructions"]
        .as_array_mut()
        .unwrap()
}
fn raw(ix: &Value) -> Vec<u8> {
    bs58::decode(ix["data"].as_str().unwrap())
        .into_vec()
        .unwrap()
}
fn set_raw(ix: &mut Value, data: &[u8]) {
    ix["data"] = bs58::encode(data).into_string().into();
}
fn expected(w: usize) -> (bool, u64, u64) {
    match w {
        2 => (false, 5_229_051_902, 124_004_527),
        6 => (true, 7_848_635, 83_991_045_815),
        7 => (false, 68_050_114_830, 6_379_006),
        9 => (false, 59_571_023_989, 5_959_150),
        11 => (false, 78_991_337_155, 7_922_400),
        8 => (false, 7_671_941_314, 2_889_036_527),
        _ => panic!("no expected new amounts"),
    }
}
fn proto(r: &Value) -> Value {
    let tx = rpc::update(r).unwrap();
    let rays = HashSet::from([
        RAY.to_owned(),
        "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C".to_owned(),
    ]);
    let pumps = HashSet::from([PUMP.to_owned()]);
    let interest = rays.union(&pumps).cloned().collect();
    let out = decode_yellowstone_swap_facts(&tx, &interest, &rays, &pumps);
    match out.facts.unwrap() {
        None => json!({"refused":format!("{:?}",out.miss)}),
        Some(f) => json!({
        "wallet":f.signer,"signature":f.signature,"slot":f.slot,"token_in":f.token_in,"token_out":f.token_out,
        "amount_in":f.amount_in,"amount_out":f.amount_out,"exact_amounts":f.exact_amounts,"dex_hint":f.dex_hint}),
    }
}
fn json_decode(r: &Value, parsed: bool) -> Value {
    let r = parity::expanded(r, parsed);
    let signer = &keys_raw_signer(&r);
    let attr =
        native_attribution::json::infer(&r, &r["meta"], signer, &HashSet::from([PUMP.to_owned()]));
    let status = match attr {
        native_attribution::Attribution::Unknown => "unknown",
        native_attribution::Attribution::Known(_) => "known",
        native_attribution::Attribution::NotApplicable => "not_applicable",
    };
    let swap = HeliusWsSource::infer_swap_from_json_balances_with_attribution(
        &r["meta"],
        0,
        signer,
        || attr,
    );
    match swap {
        None => json!({"refused":status}),
        Some((ti, ai, to, ao)) => json!({"attribution":status,"token_in":ti,"token_out":to,
        "amount_in":ai.amount,"amount_out":ao.amount,"exact_amounts":{
            "amount_in_raw":ai.raw_amount,"amount_out_raw":ao.raw_amount,"amount_in_decimals":ai.decimals,"amount_out_decimals":ao.decimals}}),
    }
}
fn keys_raw_signer(r: &Value) -> String {
    r["transaction"]["message"]["accountKeys"][0]["pubkey"]
        .as_str()
        .unwrap()
        .to_owned()
}
fn runtime_rpc(r: &Value, parsed: bool) -> Option<crate::RawSwapObservation> {
    let r = parity::expanded(r, parsed);
    let rays = HashSet::from([RAY.to_owned()]);
    let pumps = HashSet::from([PUMP.to_owned()]);
    let interest = rays.union(&pumps).cloned().collect();
    crate::source::rpc_backfill::raw_observation_from_transaction_result(
        r["transaction"]["signatures"][0].as_str().unwrap(),
        r["slot"].as_u64().unwrap(),
        &r,
        &interest,
        &rays,
        &pumps,
    )
    .unwrap()
}
fn assert_exact(r: &Value, w: usize) {
    let p = proto(r);
    let (buy, input, output) = expected(w);
    let e = &p["exact_amounts"];
    assert_eq!(e["amount_in_raw"], input.to_string(), "wallet{w}");
    assert_eq!(e["amount_out_raw"], output.to_string(), "wallet{w}");
    assert_eq!(p[if buy { "token_in" } else { "token_out" }], SOL_MINT);
    assert_eq!(p["wallet"], keys(r)[0]);
    assert_eq!(p["signature"], r["transaction"]["signatures"][0]);
    for parsed in [false, true] {
        let j = json_decode(r, parsed);
        assert_eq!(j["exact_amounts"], *e, "JSON/proto {w}");
        assert_eq!(j["token_in"], p["token_in"]);
        assert_eq!(j["token_out"], p["token_out"]);
        let observation = runtime_rpc(r, parsed).expect("actual runtime RPC decoder");
        assert_eq!(
            serde_json::to_value(&observation.exact_amounts).unwrap(),
            *e
        );
        assert_eq!(observation.signer, keys(r)[0]);
        assert_eq!(observation.signature, r["transaction"]["signatures"][0]);
    }
}
fn assert_terminal(r: &Value) {
    assert!(
        proto(r).get("token_in").is_none(),
        "damaged witness must not infer any swap"
    );
    for parsed in [false, true] {
        assert_eq!(json_decode(r, parsed)["refused"], "unknown");
        assert!(runtime_rpc(r, parsed).is_none());
    }
}
#[test]
fn saved_five_exact_and_persistent_08_unchanged() {
    for w in TARGETS.into_iter().chain([8]) {
        assert_exact(&fixture(w), w);
    }
}
#[test]
fn remaining_seeds_keep_boundaries() {
    for w in [1, 3] {
        assert!(!proto(&fixture(w))["exact_amounts"].is_null());
    }
    for w in [4, 10, 12] {
        assert!(proto(&fixture(w))["exact_amounts"].is_null());
    }
    assert_terminal(&fixture(5)); // Real USDC/token has no SOL swap leg.
}
#[test]
#[ignore = "explicit private output path for affected corpus only"]
fn replay_all_saved_36() {
    let b: Value = serde_json::from_str(include_str!("fixtures/BINDINGS.json")).unwrap();
    let rows:Vec<_>=b.as_object().unwrap().keys().map(|label|{let r=corpus(label);json!({"label":label,"proto":proto(&r),"json":json_decode(&r,false),"json_parsed":json_decode(&r,true)})}).collect();
    let p = std::env::var_os("COPYBOT_AMMV4_REPLAY_OUTPUT").unwrap();
    let mut f = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(p)
        .unwrap();
    use std::io::Write;
    f.write_all(&serde_json::to_vec_pretty(&json!({"rows":rows})).unwrap())
        .unwrap();
}
