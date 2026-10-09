//! Used12 target-ATA inputs; real production JSON/protobuf/RPC attribution only.
use super::*;
mod causal;
const ATA: &str = native_attribution::ATA;
fn bindings() -> Vec<Value> {
    serde_json::from_str(include_str!("fixtures/BINDINGS.json")).unwrap()
}
fn saved(label: &str) -> Value {
    let p = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(format!("src/source_tests/target_ata/fixtures/{label}.json"));
    let input: Value = serde_json::from_slice(&std::fs::read(p).unwrap()).unwrap();
    input[0]["result"].clone()
}
fn target_ata(r: &Value) -> usize {
    let k = keys(r);
    let target = accounts(r)[16]; // All real used12 inputs are 18/op9 SOL BUY.
    r["transaction"]["message"]["instructions"]
        .as_array()
        .unwrap()
        .iter()
        .position(|i| {
            k[i["programIdIndex"].as_u64().unwrap() as usize] == ATA && i["accounts"][1] == target
        })
        .unwrap()
}
fn assert_buy(r: &Value, binding: &Value) {
    let p = proto(r);
    let expected = json!({"amount_in_raw":binding["sol_raw"].as_u64().unwrap().to_string(),
        "amount_out_raw":binding["target_raw"].as_u64().unwrap().to_string(),
        "amount_in_decimals":9,"amount_out_decimals":6});
    assert_eq!(p["exact_amounts"], expected, "{}", binding["label"]);
    assert_eq!(p["wallet"], keys(r)[0]);
    assert_eq!(p["token_in"], SOL_MINT);
    assert_eq!(p["token_out"], binding["mint"]);
    for parsed in [false, true] {
        let j = json_decode(r, parsed);
        assert_eq!(j["exact_amounts"], expected);
        assert_eq!(j["token_in"], SOL_MINT);
        let actual = runtime_rpc(r, parsed).expect("real RPC production path");
        assert_eq!(
            serde_json::to_value(actual.exact_amounts).unwrap(),
            expected
        );
    }
}
#[test]
fn all_thirteen_saved_buy_inputs_have_executed_exact_amounts() {
    for binding in bindings() {
        assert_buy(&saved(binding["label"].as_str().unwrap()), &binding);
    }
}
#[test]
fn parsed_idempotent_ata_uses_same_common_proof() {
    for binding in bindings() {
        let r = saved(binding["label"].as_str().unwrap());
        let expected = proto(&r)["exact_amounts"].clone();
        let mut parsed = parity::expanded(&r, true);
        for ix in parsed["transaction"]["message"]["instructions"]
            .as_array_mut()
            .unwrap()
        {
            if ix["programId"] != ATA || ix["data"] != bs58::encode([1]).into_string() {
                continue;
            }
            let a = ix["accounts"].clone();
            *ix = json!({"programId":ATA,"parsed":{"type":"createIdempotent","info":{
                "source":a[0],"account":a[1],"wallet":a[2],"mint":a[3],
                "systemProgram":a[4],"tokenProgram":a[5]}}});
        }
        let signer = keys_raw_signer(&parsed);
        let attr = native_attribution::json::infer(
            &parsed,
            &parsed["meta"],
            &signer,
            &HashSet::from([PUMP.to_owned()]),
        );
        let native_attribution::Attribution::Known(trade) = attr else {
            panic!("parsed refused")
        };
        assert!(trade.buy);
        assert_eq!(trade.sol_raw.to_string(), expected["amount_in_raw"]);
        assert_eq!(trade.target_raw.to_string(), expected["amount_out_raw"]);
        let rays = HashSet::from([RAY.to_owned()]);
        let pumps = HashSet::from([PUMP.to_owned()]);
        let interest = rays.union(&pumps).cloned().collect();
        let actual = crate::source::rpc_backfill::raw_observation_from_transaction_result(
            parsed["transaction"]["signatures"][0].as_str().unwrap(),
            parsed["slot"].as_u64().unwrap(),
            &parsed,
            &interest,
            &rays,
            &pumps,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            serde_json::to_value(actual.exact_amounts).unwrap(),
            expected
        );
    }
}
#[test]
fn old_persistent_wsol_acceptance_is_unchanged() {
    assert_exact(&fixture(8), 8);
}
