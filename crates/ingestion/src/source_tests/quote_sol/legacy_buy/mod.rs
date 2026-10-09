//! Five immutable raw RPC cases exercise production protobuf and JSON attribution.
use super::{decode, json_native, SOL};
use serde_json::{json, Value};
mod controls;

fn fixture(label: &str) -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
        "src/source_tests/quote_sol/legacy_buy/fixtures/wallet-{label}.json"
    ));
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

fn expected(label: &str) -> Value {
    let (input, output, di, do_) = match label {
        "05-03" => (24_758_672_271u64, 19_607_189u64, 6, 9),
        "09-03" => (305_546_883, 831_433_829_629, 9, 6),
        "09-05" => (302_880_264, 686_277_533_925, 9, 6),
        "12-03" => (867_526_158, 2_092_236_931_943, 9, 6),
        "12-04" => (880_080_995, 2_049_686_981_793, 9, 6),
        _ => panic!("unknown fixture"),
    };
    json!({"amount_in_raw":input.to_string(),"amount_out_raw":output.to_string(),
        "amount_in_decimals":di,"amount_out_decimals":do_})
}

fn assert_exact(label: &str) {
    let r = fixture(label);
    assert!(json_presence(&r));
    let facts = decode(&r);
    assert_eq!(facts["exact_amounts"], expected(label));
    assert_eq!(
        facts[if label == "05-03" {
            "token_out"
        } else {
            "token_in"
        }],
        SOL
    );
    for parsed in [false, true] {
        let native = json_native(&r, parsed);
        assert_eq!(native["exact_amounts"], facts["exact_amounts"]);
        assert_eq!(native["token_in"], facts["token_in"]);
        assert_eq!(native["token_out"], facts["token_out"]);
    }
}

fn json_presence(r: &Value) -> bool {
    let mut r = r.clone();
    let keys = super::keys(&r);
    r["transaction"]["message"]["accountKeys"] = json!(keys);
    let expand = |ix: &mut Value| {
        let p = keys[ix["programIdIndex"].as_u64().unwrap() as usize].clone();
        let a: Vec<_> = ix["accounts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| keys[v.as_u64().unwrap() as usize].clone())
            .collect();
        *ix = json!({"programId":p,"accounts":a,"data":ix["data"],"stackHeight":ix["stackHeight"]});
    };
    for ix in r["transaction"]["message"]["instructions"]
        .as_array_mut()
        .unwrap()
    {
        expand(ix);
    }
    for group in r["meta"]["innerInstructions"].as_array_mut().unwrap() {
        for ix in group["instructions"].as_array_mut().unwrap() {
            expand(ix);
        }
    }
    crate::source::pumpswap_instruction::json_has_supported_swap(
        &r,
        &r["meta"],
        &std::collections::HashSet::from([super::PUMP.to_owned()]),
    )
}

#[test]
fn base_sol_sell() {
    assert_exact("05-03");
}
#[test]
fn quote_sol_temporary() {
    assert_exact("09-03");
}
#[test]
fn quote_sol_other_mint() {
    assert_exact("09-05");
}
#[test]
fn quote_sol_persistent() {
    assert_exact("12-03");
}
#[test]
fn quote_sol_persistent_second() {
    assert_exact("12-04");
}
