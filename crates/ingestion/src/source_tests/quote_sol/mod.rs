//! Saved RPC seeds exercise the common production decoder, without transport.
use crate::source::{native_attribution, yellowstone_facts::decode_yellowstone_swap_facts};
use serde_json::{json, Value};
use std::collections::HashSet;

mod controls;
mod capture_fixtures_tests;
mod dispatch;
mod rpc;
mod legacy_buy;

const PUMP: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const RAY: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const CPMM: &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";
const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const SOL: &str = "So11111111111111111111111111111111111111112";

fn fixture(wallet: usize) -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
        "src/source_tests/quote_sol/fixtures/wallet-{wallet:02}.json"
    ));
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

fn keys(r: &Value) -> Vec<String> {
    r["transaction"]["message"]["accountKeys"]
        .as_array()
        .unwrap()
        .iter()
        .chain(
            r["meta"]["loadedAddresses"]["writable"]
                .as_array()
                .unwrap()
                .iter(),
        )
        .chain(
            r["meta"]["loadedAddresses"]["readonly"]
                .as_array()
                .unwrap()
                .iter(),
        )
        .map(|k| k.as_str().unwrap().to_owned())
        .collect()
}

fn parent_index(r: &Value) -> usize {
    let keys = keys(r);
    r["transaction"]["message"]["instructions"]
        .as_array()
        .unwrap()
        .iter()
        .position(|ix| keys[ix["programIdIndex"].as_u64().unwrap() as usize] == PUMP)
        .unwrap()
}

fn accounts(r: &Value) -> Vec<usize> {
    r["transaction"]["message"]["instructions"][parent_index(r)]["accounts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i.as_u64().unwrap() as usize)
        .collect()
}

fn group(r: &mut Value) -> &mut Vec<Value> {
    let parent = parent_index(r);
    r["meta"]["innerInstructions"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|g| g["index"] == parent)
        .unwrap()["instructions"]
        .as_array_mut()
        .unwrap()
}

fn decode(r: &Value) -> Value {
    let tx = rpc::update(r).unwrap();
    let rays = HashSet::from([RAY.to_owned(), CPMM.to_owned()]);
    let pumps = HashSet::from([PUMP.to_owned()]);
    let interest = rays.union(&pumps).cloned().collect();
    match decode_yellowstone_swap_facts(&tx, &interest, &rays, &pumps)
        .facts
        .unwrap()
    {
        None => Value::Null,
        Some(f) => json!({"wallet": f.signer, "signature": f.signature, "slot": f.slot,
            "token_in":f.token_in,"token_out":f.token_out,"exact_amounts":f.exact_amounts,
            "amount_in":f.amount_in,"amount_out":f.amount_out,"dex_hint":f.dex_hint}),
    }
}

fn json_native(r: &Value, parsed_checked: bool) -> Value {
    let mut r = r.clone();
    let keys = keys(&r);
    let signer = keys[0].clone();
    let signers = r["transaction"]["message"]["header"]["numRequiredSignatures"]
        .as_u64()
        .unwrap() as usize;
    r["transaction"]["message"]["accountKeys"] = json!(keys
        .iter()
        .enumerate()
        .map(|(i, k)| json!({"pubkey":k,"signer":i<signers}))
        .collect::<Vec<_>>());
    let expand = |ix: &mut Value| {
        let program = keys[ix["programIdIndex"].as_u64().unwrap() as usize].clone();
        let indexes: Vec<usize> = ix["accounts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n.as_u64().unwrap() as usize)
            .collect();
        let data = bs58::decode(ix["data"].as_str().unwrap())
            .into_vec()
            .unwrap();
        let depth = ix.get("stackHeight").cloned();
        *ix = json!({"programId":program,"accounts":indexes.iter().map(|i|&keys[*i]).collect::<Vec<_>>(),
            "data":bs58::encode(&data).into_string()});
        if let Some(depth) = depth {
            ix["stackHeight"] = depth;
        }
        if parsed_checked && program == TOKEN && data.len() == 10 && data[0] == 12 {
            let amount = u64::from_le_bytes(data[1..9].try_into().unwrap());
            ix.as_object_mut().unwrap().remove("accounts");
            ix.as_object_mut().unwrap().remove("data");
            ix["parsed"] = json!({"type":"transferChecked","info":{
                "source":keys[indexes[0]],"mint":keys[indexes[1]],"destination":keys[indexes[2]],
                "authority":keys[indexes[3]],"tokenAmount":{"amount":amount.to_string(),"decimals":data[9]}}});
        }
    };
    for ix in r["transaction"]["message"]["instructions"]
        .as_array_mut()
        .unwrap()
    {
        expand(ix);
    }
    for g in r["meta"]["innerInstructions"].as_array_mut().unwrap() {
        for ix in g["instructions"].as_array_mut().unwrap() {
            expand(ix);
        }
    }
    match native_attribution::json::infer(
        &r,
        &r["meta"],
        &signer,
        &HashSet::from([PUMP.to_owned()]),
    ) {
        native_attribution::Attribution::Known(t) => {
            let (ti, ai, di, to, ao, do_) = t.legs();
            json!({"token_in":ti,"token_out":to,"exact_amounts":{"amount_in_raw":ai.to_string(),
                "amount_in_decimals":di,"amount_out_raw":ao.to_string(),"amount_out_decimals":do_}})
        }
        native_attribution::Attribution::Unknown => json!({"refused":"unknown"}),
        native_attribution::Attribution::NotApplicable => json!({"refused":"not_applicable"}),
    }
}

fn expected(wallet: usize) -> (&'static str, u64, u64, u8) {
    match wallet {
        6 => ("SELL", 1_085_477_023, 146_964_030, 6),
        10 => ("SELL", 1_205_125_574, 163_052_633, 6),
        11 => ("BUY", 21_616_157, 115_186_657, 6),
        12 => ("SELL", 117_347_303, 21_889_887, 6),
        8 => ("BUY", 1_479_491_139, 35_446_713_948, 5),
        _ => panic!("no exact proof for wallet {wallet}"),
    }
}

fn assert_exact(r: &Value, wallet: usize) {
    let out = decode(r);
    let (side, input, output, decimals) = expected(wallet);
    let e = &out["exact_amounts"];
    assert_eq!(e["amount_in_raw"], input.to_string(), "wallet {wallet}");
    assert_eq!(e["amount_out_raw"], output.to_string(), "wallet {wallet}");
    assert_eq!(
        out[if side == "BUY" {
            "token_in"
        } else {
            "token_out"
        }],
        SOL
    );
    assert_eq!(
        e["amount_in_decimals"],
        if side == "BUY" { 9 } else { decimals }
    );
    assert_eq!(
        e["amount_out_decimals"],
        if side == "BUY" { decimals } else { 9 }
    );
    assert_eq!(out["wallet"], keys(r)[0]);
    assert_eq!(out["signature"], r["transaction"]["signatures"][0]);
    if wallet != 8 {
        let a = accounts(r);
        assert_eq!(
            out[if side == "BUY" {
                "token_out"
            } else {
                "token_in"
            }],
            keys(r)[a[3]]
        );
        for parsed in [false, true] {
            let j = json_native(r, parsed);
            assert_eq!(
                j["exact_amounts"], *e,
                "JSON/proto parity wallet {wallet} parsed={parsed}"
            );
            assert_eq!(j["token_in"], out["token_in"]);
            assert_eq!(j["token_out"], out["token_out"]);
        }
    }
}

fn assert_refused(r: &Value) {
    assert!(
        decode(r)["exact_amounts"].is_null(),
        "must not emit exact legs"
    );
    for parsed in [false, true] {
        assert!(
            json_native(r, parsed)["exact_amounts"].is_null(),
            "JSON must refuse exact legs"
        );
    }
}

#[test]
fn saved_seeds_preserve_remaining_refusals_and_persistent_wsol_positive() {
    for wallet in 1..=12 {
        let r = fixture(wallet);
        if [6, 8, 10, 11, 12].contains(&wallet) {
            assert_exact(&r, wallet);
        } else if (1..=5).contains(&wallet) {
            // Historical direct AMMv4 BUY with the now-proved existing target ATA.
            // Executed SPL CPI u64 legs: SOL200000 -> USDC23190, not native cash.
            let expected = json!({"amount_in_raw":"200000","amount_out_raw":"23190",
                "amount_in_decimals":9,"amount_out_decimals":6});
            let actual = decode(&r);
            assert_eq!(actual["exact_amounts"], expected);
            assert_eq!(actual["token_in"], SOL);
            assert_eq!(
                actual["token_out"],
                "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
            );
            for parsed in [false, true] {
                let actual = json_native(&r, parsed);
                assert_eq!(actual["exact_amounts"], expected);
                assert_eq!(actual["token_in"], SOL);
            }
        } else {
            assert!(
                decode(&r)["exact_amounts"].is_null(),
                "wallet {wallet} must remain missing-exact"
            );
        }
    }
}

#[test]
#[ignore = "private bounded corpus/output paths required"]
fn replay_private_corpus() {
    let input = std::env::var_os("COPYBOT_QUOTE_REPLAY_INPUT").unwrap();
    let output = std::env::var_os("COPYBOT_QUOTE_REPLAY_OUTPUT").unwrap();
    let bytes = std::fs::read(input).unwrap();
    assert!(bytes.len() <= 32 * 1024 * 1024);
    let records: Value = serde_json::from_slice(&bytes).unwrap();
    let records = records.as_array().unwrap();
    assert!(records.len() <= 128);
    let rows: Vec<_> = records
        .iter()
        .map(|record| {
            let r = record.get("result").unwrap_or(record);
            json!({"label":record.get("label"),"facts":decode(r),
            "native_json":json_native(r,false),"native_json_parsed":json_native(r,true)})
        })
        .collect();
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(output)
        .unwrap()
        .write_all(&serde_json::to_vec_pretty(&json!({"rows":rows})).unwrap())
        .unwrap();
}
