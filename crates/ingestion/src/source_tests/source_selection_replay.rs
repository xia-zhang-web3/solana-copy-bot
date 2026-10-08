//! Offline exact-current-decoder replay. No providers, transport, daemon or signer.
use super::source_selection_rpc as rpc;
use crate::source::yellowstone_facts::decode_yellowstone_swap_facts;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashSet};

const AMM: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const CPMM: &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";
const OLD_CPMM: &str = "CPMMoo8L3F4NbTegBCKVN6DKuQh8fYfY4yR4j3uP9s5";
const PUMP: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const SOL: &str = "So11111111111111111111111111111111111111112";
const CLASSIC: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

fn classify(record: &Value, cpmm: &str) -> Value {
    let r = &record["result"];
    let raw_sha256 = format!("{:x}", Sha256::digest(serde_json::to_vec(r).unwrap()));
    let mut out = json!({"sequence":record["sequence"], "signature":r["transaction"]["signatures"][0],
        "slot":r["slot"], "block_time":r["blockTime"], "raw_result_sha256":raw_sha256,
        "rpc_error":r["meta"]["err"], "transport_replay":false, "block_association_proven":false});
    let tx = match rpc::update(r) {
        Ok(tx) => tx,
        Err(e) => {
            out["status"] = "adapter_refused".into();
            out["reason"] = e.to_string().into();
            return out;
        }
    };
    let rays = HashSet::from([AMM.to_string(), cpmm.to_string()]);
    let pumps = HashSet::from([PUMP.to_string()]);
    let interest = rays.union(&pumps).cloned().collect();
    let decoded = decode_yellowstone_swap_facts(&tx, &interest, &rays, &pumps);
    out["used_program_fallback"] = decoded.used_program_fallback.into();
    match decoded.facts {
        Ok(Some(f)) => {
            let (side, mint) = if f.token_in == SOL {
                ("BUY", &f.token_out)
            } else if f.token_out == SOL {
                ("SELL", &f.token_in)
            } else {
                ("OTHER", &f.token_out)
            };
            let mut programs = f.program_ids.clone();
            programs.sort();
            let meta = tx.transaction.as_ref().unwrap().meta.as_ref().unwrap();
            let target_programs: BTreeSet<_> = meta
                .pre_token_balances
                .iter()
                .chain(&meta.post_token_balances)
                .filter(|b| b.owner == f.signer && &b.mint == mint)
                .map(|b| b.program_id.as_str())
                .collect();
            let classic =
                !target_programs.is_empty() && target_programs.iter().all(|p| *p == CLASSIC);
            out["status"] = "decoded_swap".into();
            out["facts"] = json!({"wallet":f.signer,"mint":mint,"side":side,
                "token_in":f.token_in,"token_out":f.token_out,"amount_in":f.amount_in,
                "amount_out":f.amount_out,"exact_amounts":f.exact_amounts,
                "program_ids":programs,"dex_hint":f.dex_hint,"target_token_programs":target_programs,
                "classic_spl_target_rows":classic,"mint_account_owner_checked":false});
        }
        Ok(None) => {
            out["status"] = "decoder_refused".into();
            out["reason"] = format!("{:?}", decoded.miss).into();
        }
        Err(e) => {
            out["status"] = "decoder_error".into();
            out["reason"] = e.to_string().into();
        }
    }
    out
}

#[test]
#[ignore = "requires a saved private RPC corpus and fresh output path"]
fn replay_saved_rpc_corpus() {
    let input = std::env::var_os("COPYBOT_SOURCE_REPLAY_INPUT").expect("input path required");
    let output = std::env::var_os("COPYBOT_SOURCE_REPLAY_OUTPUT").expect("output path required");
    let raw = std::fs::read(input).unwrap();
    assert!(raw.len() <= 256 * 1024 * 1024, "replay input bound");
    let input_sha256 = format!("{:x}", Sha256::digest(&raw));
    let input: Value = serde_json::from_slice(&raw).unwrap();
    let records = rpc::records(&input).unwrap();
    let rows: Vec<_> = records
        .iter()
        .map(|r| {
            let mut row = classify(r, CPMM);
            row["old_cpmm_policy"] = classify(r, OLD_CPMM);
            row
        })
        .collect();
    let result = json!({"adapter":"rpc-raw-to-current-decoder-v1","input_sha256":input_sha256,
        "policy":{"amm_v4":AMM,"cpmm":CPMM,"pumpswap":PUMP},"rows":rows,
        "limitations":["RPC facts only: no transport, block binding, event latency or canonical fork proof",
            "RPC errors serialized as JSON presence, not Solana bincode",
            "Unused rewards/return-data omitted; no synthetic inner stackHeight or envelope clock",
            "Token rows do not replace fresh mint-account ownership preflight"]});
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(output).unwrap();
    use std::io::Write;
    file.write_all(&serde_json::to_vec_pretty(&result).unwrap())
        .unwrap();
}

fn fixture() -> Value {
    // Keep adapter controls on the accepted direct PumpSwap BUY seed. Older
    // capture fixtures are not a promise that an ambiguous swap decodes.
    let result: Value = serde_json::from_str(include_str!(
        "quote_sol/fixtures/wallet-11.json"
    ))
    .unwrap();
    let record = json!({"sequence":11,"result":result});
    assert_eq!(classify(&record, CPMM)["status"], "decoded_swap");
    record
}

#[test]
fn transaction_config_is_rejected_independently_of_version() {
    let original = fixture();
    assert_eq!(classify(&original, CPMM)["status"], "decoded_swap");
    for version in [json!("legacy"), json!(0), json!(1)] {
        for config in [json!({}), Value::Null] {
            let mut record = original.clone();
            record["result"]["version"] = version.clone();
            record["result"]["transaction"]["message"]["transactionConfig"] = config;
            let actual = classify(&record, CPMM);
            assert_eq!(actual["status"], "adapter_refused");
            assert_eq!(
                actual["reason"],
                "message.transactionConfig unsupported for current decoder"
            );
            assert!(actual.get("facts").is_none());
        }
    }
}

#[test]
fn preserves_relevant_raw_fields_and_unknown_inner_depth() {
    let mut r = fixture()["result"].clone();
    r["meta"]["innerInstructions"][0]["instructions"][0]["stackHeight"] = Value::Null;
    let tx = rpc::update(&r).unwrap();
    let info = tx.transaction.unwrap();
    let m = info.transaction.unwrap().message.unwrap();
    let meta = info.meta.unwrap();
    assert_eq!(tx.slot, r["slot"].as_u64().unwrap());
    assert_eq!(
        m.header.unwrap().num_required_signatures,
        r["transaction"]["message"]["header"]["numRequiredSignatures"]
            .as_u64()
            .unwrap() as u32
    );
    assert_eq!(
        m.account_keys.len(),
        r["transaction"]["message"]["accountKeys"]
            .as_array()
            .unwrap()
            .len()
    );
    assert_eq!(
        meta.pre_balances,
        serde_json::from_value::<Vec<u64>>(r["meta"]["preBalances"].clone()).unwrap()
    );
    assert_eq!(
        meta.post_balances,
        serde_json::from_value::<Vec<u64>>(r["meta"]["postBalances"].clone()).unwrap()
    );
    assert_eq!(
        meta.loaded_writable_addresses.len(),
        r["meta"]["loadedAddresses"]["writable"]
            .as_array()
            .map_or(0, Vec::len)
    );
    assert_eq!(
        meta.loaded_readonly_addresses.len(),
        r["meta"]["loadedAddresses"]["readonly"]
            .as_array()
            .map_or(0, Vec::len)
    );
    assert_eq!(
        meta.inner_instructions[0].instructions[0].stack_height,
        None
    );
    for (i, raw) in meta.inner_instructions[0].instructions.iter().zip(
        r["meta"]["innerInstructions"][0]["instructions"]
            .as_array()
            .unwrap(),
    ) {
        assert_eq!(
            bs58::encode(&i.data).into_string(),
            raw["data"].as_str().unwrap()
        );
        assert_eq!(i.stack_height.map(u64::from), raw["stackHeight"].as_u64());
    }
}

#[test]
fn failed_error_is_not_converted_to_success() {
    let mut f = fixture();
    let error = json!({"InstructionError":[2,{"Custom":6001}]});
    f["result"]["meta"]["err"] = error.clone();
    let tx = rpc::update(&f["result"]).unwrap();
    let stored = tx.transaction.unwrap().meta.unwrap().err.unwrap().err;
    assert_eq!(serde_json::from_slice::<Value>(&stored).unwrap(), error);
    let result = classify(&f, CPMM);
    assert_eq!(result["status"], "decoder_refused");
    assert_eq!(result["reason"], "Some(Failed)");
}

#[test]
fn synthetic_cpmm_interest_control_not_real_cpmm_execution_proof() {
    // Alter only DEX IDs in a saved Pump fixture. This proves the interest filter,
    // not that PumpSwap instruction bytes are a real CPMM instruction. Native
    // attribution can change without a Pump program; no amount equivalence claim.
    let original = fixture();
    let mut f = original.clone();
    for path in [
        "/transaction/message/accountKeys",
        "/meta/loadedAddresses/writable",
        "/meta/loadedAddresses/readonly",
    ] {
        for key in f["result"].pointer_mut(path).unwrap().as_array_mut().unwrap() {
            if key == PUMP {
                *key = CPMM.into();
            }
        }
    }
    for log in f["result"]["meta"]["logMessages"].as_array_mut().unwrap() {
        *log = log.as_str().unwrap().replace(PUMP, CPMM).into();
    }
    let old = classify(&f, OLD_CPMM);
    let fixed = classify(&f, CPMM);
    assert_eq!(old["status"], "decoder_refused");
    assert_eq!(old["reason"], "Some(UninterestedProgram)");
    assert_eq!(fixed["status"], "decoded_swap");
    assert_eq!(fixed["facts"]["dex_hint"], "raydium");
    assert!(!fixed["used_program_fallback"].as_bool().unwrap());
}

#[test]
fn block_expansion_keeps_requested_slot_time_and_loaded_keys() {
    let f = fixture();
    let r = &f["result"];
    let tx = json!({"transaction":r["transaction"],"meta":r["meta"],"version":r["version"]});
    let block = json!([{"sequence":7,"method":"getBlock","params":[r["slot"]],
        "result":{"blockTime":r["blockTime"],"transactions":[tx]}}]);
    let expanded = rpc::records(&block).unwrap();
    for field in ["transaction", "meta", "version", "slot", "blockTime"] {
        assert_eq!(expanded[0]["result"][field], r[field], "field {field}");
    }
    assert_eq!(
        classify(&expanded[0], CPMM)["facts"],
        classify(&f, CPMM)["facts"]
    );
    let missing_slot = json!([{"result":{"transactions":[{}]}}]);
    assert!(rpc::records(&missing_slot).is_err());
}

#[test]
fn real_versioned_fixture_preserves_static_and_loaded_key_bytes() {
    let f = super::capture_scope_fixture::fixtures()
        .into_iter()
        .find(|f| {
            f["result"]["version"] == 0
                && !f["result"]["meta"]["loadedAddresses"]["writable"]
                    .as_array()
                    .unwrap()
                    .is_empty()
        })
        .unwrap();
    let r = &f["result"];
    let info = rpc::update(r).unwrap().transaction.unwrap();
    let m = info.transaction.unwrap().message.unwrap();
    let meta = info.meta.unwrap();
    assert!(m.versioned);
    let compare = |actual: &[Vec<u8>], raw: &Value| {
        let original = raw.as_array().unwrap();
        assert_eq!(actual.len(), original.len());
        for (a, b) in actual.iter().zip(original) {
            assert_eq!(bs58::encode(a).into_string(), b.as_str().unwrap());
        }
    };
    compare(&m.account_keys, &r["transaction"]["message"]["accountKeys"]);
    compare(
        &meta.loaded_writable_addresses,
        &r["meta"]["loadedAddresses"]["writable"],
    );
    compare(
        &meta.loaded_readonly_addresses,
        &r["meta"]["loadedAddresses"]["readonly"],
    );
    for (actual, raw) in m.instructions.iter().zip(
        r["transaction"]["message"]["instructions"]
            .as_array()
            .unwrap(),
    ) {
        assert_eq!(
            actual.program_id_index as u64,
            raw["programIdIndex"].as_u64().unwrap()
        );
        assert_eq!(
            bs58::encode(&actual.data).into_string(),
            raw["data"].as_str().unwrap()
        );
        assert_eq!(
            actual.accounts,
            serde_json::from_value::<Vec<u8>>(raw["accounts"].clone()).unwrap()
        );
    }
}
