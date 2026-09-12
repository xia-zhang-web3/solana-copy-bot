use super::generic_buy_loopback::*;
use super::generic_buy_test_support::*;
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};

#[tokio::test]
async fn batch111_malformed_bundle_refusals_are_terminal() -> Result<()> {
    let original: Value = serde_json::from_str(INSTRUCTIONS)?;
    let mut cases = Vec::new();
    for field in [
        "tokenLedgerInstruction",
        "computeBudgetInstructions",
        "setupInstructions",
        "swapInstruction",
        "cleanupInstruction",
        "otherInstructions",
        "addressLookupTableAddresses",
        "blockhashWithMetadata",
    ] {
        let mut v = original.clone();
        v.as_object_mut().unwrap().remove(field);
        cases.push((format!("missing-{field}"), v));
    }
    for (pointer, field) in [
        ("/swapInstruction", "programId"),
        ("/swapInstruction", "accounts"),
        ("/swapInstruction", "data"),
        ("/swapInstruction/accounts/0", "pubkey"),
        ("/swapInstruction/accounts/0", "isSigner"),
        ("/swapInstruction/accounts/0", "isWritable"),
        ("/blockhashWithMetadata", "blockhash"),
        ("/blockhashWithMetadata", "lastValidBlockHeight"),
        ("/blockhashWithMetadata", "fetchedAt"),
    ] {
        let mut v = original.clone();
        v.pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove(field);
        cases.push((format!("missing-{pointer}-{field}"), v));
    }
    for path in [
        "/computeBudgetInstructions",
        "/setupInstructions",
        "/otherInstructions",
        "/addressLookupTableAddresses",
        "/swapInstruction",
        "/swapInstruction/accounts",
        "/swapInstruction/accounts/0/pubkey",
        "/swapInstruction/accounts/0/isSigner",
        "/swapInstruction/accounts/0/isWritable",
        "/swapInstruction/programId",
        "/swapInstruction/data",
        "/blockhashWithMetadata",
        "/blockhashWithMetadata/blockhash",
        "/blockhashWithMetadata/fetchedAt",
    ] {
        let mut v = original.clone();
        *v.pointer_mut(path).unwrap() = Value::Null;
        cases.push((format!("null-{path}"), v));
    }
    for (path, bad) in [
        ("/swapInstruction/accounts/0/isSigner", json!("true")),
        ("/swapInstruction/accounts/0/isWritable", json!(1)),
        ("/swapInstruction/programId", json!("111")),
        ("/swapInstruction/accounts/0/pubkey", json!("0".repeat(44))),
        ("/swapInstruction/data", json!("Missing token program")),
        ("/cleanupInstruction", json!([])),
        ("/tokenLedgerInstruction", json!([])),
        ("/setupInstructions", json!({})),
        ("/otherInstructions", json!(false)),
        ("/addressLookupTableAddresses/0", json!("bad")),
        ("/addressesByLookupTableAddress", json!([])),
        ("/blockhashWithMetadata/blockhash/0", json!(256)),
        ("/blockhashWithMetadata/blockhash/0", json!(-1)),
        ("/blockhashWithMetadata/blockhash/0", json!(1.5)),
        ("/blockhashWithMetadata/blockhash", json!(vec![0; 31])),
        ("/blockhashWithMetadata/lastValidBlockHeight", json!("1")),
        (
            "/blockhashWithMetadata/fetchedAt/nanos_since_epoch",
            json!(1_000_000_000),
        ),
    ] {
        let mut v = original.clone();
        *v.pointer_mut(path).unwrap() = bad;
        cases.push((format!("malformed-{path}"), v));
    }
    let mut v = original.clone();
    v["otherInstructions"] = json!([v["cleanupInstruction"].clone()]);
    cases.push(("unsupported-other".into(), v));
    let mut v = original.clone();
    v["tokenLedgerInstruction"] = v["cleanupInstruction"].clone();
    cases.push(("unsupported-ledger".into(), v));
    let mut v = original.clone();
    v["additionalInstructions"] = json!([]);
    cases.push(("unknown-group".into(), v));
    let mut v = original.clone();
    v["swapInstruction"]["extraData"] = json!("not enough accounts");
    cases.push(("unknown-instruction-field".into(), v));
    let mut v = original.clone();
    v["swapInstruction"]["accounts"][0]["privileges"] = json!({});
    cases.push(("unknown-meta-field".into(), v));
    let mut v = original.clone();
    v["swapInstruction"]["accounts"][1]["isSigner"] = json!(true);
    cases.push(("extra-signer".into(), v));
    let mut v = original.clone();
    v["setupInstructions"] = json!(vec![v["setupInstructions"][0].clone(); 65]);
    cases.push(("array-limit".into(), v));
    let mut v = original.clone();
    v["swapInstruction"]["accounts"] =
        json!(vec![v["swapInstruction"]["accounts"][0].clone(); 257]);
    cases.push(("key-count-limit".into(), v));
    let mut v = original.clone();
    v["swapInstruction"]["data"] = json!(STANDARD.encode(vec![0; 1233]));
    cases.push(("data-limit".into(), v));
    let mut v = original.clone();
    v["swapInstruction"]["data"] = json!("A".repeat(140_000));
    cases.push(("response-limit".into(), v));
    let mut v = original.clone();
    v["setupInstructions"].as_array_mut().unwrap().push(json!({
        "programId":"11111111111111111111111111111111","accounts":[
            {"pubkey":super::generic_buy_fixture::PAYER,"isSigner":false,"isWritable":false},
            {"pubkey":super::generic_buy_fixture::PAYER,"isSigner":false,"isWritable":false}],
        "data":STANDARD.encode([2u32.to_le_bytes().as_slice(),50_000_001u64.to_le_bytes().as_slice()].concat())}));
    cases.push(("duplicate-floor-with-unpromoted-flags".into(), v));
    let mut v = original.clone();
    v["computeBudgetInstructions"][1]["data"] =
        json!(STANDARD.encode([vec![3], 200_000u64.to_le_bytes().to_vec()].concat()));
    cases.push(("encoded-fee-over-cap".into(), v));
    for (case, value) in cases {
        let r = run(
            Replies {
                instructions: value.to_string(),
                ..Default::default()
            },
            "buy",
            |_| {},
            |_| {},
        )
        .await?;
        r.assert_rejected(&case, 0);
    }
    Ok(())
}

#[tokio::test]
async fn batch111_simulation_refusal_timeout_and_provider_success_are_terminal() -> Result<()> {
    let healthy: Value = serde_json::from_str(&Replies::default().simulation)?;
    let mut cases = Vec::new();
    for (path, bad) in [
        (
            "/result/value/err",
            json!({"InstructionError":[8,{"Custom":1}]}),
        ),
        ("/result/context/slot", json!("446128191")),
        ("/id", json!("wrong-id")),
    ] {
        let mut value = healthy.clone();
        *value.pointer_mut(path).unwrap() = bad;
        cases.push(Replies {
            simulation: value.to_string(),
            ..Default::default()
        });
    }
    let mut missing = healthy;
    missing["result"]["value"]
        .as_object_mut()
        .unwrap()
        .remove("err");
    cases.push(Replies {
        simulation: missing.to_string(),
        ..Default::default()
    });
    cases.push(Replies {
        simulation_status: 503,
        ..Default::default()
    });
    cases.push(Replies {
        simulation: "not json".into(),
        ..Default::default()
    });
    cases.push(Replies {
        simulation_delay_ms: 750,
        ..Default::default()
    });
    for (i, replies) in cases.into_iter().enumerate() {
        let r = run(replies, "buy", |c| c.quote_canary_timeout_ms = 500, |_| {}).await?;
        r.assert_rejected(&format!("simulation-{i}"), 1);
        super::generic_buy_decode::verify(
            &r.server.simulations()[0],
            &serde_json::from_str(INSTRUCTIONS)?,
            super::generic_buy_fixture::PAYER,
            50_000_001,
        );
    }
    Ok(())
}
