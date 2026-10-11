//! Historical receipt verification only, never a live freshness/quote assertion.
use anyhow::Result;
use copybot_core_types::{association_delivery::*, ExactSwapAmounts};
use serde_json::{json, Value};
fn fixture(parsed: bool) -> Value {
    serde_json::from_str(if parsed {
        include_str!("../../../storage-core/tests/fixtures/canary31-source-sell-parsed.json")
    } else {
        include_str!("../../../storage-core/tests/fixtures/canary31-source-sell-raw.json")
    })
    .unwrap()
}
fn admission(t: &Value) -> AdmissionFacts {
    AdmissionFacts {
        facts: CheckedFacts {
            signature: t["transaction"]["signatures"][0].as_str().unwrap().into(),
            slot: t["slot"].as_u64().unwrap(),
            wallet: "BwVw8ncEpWU7TwMTgysvwjQ85eEhKAMVbd7WU1iTE9Mk".into(),
            token_in: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into(),
            token_out: "So11111111111111111111111111111111111111112".into(),
            amount_in_bits: 1.091636f64.to_bits(),
            amount_out_bits: 0.009948805f64.to_bits(),
            exact_amounts: Some(ExactSwapAmounts {
                amount_in_raw: "1091636".into(),
                amount_out_raw: "9948805".into(),
                amount_in_decimals: 6,
                amount_out_decimals: 9,
            }),
            programs: vec![],
            dex: "raydium".into(),
            program_fallback: false,
        },
        info: InfoIdentity {
            encoded: vec![],
            float_bits: vec![],
        },
        message_time: MessageTime::RecoveredBlock {
            block_time: t["blockTime"].as_i64(),
        },
    }
}
#[test]
fn controlled_source_receipt_historical_raw_parsed_uses_same_closed_sell_proof() -> Result<()> {
    for parsed in [true, false] {
        let t = fixture(parsed);
        let a = admission(&t);
        assert_eq!(
            crate::execution_owned_sell_rpc::decode::sell(&t, &a)?,
            455_694_767
        );
    }
    Ok(())
}
#[test]
fn controlled_source_receipt_wrong_signature_slot_amount_or_lifecycle_refuses() {
    let t = fixture(true);
    let original = admission(&t);
    let mut a = original.clone();
    a.facts.signature = bs58::encode([3; 64]).into_string();
    assert!(crate::execution_owned_sell_rpc::decode::sell(&t, &a).is_err());
    let mut a = original.clone();
    a.facts.slot += 1;
    assert!(crate::execution_owned_sell_rpc::decode::sell(&t, &a).is_err());
    let mut a = original.clone();
    a.facts.exact_amounts.as_mut().unwrap().amount_out_raw = "9948806".into();
    assert!(crate::execution_owned_sell_rpc::decode::sell(&t, &a).is_err());
    let mut a = original.clone();
    a.facts.exact_amounts.as_mut().unwrap().amount_in_raw = "1091635".into();
    assert!(crate::execution_owned_sell_rpc::decode::sell(&t, &a).is_err());
    let mut bad = t;
    bad["transaction"]["message"]["instructions"][4]["parsed"]["info"]["destination"] =
        json!("7wDVE9mGs3rmvDqbEUAZRDDwGD3uR9WxVzC23ZZgA4hr");
    assert!(crate::execution_owned_sell_rpc::decode::sell(&bad, &original).is_err());
}
