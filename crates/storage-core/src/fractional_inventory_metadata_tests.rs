//! Exact mixed-version prefix effects are independent of signal eligibility.
use super::{rows, QuoteBinding, TOKEN};
use serde_json::{json, Value};
use std::collections::BTreeMap;
const WALLET: &str = "7EoQc9N9QrGMf6JR2j8yy4rmY1ZsexPuAXoTCQtDDbSF";
const ACCOUNT: &str = "eNQee2A5Bsx56JJMs3LBezwkBNeqvt8P98fesn43yLD";
const MINT: &str = "DVb1znJKBVJzcuzbgvcG3cSghf2i1YzdJqoJFb7ZdQuX";
const SIG: &str =
    "5qUFxPr8EZusxeKyDUWeDvRtkfJtjiYNyNqAQBxC442SvDFQsG6cebbrZvkf9aeTmiMgtBULYGBsagaXyDmi6oD4";
fn binding() -> QuoteBinding {
    QuoteBinding {
        version: 1,
        intent_id: "test".into(),
        policy: "test".into(),
        position_id: "test".into(),
        position_opened_ts: "test".into(),
        source_signature: SIG.into(),
        source_wallet: WALLET.into(),
        mint: MINT.into(),
        output_mint: "So11111111111111111111111111111111111111112".into(),
        side: "sell".into(),
        provider: "offline".into(),
        endpoint: "offline".into(),
        raw: 100,
        decimals: 9,
        fractional: None,
        snapshot_version: "test".into(),
    }
}
fn transaction(before: u64, after: u64) -> Value {
    let row = |raw: u64| {
        json!({"accountIndex":1,"owner":WALLET,"mint":MINT,
        "programId":TOKEN,"uiTokenAmount":{"amount":raw.to_string(),"decimals":9}})
    };
    json!({"version":1,"transaction":{"signatures":[SIG],"message":{
        "accountKeys":[WALLET,ACCOUNT,MINT,TOKEN],
        "header":{"numRequiredSignatures":1,"numReadonlySignedAccounts":0,
            "numReadonlyUnsignedAccounts":2},"recentBlockhash":MINT,
        "instructions":[],"transactionConfig":{"priorityFee":0}}},
        "meta":{"err":null,"fee":5000,"innerInstructions":[],
            "preBalances":[1,1,1,1],"postBalances":[1,1,1,1],
            "preTokenBalances":[row(before)],"postTokenBalances":[row(after)]}})
}
#[test]
fn mixed_prefix_exact_effects_are_counted_and_unknowns_refuse() -> anyhow::Result<()> {
    let b = binding();
    for (before, after) in [(1000, 1200), (1000, 700), (1000, 1000)] {
        let mut held = BTreeMap::from([(ACCOUNT.into(), before)]);
        rows::advance(&transaction(before, after), &b, &mut held)?;
        assert_eq!(held[ACCOUNT], after);
    }
    for fault in [
        "missing_metadata",
        "wrong_owner",
        "wrong_mint",
        "pre_conflict",
        "failed_change",
        "readonly_change",
        "unknown_version",
    ] {
        let mut t = transaction(1000, 1200);
        match fault {
            "missing_metadata" => {
                t["meta"]["preTokenBalances"] = json!([]);
                t["meta"]["postTokenBalances"] = json!([]);
            }
            "wrong_owner" => t["meta"]["postTokenBalances"][0]["owner"] = json!(MINT),
            "wrong_mint" => t["meta"]["postTokenBalances"][0]["mint"] = json!(WALLET),
            "pre_conflict" => {
                t["meta"]["preTokenBalances"][0]["uiTokenAmount"]["amount"] = json!("999")
            }
            "failed_change" => t["meta"]["err"] = json!({"failure":true}),
            "readonly_change" => {
                t["transaction"]["message"]["header"]["numReadonlyUnsignedAccounts"] = json!(3)
            }
            "unknown_version" => t["version"] = json!(2),
            _ => unreachable!(),
        }
        let mut held = BTreeMap::from([(ACCOUNT.into(), 1000)]);
        assert!(rows::advance(&t, &b, &mut held).is_err(), "{fault}");
        assert_eq!(
            held[ACCOUNT], 1000,
            "unknown cannot manufacture an inventory"
        );
    }
    Ok(())
}
#[test]
fn config_marker_is_never_a_financial_target_even_with_legacy_tag() {
    for version in [json!("legacy"), json!(0), json!(1)] {
        for config in [Value::Null, json!({})] {
            let mut t = transaction(1000, 1000);
            t["version"] = version.clone();
            t["transaction"]["message"]["transactionConfig"] = config;
            assert_eq!(
                rows::balances(&t, &binding()).unwrap_err().to_string(),
                "fraction_transaction_config"
            );
        }
    }
}
#[test]
fn unrelated_v1_missing_metadata_is_not_declared_neutral() {
    let mut t = transaction(1000, 1000);
    t["meta"]["preTokenBalances"] = Value::Null;
    let mut held = BTreeMap::from([(ACCOUNT.into(), 1000)]);
    assert!(rows::advance(&t, &binding(), &mut held).is_err());
}
