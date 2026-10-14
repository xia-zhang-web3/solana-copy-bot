//! Explicit saved39 replay. Raw RPC fixture files are external/hash-bound, never live I/O.
use super::{
    rows, Evidence, Page, ProtectedExecutionBaseline, QuoteBinding, PROTECTED_USDC_POLICY, TOKEN,
    TOKEN22, USDC,
};
use anyhow::{Context, Result};
use serde_json::Value;
use std::{collections::BTreeMap, path::PathBuf};
const SOURCE: &str = "Mrad9bhi15p5z3KmCFG9md2n4Rbd7j4SkwZb8pyZxHf";
const BOT: &str = "BwVw8ncEpWU7TwMTgysvwjQ85eEhKAMVbd7WU1iTE9Mk";
const SOURCE_ATA: &str = "GP6zUwBUtu5TJ8QA5piKQ2jHEfXKTbVUqFHVQAnuEaEF";
const BOT_ATA: &str = "HcsTb6NNeiKRuPnD4LKTyHFQzU41frBZDedQc7qCK4A3";
const SELL: &str =
    "YKHhWEutMsSpmCRNsBGvsU17ZLdJBYAk7ExSxUUzMTgVZi55cP7dcJx7c9p3zaHzF1oHJJuP4EKXxuHsctLCYmg";
fn response(n: usize) -> Result<Value> {
    let root =
        PathBuf::from(std::env::var_os("COPY39_RPC_FIXTURE").context("saved39 fixture required")?);
    let mut v: Value =
        serde_json::from_slice(&std::fs::read(root.join(format!("response-{n:06}.json")))?)?;
    Ok(v["result"].take())
}
fn binding() -> QuoteBinding {
    QuoteBinding { version:1, intent_id:format!("source-sell:{SELL}"), policy:"provider_order_strict_v1".into(),
        position_id:"saved39-position".into(),position_opened_ts:"2026-10-14T11:02:31+00:00".into(),
        source_signature:"3zu7eob6ioxqwRmcCRVMW6CoCa6LWwn9egKwmeywNm1hbMzn2fCsjbqVVrnW2gnTacwadAdZmbiJAFo9hYfiSaR3".into(),
        source_wallet:SOURCE.into(),mint:USDC.into(),output_mint:"So11111111111111111111111111111111111111112".into(),
        side:"sell".into(),provider:"generic_metis".into(),endpoint:"http://127.0.0.1:1/".into(),raw:1065935,decimals:6,fractional:None,
        snapshot_version:"saved39-test-only-binding".into() }
}
fn evidence() -> Result<Evidence> {
    Ok(Evidence {
        version: 1,
        slot: 456814720,
        block: response(18360)?,
        parent: response(18361)?,
        pages: vec![
            Page {
                program: TOKEN22.into(),
                cursor: None,
                response: response(18362)?,
            },
            Page {
                program: TOKEN.into(),
                cursor: None,
                response: response(18363)?,
            },
        ],
        execution_accounts: response(18364)?,
    })
}
#[test]
#[ignore = "explicit hash-bound private saved39 fixture, no provider I/O"]
fn copy39_saved_full_block_prefix_and_existing_jupiter_proof() -> Result<()> {
    let e = evidence()?;
    assert_eq!(e.block["transactions"].as_array().unwrap().len(), 1997);
    assert_eq!(
        e.block["transactions"][1048]["meta"]["err"],
        serde_json::json!("MaxLoadedAccountsDataSizeExceeded")
    );
    assert!(e.block["transactions"][1048]["meta"]["innerInstructions"].is_null());
    assert_eq!(
        e.block["transactions"][1067]["transaction"]["signatures"][0],
        SELL
    );
    let proof = super::verify_with_baseline(
        &e,
        &binding(),
        SELL,
        456814720,
        1065900,
        BOT,
        Some(&ProtectedExecutionBaseline {
            policy: PROTECTED_USDC_POLICY.into(),
            amount_raw: 100,
            token_account: BOT_ATA.into(),
        }),
    )?;
    assert_eq!(proof.target_index, 1067);
    assert_eq!(proof.numerator, 1065900);
    assert_eq!(proof.denominator, "1065900");
    assert_eq!(
        super::allocate(&[1065935], proof.numerator, proof.denominator.parse()?)?,
        vec![1065935]
    );
    Ok(())
}
#[test]
#[ignore = "explicit hash-bound private saved39 fixture, no provider I/O"]
fn copy39_failed_null_cpi_owned_metadata_and_shape_still_refuse() -> Result<()> {
    let block = response(18360)?;
    let neutral = block["transactions"][1048].clone();
    let mut inventory = BTreeMap::from([(SOURCE_ATA.to_owned(), 1065900)]);
    rows::advance(&neutral, &binding(), &mut inventory)?;
    assert_eq!(inventory[SOURCE_ATA], 1065900);
    for fault in [
        "owned_without_metadata",
        "missing_cpi_field",
        "successful_null_cpi",
        "missing_token_metadata",
        "malformed_cpi",
    ] {
        let mut t = neutral.clone();
        match fault {
            "owned_without_metadata" => {
                t["transaction"]["message"]["accountKeys"][0] = serde_json::json!(SOURCE_ATA)
            }
            "missing_cpi_field" => {
                t["meta"]
                    .as_object_mut()
                    .unwrap()
                    .remove("innerInstructions");
            }
            "successful_null_cpi" => t["meta"]["err"] = Value::Null,
            "missing_token_metadata" => t["meta"]["preTokenBalances"] = Value::Null,
            "malformed_cpi" => {
                t["meta"]["innerInstructions"] =
                    serde_json::json!([{"index":999999,"instructions":[]}])
            }
            _ => unreachable!(),
        }
        let before = inventory.clone();
        assert!(
            rows::advance(&t, &binding(), &mut inventory).is_err(),
            "{fault}"
        );
        assert_eq!(inventory, before, "{fault} must not mutate inventory");
    }
    let mut target = block["transactions"][1067].clone();
    target["meta"]["innerInstructions"] = Value::Null;
    assert_eq!(
        rows::balances(&target, &binding()).unwrap_err().to_string(),
        "fraction_cpi_missing"
    );
    Ok(())
}

/// Small deterministic financial-boundary case for normal CI. All rows are MOCKS;
/// the full actual39 block above remains the incident replay, not this model.
#[test]
fn failed_prefix_null_cpi_requires_complete_unchanged_owned_metadata() -> Result<()> {
    let row = |raw| {
        serde_json::json!({"accountIndex":1,"owner":SOURCE,"mint":USDC,
        "programId":TOKEN,"uiTokenAmount":{"amount":format!("{raw}"),"decimals":6}})
    };
    let neutral = serde_json::json!({"version":0,"transaction":{"signatures":[SELL],"message":{
        "accountKeys":[SOURCE,SOURCE_ATA,TOKEN],"header":{"numRequiredSignatures":1,
        "numReadonlySignedAccounts":0,"numReadonlyUnsignedAccounts":1},
        "recentBlockhash":BOT,"instructions":[]}},"meta":{"err":"MaxLoadedAccountsDataSizeExceeded",
        "fee":5000,"innerInstructions":null,"preBalances":[10000,1488440,1],
        "postBalances":[5000,1488440,1],"preTokenBalances":[row(1065900)],"postTokenBalances":[row(1065900)]}});
    let mut inventory = BTreeMap::from([(SOURCE_ATA.to_owned(), 1065900)]);
    rows::advance(&neutral, &binding(), &mut inventory)?;
    assert_eq!(inventory[SOURCE_ATA], 1065900);
    for (pre, post, reason) in [
        (
            serde_json::json!([]),
            serde_json::json!([]),
            "fraction_prefix_owned_metadata_missing",
        ),
        (
            serde_json::json!([row(1065900)]),
            serde_json::json!([row(1065899)]),
            "fraction_failed_change",
        ),
        (
            Value::Null,
            serde_json::json!([row(1065900)]),
            "fraction_token_balances",
        ),
    ] {
        let mut t = neutral.clone();
        t["meta"]["preTokenBalances"] = pre;
        t["meta"]["postTokenBalances"] = post;
        let before = inventory.clone();
        assert_eq!(
            rows::advance(&t, &binding(), &mut inventory)
                .unwrap_err()
                .to_string(),
            reason
        );
        assert_eq!(inventory, before);
    }
    assert_eq!(
        rows::balances(&neutral, &binding())
            .unwrap_err()
            .to_string(),
        "fraction_cpi_missing"
    );
    let mut successful = neutral.clone();
    successful["meta"]["err"] = Value::Null;
    assert_eq!(
        rows::advance(&successful, &binding(), &mut inventory)
            .unwrap_err()
            .to_string(),
        "fraction_cpi_missing"
    );
    let mut missing = neutral.clone();
    missing["meta"]
        .as_object_mut()
        .unwrap()
        .remove("innerInstructions");
    assert_eq!(
        rows::advance(&missing, &binding(), &mut inventory)
            .unwrap_err()
            .to_string(),
        "fraction_cpi_missing"
    );
    Ok(())
}
