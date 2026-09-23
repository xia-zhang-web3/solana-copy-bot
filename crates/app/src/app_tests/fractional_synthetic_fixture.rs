//! Public synthetic provider operands for fractional tests; b135 is checked in.
use anyhow::{Context, Result};
use copybot_storage_core::ordered_sell_quote::fractional::inventory::{
    Evidence, Page, TOKEN, TOKEN22,
};
use serde_json::{json, Value};

fn inputs() -> std::path::PathBuf {
    super::b135_fixture::inputs()
}
pub(super) fn prefix_inputs() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src/app_tests/fixtures/fractional-prefix")
}
fn transfer(raw: u64) -> String {
    let mut data = vec![3_u8];
    data.extend_from_slice(&raw.to_le_bytes());
    bs58::encode(data).into_string()
}
fn token_account(address: &str, wallet: &str, mint: &str, raw: &str) -> Value {
    json!({"pubkey":address,"account":{"owner":TOKEN,"data":{"parsed":{
        "type":"account","info":{"owner":wallet,"mint":mint,
        "tokenAmount":{"amount":raw,"decimals":3}}}}}})
}
fn balance(wallet: &str, mint: &str, raw: &str) -> Value {
    json!({"accountIndex":5,"owner":wallet,"mint":mint,"programId":TOKEN,
        "uiTokenAmount":{"amount":raw,"decimals":3}})
}

pub(super) fn evidence(prefix_inflow: bool) -> Result<Evidence> {
    let meta: Value = serde_json::from_slice(&std::fs::read(inputs().join("chain.json"))?)?;
    let mut target: Value =
        serde_json::from_slice(&std::fs::read(inputs().join("sell-rpc.json"))?)?;
    let wallet = meta["source"]["signer"].as_str().context("source wallet")?;
    let ours = meta["our"]["signer"].as_str().context("execution wallet")?;
    let mint = meta["sell"]["token_in"].as_str().context("sell mint")?;
    let keys: Vec<String> = target["transaction"]["message"]["accountKeys"]
        .as_array()
        .context("account keys")?
        .iter()
        .map(|v| {
            v["pubkey"]
                .as_str()
                .context("account pubkey")
                .map(str::to_owned)
        })
        .collect::<Result<_>>()?;
    let swap_data = target["transaction"]["message"]["instructions"][0]["data"]
        .as_str()
        .context("public synthetic swap data")?
        .to_owned();
    target["transaction"]["message"]["accountKeys"] = json!(keys);
    target["transaction"]["message"]["header"] = json!({
        "numRequiredSignatures":1,"numReadonlySignedAccounts":0,
        "numReadonlyUnsignedAccounts":0});
    target["transaction"]["message"]["instructions"] = json!([{
        "programIdIndex":9,
        "accounts":[1,0,2,3,4,5,6,7,8,1,2,3,4,5,6,7,8,1,2],
        "data":swap_data}]);
    target["meta"]["innerInstructions"] = json!([{"index":0,"instructions":[{
        "programIdIndex":10,"accounts":[5,8,0],
        "data":transfer(10_000),"stackHeight":2}]}]);
    target["meta"]["logMessages"] =
        json!(["Program pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA success"]);
    target["meta"]["preTokenBalances"] = json!([balance(
        wallet,
        mint,
        if prefix_inflow { "10001" } else { "10000" }
    )]);
    target["meta"]["postTokenBalances"] =
        json!([balance(wallet, mint, if prefix_inflow { "1" } else { "0" })]);

    // The earlier transaction adds one source token only in the prefix case.
    // The verifier must include that movement in D before the target SELL.
    let mut earlier = target.clone();
    earlier["transaction"]["signatures"] = json!([bs58::encode([3_u8; 64]).into_string()]);
    earlier["transaction"]["message"]["instructions"] = json!([{
        "programIdIndex":10,"accounts":[8,5,0],
        "data":transfer(u64::from(prefix_inflow))}]);
    earlier["meta"]["innerInstructions"] = json!([]);
    earlier["meta"]["logMessages"] = json!([]);
    earlier["meta"]["preTokenBalances"] = json!([balance(wallet, mint, "10000")]);
    earlier["meta"]["postTokenBalances"] = json!([balance(
        wallet,
        mint,
        if prefix_inflow { "10001" } else { "10000" }
    )]);
    let source = token_account(&keys[5], wallet, mint, "10000");
    let second = token_account(&keys[7], wallet, mint, "30000");
    let own = token_account(&keys[6], ours, mint, "1000");
    Ok(Evidence {
        version: 1,
        slot: 150,
        block: json!({"parentSlot":120,
            "blockhash":"B8qLUArXhjYyvaX8RovpeJmRcpZC2XyzpCyRk1is4yP3",
            "previousBlockhash":"97GUB8tDxneBjs1uLr9Fv3cYJ4F9pdPQ3xyjzcNteyhq",
            "transactions":if prefix_inflow {vec![earlier,target]}
                else {vec![target]}}),
        parent: json!({"blockhash":"97GUB8tDxneBjs1uLr9Fv3cYJ4F9pdPQ3xyjzcNteyhq"}),
        pages: vec![
            Page {
                program: TOKEN22.into(),
                cursor: None,
                response: json!({"context":{"slot":120},"value":[],"pageKey":null}),
            },
            Page {
                program: TOKEN.into(),
                cursor: None,
                response: json!({"context":{"slot":120},"value":[source,second],"pageKey":null}),
            },
        ],
        execution_accounts: json!({"context":{"slot":150},"value":[own]}),
    })
}

pub(super) fn capacity_body(number: usize) -> Result<Vec<u8>> {
    anyhow::ensure!((1..=12).contains(&number), "capacity_fixture_number");
    // 2.56–6.52 MB; capacity/envelope only, never a semantic block proof.
    let bytes = 2_560_000 + (number - 1) * 360_000;
    Ok(serde_json::to_vec(&json!({"jsonrpc":"2.0","id":number,
        "result":{"transactions":[{"synthetic":true}],
        "capacity_padding":"x".repeat(bytes)}}))?)
}
