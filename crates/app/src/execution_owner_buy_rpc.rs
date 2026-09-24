//! Owner intent chain binding and classic SPL mint proof before a fresh quote.
use anyhow::{ensure, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use copybot_storage_core::OwnerTechnicalBuyIntent;
use serde_json::{json, Value};
use std::time::Duration;

const CLASSIC_SPL: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

pub(crate) async fn verify(
    http: &reqwest::Client,
    rpc_url: &str,
    timeout_ms: u64,
    intent: &OwnerTechnicalBuyIntent,
    check: &impl Fn() -> Result<()>,
) -> Result<u8> {
    let url = reqwest::Url::parse(rpc_url).context("owner_buy_rpc_url")?;
    ensure!(url.scheme() == "https"
        || (url.scheme() == "http" && matches!(url.host_str(),
            Some("127.0.0.1" | "localhost" | "[::1]"))),
        "owner_buy_rpc_transport");
    check()?;
    let genesis = read(http, &url, timeout_ms, json!({"jsonrpc":"2.0",
        "id":"owner-buy-genesis-v1","method":"getGenesisHash","params":[]})).await?;
    check()?;
    ensure!(genesis.as_str() == Some(intent.genesis_hash.as_str()),
        "owner_buy_genesis_mismatch");
    let account = read(http, &url, timeout_ms, json!({"jsonrpc":"2.0",
        "id":"owner-buy-mint-v1","method":"getAccountInfo",
        "params":[intent.mint,{"encoding":"base64","commitment":"finalized"}]})).await?;
    check()?;
    classic_mint_decimals(&account)
}

async fn read(
    http: &reqwest::Client,
    url: &reqwest::Url,
    timeout_ms: u64,
    request: Value,
) -> Result<Value> {
    let response: Value = http.post(url.clone()).json(&request)
        .timeout(Duration::from_millis(timeout_ms.max(1))).send().await?
        .error_for_status()?.json().await?;
    ensure!(response["jsonrpc"] == "2.0"
        && response["id"] == request["id"] && response.get("error").is_none(),
        "owner_buy_rpc_binding");
    response.get("result").filter(|v| !v.is_null()).cloned()
        .context("owner_buy_rpc_result_missing")
}

pub(crate) fn classic_mint_decimals(account: &Value) -> Result<u8> {
    ensure!(account["context"]["slot"].as_u64().is_some_and(|v| v > 0),
        "owner_buy_mint_slot");
    let value = &account["value"];
    ensure!(value["owner"].as_str() == Some(CLASSIC_SPL)
        && value["executable"] == false
        && value["data"][1] == "base64",
        "owner_buy_classic_spl_only");
    let raw = value["data"][0].as_str().context("owner_buy_mint_data_missing")?;
    let data = STANDARD.decode(raw).context("owner_buy_mint_data_invalid")?;
    // The classic SPL Mint account is exactly 82 bytes. Extra extension data
    // is rejected even if the provider mislabels its program owner.
    ensure!(data.len() == 82 && data[45] == 1, "owner_buy_classic_spl_only");
    Ok(data[44])
}
