//! Fresh on-chain owner balance and USDC ExactIn quote for one position.
use crate::execution_quote_canary_helpers::{DECISION_WOULD_EXECUTE, QUOTE_STATUS_OK, SOL_MINT};
use crate::execution_submit_adapter::ExecutionBuildPlanMetadata;
use anyhow::{ensure, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::Utc;
use copybot_config::ExecutionConfig;
use copybot_storage_core::OwnerExitIntent;
use serde_json::{json, Value};
use std::time::Duration;

const CLASSIC_SPL: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

pub(crate) async fn verify_balance(
    http: &reqwest::Client, config: &ExecutionConfig, intent: &OwnerExitIntent,
    check: &impl Fn() -> Result<()>,
) -> Result<()> {
    let url = reqwest::Url::parse(&config.submit_adapter_http_url)?;
    ensure!(url.scheme() == "https" || (url.scheme() == "http"
        && matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "[::1]"))),
        "owner_exit_rpc_transport");
    let read = |id: &'static str, method: &'static str, params: Value| {
        let url = url.clone(); let http = http.clone();
        let timeout = config.submit_timeout_ms;
        async move {
            let request = json!({"jsonrpc":"2.0","id":id,"method":method,"params":params});
            let response: Value = http.post(url).json(&request)
                .timeout(Duration::from_millis(timeout.max(1))).send().await?
                .error_for_status()?.json().await?;
            ensure!(response["jsonrpc"] == "2.0" && response["id"] == id
                && response.get("error").is_none(), "owner_exit_rpc_binding");
            response.get("result").filter(|v| !v.is_null()).cloned()
                .context("owner_exit_rpc_result_missing")
        }
    };
    check()?;
    let genesis = read("owner-exit-genesis", "getGenesisHash", json!([])).await?;
    check()?;
    ensure!(genesis.as_str() == Some(intent.genesis_hash.as_str()),
        "owner_exit_genesis_mismatch");
    let wallet = crate::execution_pumpswap_accounts::parse_pubkey(&intent.wallet,
        "owner_exit_wallet")?;
    let mint = crate::execution_pumpswap_accounts::parse_pubkey(&intent.mint,
        "owner_exit_mint")?;
    let ata = crate::execution_pumpswap_accounts::associated_token_address(
        &wallet, &mint, &crate::execution_pumpswap_accounts::token_program_id());
    let address = bs58::encode(ata).into_string();
    let account = read("owner-exit-ata", "getAccountInfo",
        json!([address,{"encoding":"base64","commitment":"confirmed"}])).await?;
    check()?;
    ensure!(account["context"]["slot"].as_u64().is_some_and(|v| v > 0)
        && account["value"]["owner"] == CLASSIC_SPL
        && account["value"]["executable"] == false
        && account["value"]["data"][1] == "base64", "owner_exit_ata_identity");
    let data = STANDARD.decode(account["value"]["data"][0].as_str()
        .context("owner_exit_ata_data")?)?;
    ensure!(data.len() == 165 && data[..32] == mint && data[32..64] == wallet,
        "owner_exit_ata_owner_or_mint");
    let amount = u64::from_le_bytes(data[64..72].try_into()?);
    ensure!(amount == intent.amount_raw, "owner_exit_wallet_amount_changed");
    // The route closes the wallet's WSOL ATA. An existing account could
    // contribute a WSOL balance or rent refund unrelated to this SELL.
    let wsol = crate::execution_pumpswap_accounts::wsol_mint();
    let wsol_ata = crate::execution_pumpswap_accounts::associated_token_address(
        &wallet, &wsol, &crate::execution_pumpswap_accounts::token_program_id());
    let wsol_account = read("owner-exit-wsol-ata", "getAccountInfo",
        json!([bs58::encode(wsol_ata).into_string(),
            {"encoding":"base64","commitment":"confirmed"}])).await?;
    check()?;
    ensure!(wsol_account["context"]["slot"].as_u64().is_some_and(|v| v > 0),
        "owner_exit_wsol_context");
    ensure!(wsol_account["value"].is_null(), "owner_exit_wsol_account_present");
    let mint_account = read("owner-exit-mint", "getAccountInfo",
        json!([intent.mint,{"encoding":"base64","commitment":"finalized"}])).await?;
    check()?;
    ensure!(crate::execution_owner_buy_rpc::classic_mint_decimals(&mint_account)?
        == intent.decimals, "owner_exit_decimals_changed");
    Ok(())
}

pub(crate) async fn fetch(
    http: &reqwest::Client, config: &ExecutionConfig, intent: &OwnerExitIntent,
) -> Result<ExecutionBuildPlanMetadata> {
    let sample = crate::execution_quote_http::fetch_owner_exit_quote_sample(
        http, config, &intent.mint,
        &intent.amount_raw.to_string(), u64::from(intent.max_slippage_bps),
    ).await.map_err(|e| anyhow::anyhow!("owner_exit_fresh_quote: {e}"))?;
    let mut metadata = metadata(intent, sample)?;
    let fee = crate::execution_quote_canary_priority_fee::PriorityFeeSampler::new(
        config.clone(), http.clone()).sample_if_enabled().await;
    match fee {
        Some(sample) => {
            ensure!(sample.status == QUOTE_STATUS_OK, "owner_exit_priority_sample");
            metadata.priority_fee_source = Some("owner_exit_live_sample".into());
            metadata.priority_fee_status = Some(sample.status);
            metadata.priority_fee_lamports = sample.lamports;
            metadata.priority_fee_json = sample.json;
        }
        None => {
            metadata.priority_fee_source = Some("owner_exit_zero_priority".into());
            metadata.priority_fee_status = Some(QUOTE_STATUS_OK.into());
            metadata.priority_fee_lamports = Some(0);
            metadata.priority_fee_json = Some(json!({"version":1,
                "source":"owner_exit_zero_priority","unit":"total_priority_fee_lamports",
                "value":0}).to_string());
        }
    }
    let metadata = crate::execution_priority_fee::cap_metadata_total(
        intent.max_priority_fee_lamports, metadata);
    crate::execution_priority_fee::metadata_fee(&metadata)?;
    Ok(metadata)
}

pub(crate) fn metadata(intent: &OwnerExitIntent,
    quote: crate::execution_quote_canary_helpers::QuoteSample,
) -> Result<ExecutionBuildPlanMetadata> {
    let raw: Value = serde_json::from_str(&quote.response_json)?;
    ensure!(raw["inputMint"] == intent.mint && raw["outputMint"] == SOL_MINT
        && raw["inAmount"] == intent.amount_raw.to_string()
        && quote.in_amount == intent.amount_raw.to_string()
        && raw["outAmount"] == quote.out_amount
        && raw["swapMode"] == "ExactIn"
        && raw["slippageBps"].as_u64() == Some(u64::from(intent.max_slippage_bps))
        && raw.get("platformFee").is_none_or(|v| v.is_null() || v["feeBps"] == 0),
        "owner_exit_quote_binding");
    let output = quote.out_amount.parse::<u64>()?;
    let threshold = raw["otherAmountThreshold"].as_str()
        .context("owner_exit_threshold")?.parse::<u64>()?;
    let minimum = u128::from(output) * u128::from(10_000 - intent.max_slippage_bps) / 10_000;
    ensure!(output > 0 && threshold > 0 && threshold <= output
        && u128::from(threshold) >= minimum, "owner_exit_quote_slippage");
    let route = raw["routePlan"].as_array().context("owner_exit_route_missing")?;
    ensure!(route.len() == 1 && (route[0]["percent"] == 100 || route[0]["bps"] == 10_000)
        && route[0]["swapInfo"]["label"] == "Raydium"
        && route[0]["swapInfo"]["inputMint"] == intent.mint
        && route[0]["swapInfo"]["outputMint"] == SOL_MINT,
        "owner_exit_route_unsupported");
    let available = quote.quote_response_available_ts.context("owner_exit_quote_time")?;
    ensure!((0..=30).contains(&Utc::now().signed_duration_since(available).num_seconds()),
        "owner_exit_quote_stale");
    let event_id = format!("owner-exit:{}:quote:{}", intent.intent_id,
        crate::execution_owned_sell_rpc::digest(quote.response_json.as_bytes()));
    Ok(ExecutionBuildPlanMetadata {
        quote_event_id: Some(event_id), quote_request_ts: quote.http_request_started_ts,
        quote_response_available_ts: Some(available),
        http_request_started_ts: quote.http_request_started_ts,
        quote_source: Some("owner_exit_generic_metis".into()),
        quote_status: Some(QUOTE_STATUS_OK.into()),
        quote_in_amount_raw: Some(quote.in_amount),
        quote_out_amount_raw: Some(quote.out_amount),
        quote_response_json: Some(quote.response_json),
        quote_price_sol: Some(output as f64 / 1_000_000_000.0
            / (intent.amount_raw as f64 / 10_f64.powi(i32::from(intent.decimals)))),
        price_impact_pct: quote.price_impact_pct,
        route_plan_json: quote.route_plan_json,
        slippage_bps: Some(f64::from(intent.max_slippage_bps)),
        decision_status: Some(DECISION_WOULD_EXECUTE.into()),
        decision_reason: Some("owner_exit_fresh_quote_within_limit".into()),
        ..Default::default()
    })
}
