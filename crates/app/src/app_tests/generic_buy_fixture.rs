//! Historical110 input identity. Synthetic controls only; no fresh timestamps or keys.
use crate::execution_submit_adapter::*;
use anyhow::{ensure, Result};
use copybot_config::ExecutionConfig;
use serde_json::{json, Value};

pub(super) const PAYER: &str = "FniEKNLfrjD1mJJkXwtLRW6iHNnFKpErgjvHxJmgrbT1";
pub(super) const TOKEN: &str = "DwVyHKdjuzK1MYRpUn4B2bfQ2VMsiv5t3o3dHBF8pump";
pub(super) const WSOL: &str = "So11111111111111111111111111111111111111112";

pub(super) fn config(base: &str) -> ExecutionConfig {
    ExecutionConfig {
        enabled: false,
        canary_tiny_submit_enabled: true,
        canary_wallet_pubkey: PAYER.into(),
        execution_signer_pubkey: PAYER.into(),
        execution_signer_keypair_path: String::new(),
        pretrade_min_sol_reserve: 0.05,
        pretrade_max_priority_fee_lamports: 22_000,
        quote_canary_base_url: format!("{base}/swap/v1"),
        quote_canary_api_key: String::new(),
        quote_canary_timeout_ms: 10_000,
        quote_canary_pump_fun_parallel_enabled: false,
        swap_instructions_dry_run_enabled: true,
        swap_transaction_dry_run_enabled: true,
        submit_adapter_http_url: format!("{base}/rpc"),
        ..Default::default()
    }
}

pub(super) fn pair(side: &str) -> (&'static str, &'static str, &'static str) {
    match side {
        "buy" => (WSOL, TOKEN, "10000000"),
        "sell" => (TOKEN, WSOL, "123456"),
        _ => panic!("unselected direction"),
    }
}

pub(super) fn request(side: &str, quote: Value) -> Result<ExecutionSubmitRequest> {
    let (input, output, amount) = pair(side);
    ensure!(quote["inputMint"] == input && quote["outputMint"] == output);
    ensure!(quote["inAmount"] == amount && quote["swapMode"] == "ExactIn");
    ensure!(quote["slippageBps"] == 500 && quote["otherAmountThreshold"].is_string());
    let sample = crate::execution_quote_http::quote_sample_from_json(quote)?;
    Ok(ExecutionSubmitRequest {
        order_id: format!("batch110-{side}"),
        signal_id: format!("batch110-signal-{side}"),
        client_order_id: format!("batch110-client-{side}"),
        attempt: 1,
        route: "metis-canary".into(),
        wallet_id: "diagnostic-no-inventory".into(),
        token: TOKEN.into(),
        side: side.into(),
        buy_size_sol: 0.01,
        slippage_tolerance_bps: 500,
        wallet_pubkey: PAYER.into(),
        entry_route_plan_json: None,
        metadata: ExecutionBuildPlanMetadata {
            quote_source: Some(
                crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_METIS.into(),
            ),
            quote_event_id: Some(format!("batch110-quote-{side}")),
            quote_status: Some("ok".into()),
            quote_in_amount_raw: Some(sample.in_amount),
            quote_out_amount_raw: Some(sample.out_amount),
            quote_response_json: Some(sample.response_json),
            route_plan_json: sample.route_plan_json,
            price_impact_pct: sample.price_impact_pct,
            slippage_bps: Some(500.0),
            priority_fee_source: Some("batch110-frozen-diagnostic-input".into()),
            priority_fee_status: Some("ok".into()),
            priority_fee_lamports: Some(22_000),
            priority_fee_json: Some(
                json!({"version":1,"source":"batch110-frozen-diagnostic-input",
                "unit":"total_priority_fee_lamports","value":22000})
                .to_string(),
            ),
            ..Default::default()
        },
    })
}

pub(super) fn synthetic_quote(side: &str) -> Value {
    let (input, output, amount) = pair(side);
    json!({"inputMint":input,"outputMint":output,"inAmount":amount,"outAmount":"100",
        "otherAmountThreshold":"95","swapMode":"ExactIn","slippageBps":500,
        "routePlan":[{"swapInfo":{"label":"synthetic-control-only"}}]})
}
