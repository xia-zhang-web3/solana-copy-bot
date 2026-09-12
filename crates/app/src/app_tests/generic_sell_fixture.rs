//! Exact recorded R4 quote; offline public-holder fixture, not owned inventory.
use crate::execution_submit_adapter::*;
use anyhow::Result;
use copybot_config::ExecutionConfig;
use serde_json::{json, Value};

pub(super) const PAYER: &str = "6GtToqWErEXWq3nhNuGYBuL2bRJeaUbzXvMTm7L4PASf";
pub(super) const QUOTE: &str = include_str!("generic_sell_fixtures/quote.json");
pub(super) const INSTRUCTIONS: &str = include_str!("generic_sell_fixtures/instructions.json");
pub(super) const REQUEST: &str = include_str!("generic_sell_fixtures/request.json");
pub(super) const OLD_SWAP: &str = include_str!("generic_sell_fixtures/old-swap.json");
pub(super) const SIMULATION: &str = include_str!("generic_sell_fixtures/simulation.json");
pub(super) const SIMULATION_REQUEST: &str =
    include_str!("generic_sell_fixtures/simulation-request.json");
pub(super) const DIRECT_REQUEST: &str =
    include_str!("generic_sell_fixtures/synthetic-direct-request.json");

pub(super) fn config(base: &str) -> ExecutionConfig {
    let mut cfg = super::generic_buy_fixture::config(base);
    cfg.execution_signer_pubkey = PAYER.into();
    cfg.canary_wallet_pubkey = PAYER.into();
    cfg
}
pub(super) fn legacy() -> String {
    serde_json::from_str::<Value>(SIMULATION_REQUEST).unwrap()["params"][0]
        .as_str()
        .unwrap()
        .into()
}
pub(super) fn old_v0() -> String {
    serde_json::from_str::<Value>(OLD_SWAP).unwrap()["swapTransaction"]
        .as_str()
        .unwrap()
        .into()
}
pub(super) fn request() -> Result<ExecutionSubmitRequest> {
    let quote: Value = serde_json::from_str(QUOTE)?;
    let sample = crate::execution_quote_http::quote_sample_from_json(quote.clone())?;
    Ok(ExecutionSubmitRequest {
        order_id: "batch116-recorded-order".into(),
        signal_id: "batch116-recorded-signal".into(),
        client_order_id: "batch116-recorded-client".into(),
        attempt: 1,
        route: "metis-canary".into(),
        wallet_id: "public-holder-fixture".into(),
        token: quote["inputMint"].as_str().unwrap().into(),
        side: "sell".into(),
        buy_size_sol: 0.0,
        slippage_tolerance_bps: 500,
        wallet_pubkey: PAYER.into(),
        entry_route_plan_json: None,
        metadata: ExecutionBuildPlanMetadata {
            quote_source: Some(
                crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_METIS.into(),
            ),
            quote_event_id: Some("batch116-frozen-r4-quote".into()),
            quote_status: Some("ok".into()),
            quote_in_amount_raw: Some(sample.in_amount),
            quote_out_amount_raw: Some(sample.out_amount),
            quote_response_json: Some(sample.response_json),
            route_plan_json: sample.route_plan_json,
            price_impact_pct: sample.price_impact_pct,
            slippage_bps: Some(500.0),
            priority_fee_source: Some("batch116-offline-fixture".into()),
            priority_fee_status: Some("ok".into()),
            priority_fee_lamports: Some(22_000),
            priority_fee_json: Some(
                json!({"version":1,"source":"batch116-offline-fixture",
                "unit":"total_priority_fee_lamports","value":22000})
                .to_string(),
            ),
            ..Default::default()
        },
    })
}

pub(super) fn save(name: &str, value: &Value) -> Result<()> {
    if let Ok(dir) = std::env::var("BATCH116_OUTPUT") {
        std::fs::write(
            std::path::Path::new(&dir).join(name.replace('/', "_")),
            serde_json::to_vec_pretty(value)?,
        )?;
    }
    Ok(())
}
