use crate::quote_portfolio_report_support::*;
use copybot_storage_core::ExecutionQuoteCanaryEventInsert;
use serde_json::json;
pub fn row(
    id: &str,
    n: u8,
    side: &str,
    input: u64,
    output: u64,
) -> ExecutionQuoteCanaryEventInsert {
    let (im, om, idec, odec) = if side == "buy" {
        (SOL.to_string(), mint(n), 9, 0)
    } else {
        (mint(n), SOL.to_string(), 0, 9)
    };
    ExecutionQuoteCanaryEventInsert {
        event_id: id.into(),
        wallet_id: "fixture-wallet".into(),
        signal_id: None,
        shadow_closed_trade_id: None,
        token: mint(n),
        side: side.into(),
        quote_status: "ok".into(),
        request_ts: TS.parse().unwrap(),
        http_request_started_ts: Some(TS.parse().unwrap()),
        quote_response_available_ts: Some(TS.parse().unwrap()),
        signal_ts: None,
        decision_delay_ms: None,
        quote_latency_ms: None,
        leader_notional_sol: None,
        quote_in_amount_raw: Some(input.to_string()),
        quote_out_amount_raw: Some(output.to_string()),
        quote_response_json: Some(
            json!({"inputMint":im,"outputMint":om,"inAmount":input.to_string(),
            "outAmount":output.to_string(),"inputDecimals":idec,"outputDecimals":odec})
            .to_string(),
        ),
        quote_price_sol: None,
        shadow_price_sol: None,
        slippage_bps: None,
        price_impact_pct: None,
        route_plan_json: None,
        priority_fee_status: None,
        priority_fee_lamports: None,
        priority_fee_json: None,
        decision_status: None,
        decision_reason: None,
        error: None,
    }
}
