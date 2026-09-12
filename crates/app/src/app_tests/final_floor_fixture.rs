pub(super) use super::native_funding_fixture::{direct, payload, transfer, TOKEN, WALLET};
use crate::execution_submit_adapter::{ExecutionBuildPlanMetadata, ExecutionSubmitRequest};

pub(super) fn request(buy: bool) -> ExecutionSubmitRequest {
    ExecutionSubmitRequest {
        order_id: "b23-layout-order".into(),
        signal_id: "b23-layout-signal".into(),
        client_order_id: "b23-layout-client".into(),
        attempt: 1,
        route: "pumpswap-direct-canary".into(),
        wallet_id: "synthetic".into(),
        token: TOKEN.into(),
        side: if buy { "buy" } else { "sell" }.into(),
        buy_size_sol: 0.01,
        slippage_tolerance_bps: 500,
        wallet_pubkey: bs58::encode(WALLET).into_string(),
        entry_route_plan_json: None,
        metadata: ExecutionBuildPlanMetadata {
            http_request_started_ts: None,
            quote_response_available_ts: None,
            priority_fee_status: Some("ok".into()),
            priority_fee_lamports: Some(14_000),
            priority_fee_json: Some(super::priority_fee_fixture::total_json(14_000)),
            ..Default::default()
        },
    }
}
