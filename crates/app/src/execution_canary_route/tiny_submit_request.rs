use crate::execution_quote_canary_helpers::quote_canary_slippage_limit_bps;
use crate::execution_submit_adapter::{
    cap_execution_priority_fee_lamports, ExecutionBuildPlanMetadata, ExecutionSubmitRequest,
};
use copybot_config::ExecutionConfig;
use copybot_core_types::CopySignalRow;
use copybot_storage_core::ExecutionCanaryOrder;

pub(super) fn build_submit_request(
    config: &ExecutionConfig,
    signal: &CopySignalRow,
    order: &ExecutionCanaryOrder,
    metadata: ExecutionBuildPlanMetadata,
) -> ExecutionSubmitRequest {
    let metadata = cap_execution_priority_fee_lamports(config, metadata);
    ExecutionSubmitRequest {
        order_id: order.order_id.clone(),
        signal_id: signal.signal_id.clone(),
        client_order_id: order.client_order_id.clone(),
        attempt: order.attempt,
        route: config.canary_route.clone(),
        wallet_id: signal.wallet_id.clone(),
        token: signal.token.clone(),
        side: signal.side.clone(),
        buy_size_sol: config.canary_buy_size_sol,
        slippage_tolerance_bps: quote_canary_slippage_limit_bps(config, signal.side.as_str()),
        wallet_pubkey: config.canary_wallet_pubkey.clone(),
        metadata,
    }
}
