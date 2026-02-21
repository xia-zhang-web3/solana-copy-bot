use crate::candidate::ShadowCandidate;
use crate::ShadowDropReason;
use copybot_core_types::SwapEvent;
use tracing::info;

pub(super) fn log_gate_drop(
    stage: &str,
    reason: ShadowDropReason,
    swap: &SwapEvent,
    candidate: &ShadowCandidate,
    latency_ms: i64,
    runtime_followed: bool,
    temporal_followed: bool,
    is_unfollowed_sell_exit: bool,
) {
    if !runtime_followed && !temporal_followed && !is_unfollowed_sell_exit {
        return;
    }
    info!(
        stage,
        reason = reason.as_str(),
        wallet = %swap.wallet,
        token = %candidate.token,
        side = %candidate.side,
        signature = %swap.signature,
        leader_notional_sol = candidate.leader_notional_sol,
        latency_ms,
        runtime_followed,
        temporal_followed,
        is_unfollowed_sell_exit,
        "shadow gate dropped"
    );
}
