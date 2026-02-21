use copybot_core_types::SwapEvent;
use copybot_shadow::ShadowDropReason;
use std::collections::BTreeMap;
use tracing::{info, warn};

pub(crate) fn reason_to_key(reason: ShadowDropReason) -> &'static str {
    reason.as_str()
}

pub(crate) fn reason_to_stage(reason: ShadowDropReason) -> &'static str {
    match reason {
        ShadowDropReason::Disabled => "disabled",
        ShadowDropReason::NotFollowed => "follow",
        ShadowDropReason::NotSolLeg => "pair",
        ShadowDropReason::BelowNotional => "notional",
        ShadowDropReason::LagExceeded => "lag",
        ShadowDropReason::TooNew
        | ShadowDropReason::LowHolders
        | ShadowDropReason::LowLiquidity
        | ShadowDropReason::LowVolume
        | ShadowDropReason::ThinMarket => "quality",
        ShadowDropReason::InvalidSizing => "sizing",
        ShadowDropReason::DuplicateSignal => "dedupe",
        ShadowDropReason::UnsupportedSide => "side",
    }
}

pub(crate) fn format_error_chain(error: &anyhow::Error) -> String {
    let mut chain = String::new();
    for (idx, cause) in error.chain().enumerate() {
        if idx > 0 {
            chain.push_str(" | ");
        }
        chain.push_str(&cause.to_string());
    }
    chain
}

pub(crate) fn record_shadow_queue_full_buy_drop(
    swap: &SwapEvent,
    shadow_drop_reason_counts: &mut BTreeMap<&'static str, u64>,
    shadow_drop_stage_counts: &mut BTreeMap<&'static str, u64>,
    shadow_queue_full_outcome_counts: &mut BTreeMap<&'static str, u64>,
) {
    let reason = "queue_full_buy_drop";
    *shadow_drop_reason_counts.entry(reason).or_insert(0) += 1;
    *shadow_drop_stage_counts.entry("scheduler").or_insert(0) += 1;
    *shadow_queue_full_outcome_counts.entry(reason).or_insert(0) += 1;
    warn!(
        stage = "scheduler",
        reason,
        side = "buy",
        wallet = %swap.wallet,
        token = %swap.token_out,
        signature = %swap.signature,
        "shadow gate dropped"
    );
}

pub(crate) fn record_shadow_queue_full_sell_outcome(
    swap: &SwapEvent,
    kept: bool,
    shadow_drop_reason_counts: &mut BTreeMap<&'static str, u64>,
    shadow_drop_stage_counts: &mut BTreeMap<&'static str, u64>,
    shadow_queue_full_outcome_counts: &mut BTreeMap<&'static str, u64>,
) {
    let reason = "queue_full_sell_kept_or_dropped";
    let outcome_key = if kept {
        "queue_full_sell_kept"
    } else {
        "queue_full_sell_dropped"
    };
    *shadow_queue_full_outcome_counts
        .entry(outcome_key)
        .or_insert(0) += 1;
    if kept {
        info!(
            stage = "scheduler",
            reason,
            outcome = "kept",
            side = "sell",
            wallet = %swap.wallet,
            token = %swap.token_in,
            signature = %swap.signature,
            "shadow queue_full sell outcome"
        );
    } else {
        *shadow_drop_reason_counts
            .entry("queue_full_sell_dropped")
            .or_insert(0) += 1;
        *shadow_drop_stage_counts.entry("scheduler").or_insert(0) += 1;
        warn!(
            stage = "scheduler",
            reason,
            outcome = "dropped",
            side = "sell",
            wallet = %swap.wallet,
            token = %swap.token_in,
            signature = %swap.signature,
            "shadow gate dropped"
        );
    }
}
