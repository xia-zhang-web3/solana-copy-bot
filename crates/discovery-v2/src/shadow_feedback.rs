use crate::metric::{reject_wallet_metric, DiscoveryV2WalletMetric};
use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use copybot_config::{
    DISCOVERY_V2_SHADOW_FEEDBACK_CATASTROPHE_MAX_ROI,
    DISCOVERY_V2_SHADOW_FEEDBACK_CATASTROPHE_MIN_CLOSED_TRADES,
    DISCOVERY_V2_SHADOW_FEEDBACK_CATASTROPHE_MIN_ENTRY_SOL,
    DISCOVERY_V2_SHADOW_FEEDBACK_MAX_PNL_SOL, DISCOVERY_V2_SHADOW_FEEDBACK_MAX_ROI,
    DISCOVERY_V2_SHADOW_FEEDBACK_MIN_CLOSED_TRADES, DISCOVERY_V2_SHADOW_FEEDBACK_MIN_ENTRY_SOL,
    DISCOVERY_V2_SHADOW_FEEDBACK_SINGLE_LOSS_MAX_ROI,
    DISCOVERY_V2_SHADOW_FEEDBACK_SINGLE_LOSS_MIN_ENTRY_SOL,
    DISCOVERY_V2_SHADOW_FEEDBACK_WINDOW_HOURS,
};
use copybot_storage_core::{ShadowWalletFeedback, SqliteDiscoveryStore};
use std::collections::HashMap;

pub(crate) const SHADOW_FEEDBACK_REJECT_REASON: &str = "shadow_feedback_negative";
pub(crate) const SHADOW_FEEDBACK_CATASTROPHE_REJECT_REASON: &str =
    "shadow_feedback_catastrophic_loss";
pub(crate) const SHADOW_FEEDBACK_SINGLE_LOSS_REJECT_REASON: &str =
    "shadow_feedback_single_bad_loss";

pub(super) fn load_shadow_wallet_feedback(
    store: &SqliteDiscoveryStore,
    now: chrono::DateTime<Utc>,
) -> Result<HashMap<String, ShadowWalletFeedback>> {
    store
        .shadow_wallet_feedback_since(
            now - Duration::hours(DISCOVERY_V2_SHADOW_FEEDBACK_WINDOW_HOURS),
        )
        .context("failed loading discovery v2 shadow wallet feedback")
}

pub(super) fn apply_shadow_feedback(
    metric: &mut DiscoveryV2WalletMetric,
    feedback: Option<&ShadowWalletFeedback>,
) {
    let Some(feedback) = feedback else {
        return;
    };
    metric.shadow_closed_trades_24h = Some(feedback.closed_trades.min(u64::from(u32::MAX)) as u32);
    metric.shadow_pnl_sol_24h = Some(feedback.pnl_sol);
    metric.shadow_roi_24h = feedback.roi();
    metric.shadow_worst_trade_roi_24h = feedback.worst_trade_roi;
    if rejects_catastrophic_wallet(feedback) {
        reject_wallet_metric(metric, SHADOW_FEEDBACK_CATASTROPHE_REJECT_REASON);
    } else if rejects_single_bad_loss_wallet(feedback) {
        reject_wallet_metric(metric, SHADOW_FEEDBACK_SINGLE_LOSS_REJECT_REASON);
    } else if rejects_wallet(feedback) {
        reject_wallet_metric(metric, SHADOW_FEEDBACK_REJECT_REASON);
    }
}

fn rejects_catastrophic_wallet(feedback: &ShadowWalletFeedback) -> bool {
    feedback.closed_trades >= DISCOVERY_V2_SHADOW_FEEDBACK_CATASTROPHE_MIN_CLOSED_TRADES
        && feedback.entry_cost_sol >= DISCOVERY_V2_SHADOW_FEEDBACK_CATASTROPHE_MIN_ENTRY_SOL
        && feedback
            .roi()
            .is_some_and(|roi| roi <= DISCOVERY_V2_SHADOW_FEEDBACK_CATASTROPHE_MAX_ROI)
}

fn rejects_single_bad_loss_wallet(feedback: &ShadowWalletFeedback) -> bool {
    feedback.worst_trade_entry_cost_sol >= DISCOVERY_V2_SHADOW_FEEDBACK_SINGLE_LOSS_MIN_ENTRY_SOL
        && feedback
            .worst_trade_roi
            .is_some_and(|roi| roi <= DISCOVERY_V2_SHADOW_FEEDBACK_SINGLE_LOSS_MAX_ROI)
}

fn rejects_wallet(feedback: &ShadowWalletFeedback) -> bool {
    if feedback.closed_trades < DISCOVERY_V2_SHADOW_FEEDBACK_MIN_CLOSED_TRADES
        || feedback.entry_cost_sol < DISCOVERY_V2_SHADOW_FEEDBACK_MIN_ENTRY_SOL
    {
        return false;
    }
    feedback.pnl_sol <= DISCOVERY_V2_SHADOW_FEEDBACK_MAX_PNL_SOL
        || feedback
            .roi()
            .is_some_and(|roi| roi <= DISCOVERY_V2_SHADOW_FEEDBACK_MAX_ROI)
}
