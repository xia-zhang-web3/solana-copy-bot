use crate::metric::{reject_wallet_metric, DiscoveryV2WalletMetric};
use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use copybot_storage_core::{ShadowWalletFeedback, SqliteDiscoveryStore};
use std::collections::HashMap;

pub(crate) const SHADOW_FEEDBACK_WINDOW_HOURS: i64 = 24;
pub(crate) const SHADOW_FEEDBACK_MIN_CLOSED_TRADES: u64 = 3;
pub(crate) const SHADOW_FEEDBACK_MIN_ENTRY_SOL: f64 = 0.30;
pub(crate) const SHADOW_FEEDBACK_MAX_PNL_SOL: f64 = -0.05;
pub(crate) const SHADOW_FEEDBACK_MAX_ROI: f64 = -0.10;
pub(crate) const SHADOW_FEEDBACK_REJECT_REASON: &str = "shadow_feedback_negative";

pub(super) fn load_shadow_wallet_feedback(
    store: &SqliteDiscoveryStore,
    now: chrono::DateTime<Utc>,
) -> Result<HashMap<String, ShadowWalletFeedback>> {
    store
        .shadow_wallet_feedback_since(now - Duration::hours(SHADOW_FEEDBACK_WINDOW_HOURS))
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
    if rejects_wallet(feedback) {
        reject_wallet_metric(metric, SHADOW_FEEDBACK_REJECT_REASON);
    }
}

fn rejects_wallet(feedback: &ShadowWalletFeedback) -> bool {
    if feedback.closed_trades < SHADOW_FEEDBACK_MIN_CLOSED_TRADES
        || feedback.entry_cost_sol < SHADOW_FEEDBACK_MIN_ENTRY_SOL
    {
        return false;
    }
    feedback.pnl_sol <= SHADOW_FEEDBACK_MAX_PNL_SOL
        || feedback
            .roi()
            .is_some_and(|roi| roi <= SHADOW_FEEDBACK_MAX_ROI)
}
