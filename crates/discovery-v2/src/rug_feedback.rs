use crate::metric::{reject_wallet_metric, DiscoveryV2WalletMetric};
use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use copybot_config::DiscoveryConfig;
use copybot_storage_core::{RugWalletFeedback, SqliteDiscoveryStore};
use std::collections::HashMap;

pub(crate) const RUG_FEEDBACK_REJECT_REASON: &str = "rug_feedback_stale_terminal";

pub(super) fn load_rug_wallet_feedback(
    store: &SqliteDiscoveryStore,
    discovery: &DiscoveryConfig,
    now: chrono::DateTime<Utc>,
) -> Result<HashMap<String, RugWalletFeedback>> {
    if !discovery.rug_wallet_filter_enabled {
        return Ok(HashMap::new());
    }
    store
        .rug_wallet_feedback_since(
            now - Duration::hours(discovery.rug_wallet_filter_window_hours as i64),
        )
        .context("failed loading discovery v2 rug wallet feedback")
}

pub(super) fn apply_rug_feedback(
    metric: &mut DiscoveryV2WalletMetric,
    feedback: Option<&RugWalletFeedback>,
    discovery: &DiscoveryConfig,
) {
    let Some(feedback) = feedback else {
        return;
    };
    metric.rug_feedback_closed_trades =
        Some(feedback.closed_trades.min(u64::from(u32::MAX)) as u32);
    metric.rug_feedback_stale_terminal_closes =
        Some(feedback.stale_terminal_closes.min(u64::from(u32::MAX)) as u32);
    metric.rug_feedback_stale_terminal_rate = feedback.stale_terminal_rate();
    metric.rug_feedback_stale_terminal_pnl_sol = Some(feedback.stale_terminal_pnl_sol);
    if rejects_wallet(feedback, discovery) {
        reject_wallet_metric(metric, RUG_FEEDBACK_REJECT_REASON);
    }
}

fn rejects_wallet(feedback: &RugWalletFeedback, discovery: &DiscoveryConfig) -> bool {
    if feedback.closed_trades < u64::from(discovery.rug_wallet_filter_min_closed_trades) {
        return false;
    }
    let rate_reject = feedback
        .stale_terminal_rate()
        .is_some_and(|rate| rate > discovery.rug_wallet_filter_max_stale_terminal_rate);
    let pnl_reject =
        feedback.stale_terminal_pnl_sol < discovery.rug_wallet_filter_max_stale_terminal_pnl_sol;
    rate_reject || pnl_reject
}
