use crate::metric::{reject_wallet_metric, DiscoveryV2WalletMetric};
use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use copybot_config::DiscoveryConfig;
use copybot_storage_core::{ExecutableWalletFeedback, SqliteDiscoveryStore};
use std::collections::HashMap;

pub(crate) const EXECUTABLE_FEEDBACK_REJECT_REASON: &str = "executable_feedback_negative";

pub(super) fn load_executable_wallet_feedback(
    store: &SqliteDiscoveryStore,
    discovery: &DiscoveryConfig,
    now: chrono::DateTime<Utc>,
) -> Result<HashMap<String, ExecutableWalletFeedback>> {
    if !discovery.executable_wallet_filter_enabled {
        return Ok(HashMap::new());
    }
    store
        .executable_wallet_feedback_since(
            now - Duration::hours(discovery.executable_wallet_filter_window_hours as i64),
        )
        .context("failed loading discovery v2 executable wallet feedback")
}

pub(super) fn apply_executable_feedback(
    metric: &mut DiscoveryV2WalletMetric,
    feedback: Option<&ExecutableWalletFeedback>,
    discovery: &DiscoveryConfig,
) {
    let Some(feedback) = feedback else {
        return;
    };
    metric.executable_feedback_samples = Some(feedback.samples.min(u64::from(u32::MAX)) as u32);
    metric.executable_feedback_pnl_after_fee_sol =
        Some(feedback.quote_adjusted_pnl_after_priority_fee_sol);
    metric.executable_feedback_flip_rate = feedback.flip_rate();
    if rejects_wallet(feedback, discovery) {
        reject_wallet_metric(metric, EXECUTABLE_FEEDBACK_REJECT_REASON);
    }
}

fn rejects_wallet(feedback: &ExecutableWalletFeedback, discovery: &DiscoveryConfig) -> bool {
    if feedback.samples < u64::from(discovery.executable_wallet_filter_min_samples) {
        return false;
    }
    let pnl_reject = feedback.quote_adjusted_pnl_after_priority_fee_sol
        < discovery.executable_wallet_filter_max_pnl_sol;
    let flip_reject = feedback
        .flip_rate()
        .is_some_and(|rate| rate > discovery.executable_wallet_filter_max_flip_rate);
    pnl_reject || flip_reject
}
