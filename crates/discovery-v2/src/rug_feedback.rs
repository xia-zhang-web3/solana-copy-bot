use crate::metric::{reject_wallet_metric, DiscoveryV2WalletMetric};
use crate::status::DiscoveryV2RugQuarantineCandidate;
use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use copybot_config::DiscoveryConfig;
use copybot_storage_core::{RugWalletFeedback, SqliteDiscoveryStore};
use std::collections::{HashMap, HashSet};

pub(crate) const RUG_FEEDBACK_REJECT_REASON: &str = "rug_feedback_stale_terminal";

#[derive(Default)]
pub(super) struct RugWalletFilterState {
    feedback: HashMap<String, RugWalletFeedback>,
    quarantined: HashSet<String>,
}

impl RugWalletFilterState {
    pub(super) fn feedback(&self, wallet_id: &str) -> Option<&RugWalletFeedback> {
        self.feedback.get(wallet_id)
    }

    pub(super) fn is_quarantined(&self, wallet_id: &str) -> bool {
        self.quarantined.contains(wallet_id)
    }
}

pub(super) fn load_rug_wallet_filter_state(
    store: &SqliteDiscoveryStore,
    discovery: &DiscoveryConfig,
    now: chrono::DateTime<Utc>,
) -> Result<RugWalletFilterState> {
    if !discovery.rug_wallet_filter_enabled {
        return Ok(RugWalletFilterState::default());
    }
    let feedback = store
        .rug_wallet_feedback_since(
            now - Duration::hours(discovery.rug_wallet_filter_window_hours as i64),
        )
        .context("failed loading discovery v2 rug wallet feedback")?;
    let quarantined = store
        .active_rug_wallet_quarantines(RUG_FEEDBACK_REJECT_REASON, now)
        .context("failed loading discovery v2 active rug wallet quarantines")?
        .into_iter()
        .map(|row| row.wallet_id)
        .collect::<HashSet<_>>();
    Ok(RugWalletFilterState {
        feedback,
        quarantined,
    })
}

pub(super) fn apply_rug_feedback(
    metric: &mut DiscoveryV2WalletMetric,
    feedback: Option<&RugWalletFeedback>,
    quarantined: bool,
    discovery: &DiscoveryConfig,
) {
    let rejects = feedback.is_some_and(|feedback| {
        metric.rug_feedback_closed_trades =
            Some(feedback.closed_trades.min(u64::from(u32::MAX)) as u32);
        metric.rug_feedback_stale_terminal_closes =
            Some(feedback.stale_terminal_closes.min(u64::from(u32::MAX)) as u32);
        metric.rug_feedback_stale_terminal_rate = feedback.stale_terminal_rate();
        metric.rug_feedback_stale_terminal_pnl_sol = Some(feedback.stale_terminal_pnl_sol);
        rejects_wallet(feedback, discovery)
    });
    if quarantined || rejects {
        reject_wallet_metric(metric, RUG_FEEDBACK_REJECT_REASON);
    }
}

pub(super) fn rug_quarantine_candidate(
    metric: &DiscoveryV2WalletMetric,
) -> Option<DiscoveryV2RugQuarantineCandidate> {
    metric
        .reject_reasons
        .iter()
        .any(|reason| reason == RUG_FEEDBACK_REJECT_REASON)
        .then(|| DiscoveryV2RugQuarantineCandidate {
            wallet_id: metric.wallet_id.clone(),
            closed_trades: metric.rug_feedback_closed_trades,
            stale_terminal_closes: metric.rug_feedback_stale_terminal_closes,
            stale_terminal_rate: metric.rug_feedback_stale_terminal_rate,
            stale_terminal_pnl_sol: metric.rug_feedback_stale_terminal_pnl_sol,
        })
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
