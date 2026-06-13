use crate::{
    executable_feedback::EXECUTABLE_FEEDBACK_REJECT_REASON,
    rug_feedback::RUG_FEEDBACK_REJECT_REASON, DiscoveryV2Status, DiscoveryV2WalletMetric,
};
use copybot_config::DiscoveryConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2WalletFilterImpact {
    pub publish_min_candidate_wallets: usize,
    pub follow_top_n: u32,
    pub candidate_wallet_count: usize,
    pub below_publish_floor: bool,
    pub wallet_metrics_total: usize,
    pub wallet_metrics_returned: usize,
    pub wallet_metrics_truncated: bool,
    pub eligible_returned: usize,
    pub executable_rejected_returned: usize,
    pub rug_rejected_returned: usize,
    pub executable_and_rug_rejected_returned: usize,
    pub rug_filter_enabled: bool,
    pub rug_filter_min_closed_trades: u32,
    pub rug_filter_max_stale_terminal_rate: f64,
    pub rug_filter_max_stale_terminal_pnl_sol: f64,
    pub rug_filter_quarantine_hours: u64,
}

pub(crate) fn wallet_filter_impact(
    status: &DiscoveryV2Status,
    discovery: &DiscoveryConfig,
) -> DiscoveryV2WalletFilterImpact {
    let mut eligible_returned = 0usize;
    let mut executable_rejected_returned = 0usize;
    let mut rug_rejected_returned = 0usize;
    let mut executable_and_rug_rejected_returned = 0usize;
    for metric in &status.wallet_metrics {
        if metric.eligible {
            eligible_returned = eligible_returned.saturating_add(1);
        }
        let executable = has_reason(metric, EXECUTABLE_FEEDBACK_REJECT_REASON);
        let rug = has_reason(metric, RUG_FEEDBACK_REJECT_REASON);
        if executable {
            executable_rejected_returned = executable_rejected_returned.saturating_add(1);
        }
        if rug {
            rug_rejected_returned = rug_rejected_returned.saturating_add(1);
        }
        if executable && rug {
            executable_and_rug_rejected_returned =
                executable_and_rug_rejected_returned.saturating_add(1);
        }
    }
    let publish_min_candidate_wallets = discovery.effective_publish_min_candidate_wallets();
    DiscoveryV2WalletFilterImpact {
        publish_min_candidate_wallets,
        follow_top_n: discovery.follow_top_n,
        candidate_wallet_count: status.candidate_wallets.len(),
        below_publish_floor: status.candidate_wallets.len() < publish_min_candidate_wallets,
        wallet_metrics_total: status.wallet_metrics_total,
        wallet_metrics_returned: status.wallet_metrics_returned,
        wallet_metrics_truncated: status.wallet_metrics_truncated,
        eligible_returned,
        executable_rejected_returned,
        rug_rejected_returned,
        executable_and_rug_rejected_returned,
        rug_filter_enabled: discovery.rug_wallet_filter_enabled,
        rug_filter_min_closed_trades: discovery.rug_wallet_filter_min_closed_trades,
        rug_filter_max_stale_terminal_rate: discovery.rug_wallet_filter_max_stale_terminal_rate,
        rug_filter_max_stale_terminal_pnl_sol: discovery
            .rug_wallet_filter_max_stale_terminal_pnl_sol,
        rug_filter_quarantine_hours: discovery.rug_wallet_filter_quarantine_hours,
    }
}

fn has_reason(metric: &DiscoveryV2WalletMetric, reason: &str) -> bool {
    metric
        .reject_reasons
        .iter()
        .any(|candidate| candidate == reason)
}
