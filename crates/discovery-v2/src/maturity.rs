use crate::metric::DiscoveryV2WalletMetric;
use crate::status::DiscoveryV2MaturityStatus;
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_config::DiscoveryConfig;
use copybot_storage_core::SqliteDiscoveryStore;
use std::collections::HashSet;
use std::time::Instant;

pub(crate) fn apply_maturity_ranking(
    store: &SqliteDiscoveryStore,
    discovery: &DiscoveryConfig,
    now: DateTime<Utc>,
    deadline: Instant,
    metrics: &mut [DiscoveryV2WalletMetric],
) -> Result<DiscoveryV2MaturityStatus> {
    let mut status = configured_status(discovery);
    if !status.enabled {
        return Ok(status);
    }
    let window_start = now - Duration::days(discovery.maturity_window_days as i64);
    for metric in metrics
        .iter_mut()
        .filter(|metric| metric.eligible && metric.score >= discovery.min_score)
    {
        if Instant::now() >= deadline {
            status.time_budget_exhausted = true;
            break;
        }
        let activity = store.wallet_sol_leg_activity_in_window_read_only(
            &metric.wallet_id,
            window_start,
            now,
            deadline,
        )?;
        status.evaluated_wallets = status.evaluated_wallets.saturating_add(1);
        if activity.time_budget_exhausted {
            status.time_budget_exhausted = true;
            break;
        }
        metric.maturity_window_days = discovery.maturity_window_days;
        metric.maturity_active_days = activity.active_days;
        metric.maturity_trades = activity.trades;
        metric.maturity_preferred = activity.active_days >= discovery.maturity_min_active_days;
        metric.selection_score = if metric.maturity_preferred {
            metric.score + discovery.maturity_score_bonus
        } else {
            metric.score
        };
        if metric.maturity_preferred {
            status.preferred_wallets = status.preferred_wallets.saturating_add(1);
        }
    }
    Ok(status)
}

pub(crate) fn configured_status(discovery: &DiscoveryConfig) -> DiscoveryV2MaturityStatus {
    let enabled = discovery.maturity_window_days > 0 && discovery.maturity_min_active_days > 0;
    DiscoveryV2MaturityStatus {
        enabled,
        window_days: discovery.maturity_window_days,
        min_active_days: discovery.maturity_min_active_days,
        score_bonus: discovery.maturity_score_bonus,
        evaluated_wallets: 0,
        preferred_wallets: 0,
        selected_primary_wallets: 0,
        selected_secondary_wallets: 0,
        selected_emergency_wallets: 0,
        time_budget_exhausted: false,
    }
}

pub(crate) fn observe_selected_maturity_tiers(
    status: &mut DiscoveryV2MaturityStatus,
    metrics: &[DiscoveryV2WalletMetric],
    candidate_wallets: &[String],
) {
    status.selected_primary_wallets = 0;
    status.selected_secondary_wallets = 0;
    status.selected_emergency_wallets = 0;
    if !status.enabled {
        return;
    }
    let selected = candidate_wallets
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    for metric in metrics
        .iter()
        .filter(|metric| selected.contains(metric.wallet_id.as_str()))
    {
        match maturity_tier(metric, status.min_active_days) {
            MaturityTier::Primary => {
                status.selected_primary_wallets = status.selected_primary_wallets.saturating_add(1);
            }
            MaturityTier::Secondary => {
                status.selected_secondary_wallets =
                    status.selected_secondary_wallets.saturating_add(1);
            }
            MaturityTier::Emergency => {
                status.selected_emergency_wallets =
                    status.selected_emergency_wallets.saturating_add(1);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum MaturityTier {
    Primary,
    Secondary,
    Emergency,
}

pub(crate) fn maturity_tier(
    metric: &DiscoveryV2WalletMetric,
    maturity_min_active_days: u32,
) -> MaturityTier {
    if maturity_min_active_days <= 1 || metric.maturity_active_days >= maturity_min_active_days {
        return MaturityTier::Primary;
    }
    if metric.maturity_active_days.saturating_add(1) >= maturity_min_active_days {
        return MaturityTier::Secondary;
    }
    MaturityTier::Emergency
}
