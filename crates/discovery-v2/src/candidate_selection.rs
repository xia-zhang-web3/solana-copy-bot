use crate::metric::DiscoveryV2WalletMetric;
use crate::slow_hold::{slow_hold_score, SOURCE_BASELINE, SOURCE_SLOW_HOLD};
use crate::status::DiscoveryV2CandidateWalletSource;
use chrono::{DateTime, Utc};
use copybot_config::DiscoveryConfig;
use std::collections::HashSet;

pub(super) struct CandidateWalletSelection {
    pub(super) wallets: Vec<String>,
    pub(super) sources: Vec<DiscoveryV2CandidateWalletSource>,
}

pub(super) fn candidate_wallets_without_live_gate(
    discovery: &DiscoveryConfig,
    metrics: &[DiscoveryV2WalletMetric],
    active_follow_wallets: &HashSet<String>,
    now: DateTime<Utc>,
) -> CandidateWalletSelection {
    let selected = stable_candidate_wallets(
        discovery,
        metrics,
        active_follow_wallets,
        discovery.follow_top_n.max(1) as usize,
        total_candidate_limit(discovery),
        now,
    );
    CandidateWalletSelection {
        wallets: selected
            .iter()
            .map(|(_, wallet_id, _)| wallet_id.clone())
            .collect(),
        sources: selected
            .iter()
            .map(|(_, wallet_id, source)| DiscoveryV2CandidateWalletSource {
                wallet_id: wallet_id.clone(),
                source_cohort: (*source).to_string(),
            })
            .collect(),
    }
}

pub(super) fn stable_candidate_wallets(
    discovery: &DiscoveryConfig,
    metrics: &[DiscoveryV2WalletMetric],
    active_follow_wallets: &HashSet<String>,
    baseline_limit: usize,
    total_limit: usize,
    now: DateTime<Utc>,
) -> Vec<(usize, String, &'static str)> {
    let mut ordered = Vec::new();
    let mut seen = HashSet::new();
    let baseline_limit = baseline_limit.max(1);
    for prefer_active in [true, false] {
        for (index, metric) in metrics.iter().enumerate() {
            if active_follow_wallets.contains(&metric.wallet_id) != prefer_active {
                continue;
            }
            if metric.eligible && metric.score >= discovery.min_score {
                seen.insert(metric.wallet_id.clone());
                ordered.push((index, metric.wallet_id.clone(), SOURCE_BASELINE));
                if ordered.len() >= baseline_limit {
                    break;
                }
            }
        }
        if ordered.len() >= baseline_limit {
            break;
        }
    }
    if discovery.slow_hold_wallets_enabled && ordered.len() < total_limit {
        for (index, wallet_id, _) in
            stable_slow_hold_wallets(discovery, metrics, active_follow_wallets, now)
        {
            if seen.insert(wallet_id.clone()) {
                ordered.push((index, wallet_id, SOURCE_SLOW_HOLD));
                if ordered.len() >= total_limit {
                    break;
                }
            }
        }
    }
    ordered
}

pub(super) fn total_candidate_limit(discovery: &DiscoveryConfig) -> usize {
    let baseline = discovery.follow_top_n.max(1) as usize;
    if discovery.slow_hold_wallets_enabled {
        baseline.saturating_add(discovery.slow_hold_top_m.max(1) as usize)
    } else {
        baseline
    }
}

fn stable_slow_hold_wallets(
    discovery: &DiscoveryConfig,
    metrics: &[DiscoveryV2WalletMetric],
    active_follow_wallets: &HashSet<String>,
    now: DateTime<Utc>,
) -> Vec<(usize, String, f64)> {
    let mut ordered = Vec::new();
    for prefer_active in [true, false] {
        let mut scored = metrics
            .iter()
            .enumerate()
            .filter(|(_, metric)| {
                active_follow_wallets.contains(&metric.wallet_id) == prefer_active
            })
            .filter_map(|(index, metric)| {
                slow_hold_score(metric, discovery, now)
                    .map(|score| (index, metric.wallet_id.clone(), score))
            })
            .collect::<Vec<_>>();
        scored.sort_by(|left, right| {
            right
                .2
                .total_cmp(&left.2)
                .then_with(|| left.1.cmp(&right.1))
        });
        ordered.extend(scored);
        if ordered.len() >= discovery.slow_hold_top_m.max(1) as usize {
            break;
        }
    }
    ordered.truncate(discovery.slow_hold_top_m.max(1) as usize);
    ordered
}
