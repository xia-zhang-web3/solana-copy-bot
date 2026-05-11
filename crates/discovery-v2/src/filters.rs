use crate::metric::DiscoveryV2WalletMetric;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2FilterStatus {
    pub total_wallets: usize,
    pub eligible_wallets: usize,
    pub rejected_wallets: usize,
    pub reject_breakdown: BTreeMap<String, u64>,
}

pub(crate) fn build_filter_status(
    wallet_metrics: &[DiscoveryV2WalletMetric],
) -> DiscoveryV2FilterStatus {
    let mut reject_breakdown = BTreeMap::new();
    for metric in wallet_metrics {
        for reason in &metric.reject_reasons {
            *reject_breakdown.entry(reason.clone()).or_insert(0) += 1;
        }
    }
    let eligible_wallets = wallet_metrics
        .iter()
        .filter(|metric| metric.eligible)
        .count();
    DiscoveryV2FilterStatus {
        total_wallets: wallet_metrics.len(),
        eligible_wallets,
        rejected_wallets: wallet_metrics.len().saturating_sub(eligible_wallets),
        reject_breakdown,
    }
}

pub(crate) fn build_budget_exhausted_filter_status(
    total_wallets: usize,
) -> DiscoveryV2FilterStatus {
    let mut reject_breakdown = BTreeMap::new();
    if total_wallets > 0 {
        reject_breakdown.insert("scan_budget_exhausted".to_string(), total_wallets as u64);
    }
    DiscoveryV2FilterStatus {
        total_wallets,
        eligible_wallets: 0,
        rejected_wallets: total_wallets,
        reject_breakdown,
    }
}
