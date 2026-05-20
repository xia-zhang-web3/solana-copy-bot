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

#[derive(Debug, Default)]
pub(crate) struct DiscoveryV2FilterStatusBuilder {
    total_wallets: usize,
    eligible_wallets: usize,
    reject_breakdown: BTreeMap<String, u64>,
}

impl DiscoveryV2FilterStatusBuilder {
    pub(crate) fn observe_metric(&mut self, metric: &DiscoveryV2WalletMetric) {
        self.total_wallets = self.total_wallets.saturating_add(1);
        if metric.eligible {
            self.eligible_wallets = self.eligible_wallets.saturating_add(1);
        }
        for reason in &metric.reject_reasons {
            self.observe_reject_reason(reason);
        }
    }

    pub(crate) fn observe_live_rejection(&mut self, reason: &str) {
        self.eligible_wallets = self.eligible_wallets.saturating_sub(1);
        self.observe_reject_reason(reason);
    }

    pub(crate) fn observe_rejected_wallets(&mut self, reason: &str, count: u64) {
        if count == 0 {
            return;
        }
        self.total_wallets = self.total_wallets.saturating_add(count as usize);
        *self.reject_breakdown.entry(reason.to_string()).or_insert(0) += count;
    }

    pub(crate) fn finish(self) -> DiscoveryV2FilterStatus {
        DiscoveryV2FilterStatus {
            total_wallets: self.total_wallets,
            eligible_wallets: self.eligible_wallets,
            rejected_wallets: self.total_wallets.saturating_sub(self.eligible_wallets),
            reject_breakdown: self.reject_breakdown,
        }
    }

    fn observe_reject_reason(&mut self, reason: &str) {
        *self.reject_breakdown.entry(reason.to_string()).or_insert(0) += 1;
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
