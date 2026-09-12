use crate::live_inventory::INVENTORY_CONTRACT_VERSION;
use crate::{DiscoveryV2Status, DiscoveryV2WalletMetric};
use anyhow::{bail, Result};

pub(crate) const COVERAGE_UNVERIFIED: &str = "discovery_v2_live_inventory_coverage_unverified";

pub(crate) fn metric_has_inventory_coverage(metric: &DiscoveryV2WalletMetric) -> bool {
    metric
        .live_inventory
        .as_ref()
        .is_some_and(|evidence| evidence.contract_version == INVENTORY_CONTRACT_VERSION)
}

/// Legacy serde defaults mean unknown, never proof of both account programs.
pub(crate) fn validate_inventory_coverage(
    status: &DiscoveryV2Status,
    required: bool,
) -> Result<()> {
    // The existing policy identity also binds the enabled bit. A missing live object
    // must not turn a persisted enabled-gate status into the standalone disabled path.
    let required = required
        || status
            .policy_fingerprint
            .split(';')
            .any(|field| field == "live_portfolio_gate_enabled=true");
    if !required {
        return Ok(());
    }
    if !status.live_portfolio.as_ref().is_some_and(|live| {
        live.enabled && live.inventory_contract_version == Some(INVENTORY_CONTRACT_VERSION)
    }) {
        bail!(COVERAGE_UNVERIFIED);
    }
    for candidate in &status.candidate_wallets {
        let metric = status
            .wallet_metrics
            .iter()
            .find(|metric| metric.wallet_id == *candidate)
            .ok_or_else(|| {
                anyhow::anyhow!("candidate wallet is missing from V2 wallet metrics: {candidate}")
            })?;
        if !metric_has_inventory_coverage(metric) {
            bail!(COVERAGE_UNVERIFIED);
        }
    }
    Ok(())
}
