use crate::live_inventory_parse::valid_pubkey;
use crate::live_valuation::{
    contribution_reason, decide_valuation, DiscoveryV2LiveValuationEvidence,
    DiscoveryV2PriceUnknownReason as Reason, PRICE_EXPIRED, PROOF_UNVERIFIED, VALUATION_VERSION,
};
use crate::live_valuation_numbers::{normalize_numbers, numeric_alias_matches};
use crate::{
    discovery_v2_policy_fingerprint, DiscoveryV2DecisionContext, DiscoveryV2Status,
    DiscoveryV2WalletMetric,
};
use anyhow::{bail, ensure, Result};
use std::collections::HashSet;

/// No storage/RPC reads or persistence. Derive a current view from complete
/// original evidence; an expired candidate requires an ordinary rebuild.
pub fn revalidate_discovery_v2_status(
    mut status: DiscoveryV2Status,
    context: DiscoveryV2DecisionContext<'_>,
) -> Result<DiscoveryV2Status> {
    let required = context.discovery.live_portfolio_gate_enabled
        || status
            .live_portfolio
            .as_ref()
            .is_some_and(|live| live.enabled)
        || status
            .policy_fingerprint
            .split(';')
            .any(|p| p == "live_portfolio_gate_enabled=true");
    crate::live_inventory_status::validate_inventory_coverage(&status, required)?;
    if !required {
        return Ok(status);
    }
    let discovery = context.discovery;
    let options = context.options;
    let window_start = options
        .checked_window_start()
        .ok_or_else(|| anyhow::anyhow!("discovery_v2_price_window_invalid"))?;
    ensure!(
        status.policy_fingerprint
            == discovery_v2_policy_fingerprint(discovery, context.shadow, options),
        "discovery v2 decision policy mismatch"
    );
    crate::materialized_status::validate_status_identity(&status, options)?;
    crate::materialized_status::validate_status_age(&status, discovery, options.now)?;
    ensure!(
        status
            .live_portfolio
            .as_ref()
            .is_some_and(|live| live.valuation_contract_version == Some(VALUATION_VERSION)),
        PROOF_UNVERIFIED
    );
    ensure!(
        discovery.min_live_sol_balance.is_finite()
            && discovery.min_live_sol_balance >= 0.0
            && discovery.min_live_portfolio_value_sol.is_finite()
            && discovery.min_live_portfolio_value_sol >= 0.0,
        PROOF_UNVERIFIED
    );
    let mut origin_options = options.clone();
    origin_options.now = status.now;
    ensure!(
        origin_options.checked_window_start() == Some(status.window_start),
        PROOF_UNVERIFIED
    );
    let candidates = status
        .candidate_wallets
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    ensure!(
        candidates.len() == status.candidate_wallets.len(),
        PROOF_UNVERIFIED
    );
    ensure!(
        !status.production_green
            || (!candidates.is_empty()
                && candidates.len() >= discovery.effective_publish_min_candidate_wallets()),
        "discovery_v2_candidate_wallets_below_publish_floor"
    );
    let mut expired = 0;
    let mut unknown_wallets = 0;
    for metric in &mut status.wallet_metrics {
        let candidate = candidates.contains(&metric.wallet_id);
        let Some(proof) = metric.live_valuation.clone() else {
            ensure!(
                !candidate && metric.live_inventory.is_none(),
                PROOF_UNVERIFIED
            );
            continue;
        };
        let mut proof = validate_proof(
            metric,
            proof,
            status.now,
            status.window_start,
            options.window_minutes,
            discovery.live_portfolio_max_token_accounts.max(1),
            options.now,
        )?;
        let decision = decide_valuation(
            &proof.classic_positions,
            proof.token_2022_positive_positions,
            window_start,
            options.now,
        );
        let sol = f64::from_bits(proof.sol_balance_bits);
        ensure!(
            metric
                .live_sol_balance
                .is_some_and(|alias| numeric_alias_matches(alias, sol)),
            PROOF_UNVERIFIED
        );
        metric.live_sol_balance = Some(sol);
        if candidate
            && sol < discovery.min_live_sol_balance
            && decision.known_classic_value_sol < discovery.min_live_portfolio_value_sol
        {
            expired += 1;
        }
        if decision.unvalued_positions > 0 {
            unknown_wallets += 1;
        }
        metric.live_token_value_sol = Some(decision.known_classic_value_sol);
        metric.live_tradable_token_positions = Some(decision.valued_classic_positions);
        let inventory = metric.live_inventory.as_mut().expect("proof validated");
        inventory.known_classic_value_sol = decision.known_classic_value_sol;
        inventory.unvalued_token_positions = decision.unvalued_positions;
        proof.decision = decision;
        metric.live_valuation = Some(proof);
    }
    if expired > 0 {
        bail!(
            "{}: expired_candidates={} surviving_candidates={} publish_floor={}",
            PRICE_EXPIRED,
            expired,
            candidates.len().saturating_sub(expired),
            discovery.effective_publish_min_candidate_wallets()
        );
    }
    status
        .live_portfolio
        .as_mut()
        .expect("coverage validated")
        .unknown_valuation_wallets = unknown_wallets;
    Ok(status)
}

fn validate_proof(
    metric: &DiscoveryV2WalletMetric,
    mut proof: DiscoveryV2LiveValuationEvidence,
    original_now: chrono::DateTime<chrono::Utc>,
    original_start: chrono::DateTime<chrono::Utc>,
    window_minutes: u64,
    max_accounts: usize,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<DiscoveryV2LiveValuationEvidence> {
    let inventory = metric
        .live_inventory
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!(PROOF_UNVERIFIED))?;
    ensure!(
        crate::live_inventory_status::metric_has_inventory_coverage(metric),
        PROOF_UNVERIFIED
    );
    ensure!(
        proof.contract_version == VALUATION_VERSION
            && proof.observed_as_of == original_now
            && proof.observed_window_start == original_start,
        PROOF_UNVERIFIED
    );
    ensure!(
        proof.decision.as_of >= original_now && proof.decision.as_of <= now,
        PROOF_UNVERIFIED
    );
    let duration = chrono::Duration::try_minutes(i64::try_from(window_minutes)?)
        .ok_or_else(|| anyhow::anyhow!(PROOF_UNVERIFIED))?;
    ensure!(
        proof.decision.as_of.checked_sub_signed(duration) == Some(proof.decision.window_start),
        PROOF_UNVERIFIED
    );
    let account_count = inventory
        .classic_accounts
        .checked_add(inventory.token_2022_accounts)
        .ok_or_else(|| anyhow::anyhow!(PROOF_UNVERIFIED))?;
    ensure!(
        account_count <= max_accounts
            && proof.positive_positions as usize <= account_count
            && proof.classic_positions.len() <= inventory.classic_accounts
            && proof.token_2022_positive_positions as usize <= inventory.token_2022_accounts,
        PROOF_UNVERIFIED
    );
    ensure!(
        proof
            .classic_positions
            .len()
            .checked_add(proof.token_2022_positive_positions as usize)
            == Some(proof.positive_positions as usize)
            && metric.live_token_positions == Some(proof.positive_positions)
            && inventory.token_2022_positive_positions == proof.token_2022_positive_positions,
        PROOF_UNVERIFIED
    );
    normalize_numbers(&mut proof)?;
    let mut mints = HashSet::new();
    for row in &proof.classic_positions {
        ensure!(
            valid_pubkey(&row.mint)
                && mints.insert(row.mint.as_str())
                && row.token_amount.is_finite()
                && row.token_amount > 0.0,
            PROOF_UNVERIFIED
        );
        if let Some(price) = &row.observation {
            ensure!(
                !price.signature.is_empty() && price.signature.len() <= 128,
                PROOF_UNVERIFIED
            );
            let mut original = row.clone();
            original.initial_unknown_reason = None;
            ensure!(
                row.initial_unknown_reason
                    == contribution_reason(&original, original_start, original_now),
                PROOF_UNVERIFIED
            );
        } else {
            ensure!(
                matches!(
                    row.initial_unknown_reason,
                    Some(Reason::PriceMissing | Reason::PriceSourceUnverified)
                ),
                PROOF_UNVERIFIED
            );
        }
    }
    let expected = decide_valuation(
        &proof.classic_positions,
        proof.token_2022_positive_positions,
        proof.decision.window_start,
        proof.decision.as_of,
    );
    ensure!(
        numeric_alias_matches(
            proof.decision.known_classic_value_sol,
            expected.known_classic_value_sol
        ),
        PROOF_UNVERIFIED
    );
    proof.decision.known_classic_value_sol = expected.known_classic_value_sol;
    ensure!(
        proof.decision == expected
            && metric
                .live_token_value_sol
                .is_some_and(|alias| numeric_alias_matches(
                    alias,
                    expected.known_classic_value_sol
                ))
            && metric.live_tradable_token_positions == Some(expected.valued_classic_positions)
            && numeric_alias_matches(
                inventory.known_classic_value_sol,
                expected.known_classic_value_sol
            )
            && inventory.unvalued_token_positions == expected.unvalued_positions,
        PROOF_UNVERIFIED
    );
    Ok(proof)
}
