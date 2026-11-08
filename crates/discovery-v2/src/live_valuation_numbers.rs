use crate::live_valuation::{
    DiscoveryV2LiveValuationEvidence, DiscoveryV2ValuationDecision, PROOF_UNVERIFIED,
};
use anyhow::{ensure, Result};

/// These are consistency checks on JSON display aliases, never capital/price
/// tolerances. Threshold decisions always use the exact original f64 bit values.
pub(crate) fn numeric_alias_matches(alias: f64, exact: f64) -> bool {
    alias.is_finite()
        && exact.is_finite()
        && alias >= 0.0
        && exact >= 0.0
        && alias.to_bits().abs_diff(exact.to_bits()) <= 2
}

pub(crate) fn normalize_numbers(proof: &mut DiscoveryV2LiveValuationEvidence) -> Result<()> {
    for row in proof
        .classic_positions
        .iter_mut()
        .chain(&mut proof.token_2022_positions)
    {
        let amount = f64::from_bits(row.token_amount_bits);
        ensure!(
            amount > 0.0 && numeric_alias_matches(row.token_amount, amount),
            PROOF_UNVERIFIED
        );
        row.token_amount = amount;
        match (&mut row.observation, row.price_sol_bits) {
            (Some(price), Some(bits)) => {
                let exact = f64::from_bits(bits);
                ensure!(
                    exact > 0.0 && numeric_alias_matches(price.price_sol, exact),
                    PROOF_UNVERIFIED
                );
                price.price_sol = exact;
            }
            (None, None) => {}
            _ => anyhow::bail!(PROOF_UNVERIFIED),
        }
    }
    Ok(())
}

pub(crate) fn normalize_subtotals(
    decision: &mut DiscoveryV2ValuationDecision,
    expected: &DiscoveryV2ValuationDecision,
) -> Result<()> {
    for (alias, exact) in [
        (
            &mut decision.known_classic_value_sol,
            expected.known_classic_value_sol,
        ),
        (
            &mut decision.known_token_2022_value_sol,
            expected.known_token_2022_value_sol,
        ),
        (
            &mut decision.known_supported_token_value_sol,
            expected.known_supported_token_value_sol,
        ),
    ] {
        ensure!(numeric_alias_matches(*alias, exact), PROOF_UNVERIFIED);
        *alias = exact;
    }
    Ok(())
}
