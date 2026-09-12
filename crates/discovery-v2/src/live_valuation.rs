use chrono::{DateTime, Utc};
use copybot_storage_core::ObservedTokenSolPrice;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub(crate) const VALUATION_VERSION: u8 = 1;
pub(crate) const PRICE_UNKNOWN: &str = "live_portfolio_price_valuation_unknown";
pub(crate) const PROOF_UNVERIFIED: &str = "discovery_v2_price_proof_unverified_rebuild_required";
pub(crate) const PRICE_EXPIRED: &str = "discovery_v2_price_support_expired_rebuild_required";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryV2PriceUnknownReason {
    PriceMissing,
    PriceSourceUnverified,
    PriceOutsideWindow,
    PriceFuture,
    PriceInvalid,
    QualityNotEligible,
    ValueInvalid,
    Token2022Unsupported,
}

/// One row for every positive classic program/mint position, without sampling.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiscoveryV2PriceContribution {
    pub mint: String,
    pub token_amount: f64,
    /// Exact model value across JSON parsers; numeric fields remain display aliases.
    pub token_amount_bits: u64,
    pub observation: Option<ObservedTokenSolPrice>,
    pub price_sol_bits: Option<u64>,
    pub quality_eligible: bool,
    pub initial_unknown_reason: Option<DiscoveryV2PriceUnknownReason>,
}

/// A derived view of the original observations at an explicit decision time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiscoveryV2ValuationDecision {
    pub as_of: DateTime<Utc>,
    pub window_start: DateTime<Utc>,
    pub known_classic_value_sol: f64,
    pub valued_classic_positions: u32,
    pub unvalued_positions: u32,
    pub unknown_reasons: BTreeMap<DiscoveryV2PriceUnknownReason, u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiscoveryV2LiveValuationEvidence {
    pub contract_version: u8,
    /// Original build context. Reuse never updates these or observation timestamps.
    pub observed_as_of: DateTime<Utc>,
    pub observed_window_start: DateTime<Utc>,
    pub sol_balance_bits: u64,
    pub token_2022_positive_positions: u32,
    pub positive_positions: u32,
    pub classic_positions: Vec<DiscoveryV2PriceContribution>,
    pub decision: DiscoveryV2ValuationDecision,
}

pub(crate) fn contribution_reason(
    row: &DiscoveryV2PriceContribution,
    window_start: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Option<DiscoveryV2PriceUnknownReason> {
    use DiscoveryV2PriceUnknownReason as Reason;
    let Some(price) = &row.observation else {
        return Some(row.initial_unknown_reason.unwrap_or(Reason::PriceMissing));
    };
    if !price.price_sol.is_finite() || price.price_sol <= 0.0 {
        return Some(Reason::PriceInvalid);
    }
    if price.observed_at > now {
        return Some(Reason::PriceFuture);
    }
    if price.observed_at < window_start {
        return Some(Reason::PriceOutsideWindow);
    }
    if !row.quality_eligible {
        return Some(Reason::QualityNotEligible);
    }
    let value = row.token_amount * price.price_sol;
    if !value.is_finite() || value <= 0.0 {
        return Some(Reason::ValueInvalid);
    }
    None
}

pub(crate) fn decide_valuation(
    rows: &[DiscoveryV2PriceContribution],
    token_2022: u32,
    window_start: DateTime<Utc>,
    now: DateTime<Utc>,
) -> DiscoveryV2ValuationDecision {
    let mut decision = DiscoveryV2ValuationDecision {
        as_of: now,
        window_start,
        known_classic_value_sol: 0.0,
        valued_classic_positions: 0,
        unvalued_positions: token_2022,
        unknown_reasons: BTreeMap::new(),
    };
    if token_2022 > 0 {
        decision.unknown_reasons.insert(
            DiscoveryV2PriceUnknownReason::Token2022Unsupported,
            token_2022,
        );
    }
    for row in rows {
        // Reuse can retire support but cannot promote an originally unknown price.
        let mut reason = row
            .initial_unknown_reason
            .or_else(|| contribution_reason(row, window_start, now));
        if reason.is_none() {
            let value =
                row.token_amount * row.observation.as_ref().expect("price checked").price_sol;
            if (decision.known_classic_value_sol + value).is_finite() {
                decision.known_classic_value_sol += value;
                decision.valued_classic_positions += 1;
            } else {
                reason = Some(DiscoveryV2PriceUnknownReason::ValueInvalid);
            }
        }
        if let Some(reason) = reason {
            decision.unvalued_positions += 1;
            *decision.unknown_reasons.entry(reason).or_default() += 1;
        }
    }
    decision
}
