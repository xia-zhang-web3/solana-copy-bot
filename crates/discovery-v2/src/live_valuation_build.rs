use crate::live_inventory::{
    DiscoveryV2LiveInventoryEvidence, DiscoveryV2LiveValuationBasis, LivePortfolioSnapshot,
    TokenProgram, INVENTORY_CONTRACT_VERSION, UNKNOWN_TOKEN_2022_VALUE,
};
use crate::live_portfolio::LivePortfolioEvaluation;
use crate::live_valuation::{
    contribution_reason, decide_valuation, DiscoveryV2LiveValuationEvidence,
    DiscoveryV2PriceContribution, DiscoveryV2PriceUnknownReason as Reason, PRICE_UNKNOWN,
    VALUATION_VERSION,
};
use crate::DiscoveryV2BuildOptions;
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig, DISCOVERY_V2_TOKEN_QUALITY_TTL_SECONDS};
use copybot_core_types::TokenQualityCacheRow;
use copybot_storage_core::{ObservedTokenSolPrice, SqliteDiscoveryStore};
use std::collections::HashMap;

pub(crate) type PriceRead = std::result::Result<Option<ObservedTokenSolPrice>, ()>;

pub(crate) fn evaluate_live_portfolio_snapshot(
    snapshot: &LivePortfolioSnapshot,
    discovery: &DiscoveryConfig,
    shadow: &ShadowConfig,
    prices: &HashMap<String, PriceRead>,
    quality: &HashMap<String, TokenQualityCacheRow>,
    options: &DiscoveryV2BuildOptions,
) -> LivePortfolioEvaluation {
    // Public build validates the window before any scan or RPC.
    let window_start = options.window_start();
    let mut rows = Vec::new();
    let token_2022 = snapshot
        .token_positions
        .iter()
        .filter(|p| p.program == TokenProgram::Token2022)
        .count() as u32;
    for position in snapshot
        .token_positions
        .iter()
        .filter(|p| p.program == TokenProgram::Classic)
    {
        let (observation, failure) = match prices.get(&position.mint) {
            Some(Ok(Some(price))) => (Some(price.clone()), None),
            Some(Err(())) => (None, Some(Reason::PriceSourceUnverified)),
            _ => (None, Some(Reason::PriceMissing)),
        };
        let mut row = DiscoveryV2PriceContribution {
            mint: position.mint.clone(),
            token_amount: position.amount,
            token_amount_bits: position.amount.to_bits(),
            price_sol_bits: observation.as_ref().map(|price| price.price_sol.to_bits()),
            observation,
            quality_eligible: quality_satisfies_shadow_gate(
                quality.get(&position.mint),
                shadow,
                options.now,
            ),
            initial_unknown_reason: failure,
        };
        row.initial_unknown_reason = contribution_reason(&row, window_start, options.now);
        rows.push(row);
    }
    let decision = decide_valuation(&rows, token_2022, window_start, options.now);
    let value = decision.known_classic_value_sol;
    let valued = decision.valued_classic_positions;
    let accepted = snapshot.sol_balance >= discovery.min_live_sol_balance
        || value >= discovery.min_live_portfolio_value_sol;
    let price_unknown = decision.unknown_reasons.keys().any(|reason| {
        !matches!(
            reason,
            Reason::QualityNotEligible | Reason::Token2022Unsupported
        )
    });
    let reject_reason = if accepted {
        None
    } else if token_2022 > 0 {
        Some(UNKNOWN_TOKEN_2022_VALUE)
    } else if price_unknown {
        Some(PRICE_UNKNOWN)
    } else if snapshot.token_positions.is_empty() {
        Some("capital_drained_after_window")
    } else if value > 0.0 {
        Some("only_dust_positions")
    } else {
        Some("only_illiquid_positions")
    };
    let valuation = DiscoveryV2LiveValuationEvidence {
        contract_version: VALUATION_VERSION,
        observed_as_of: options.now,
        observed_window_start: window_start,
        sol_balance_bits: snapshot.sol_balance.to_bits(),
        token_2022_positive_positions: token_2022,
        positive_positions: snapshot.token_positions.len() as u32,
        classic_positions: rows,
        decision,
    };
    evaluation(
        accepted,
        reject_reason,
        snapshot.sol_balance,
        value,
        snapshot,
        valued,
        valuation,
    )
}

pub(crate) fn load_live_token_prices(
    store: &SqliteDiscoveryStore,
    snapshot: &LivePortfolioSnapshot,
    now: DateTime<Utc>,
) -> Result<HashMap<String, PriceRead>> {
    let mut rows = HashMap::new();
    for position in &snapshot.token_positions {
        if position.program == TokenProgram::Classic && !rows.contains_key(&position.mint) {
            // Source/parse failures are valuation unknown, never a fabricated zero
            // observation and never an RPC failure. Raw DB errors are not serialized.
            rows.insert(
                position.mint.clone(),
                store
                    .latest_token_sol_price_observation(&position.mint, now)
                    .map_err(|_| ()),
            );
        }
    }
    Ok(rows)
}

fn quality_satisfies_shadow_gate(
    quality: Option<&TokenQualityCacheRow>,
    shadow: &ShadowConfig,
    now: DateTime<Utc>,
) -> bool {
    if !shadow.quality_gates_enabled {
        return true;
    }
    let Some(quality) = quality else {
        return false;
    };
    if quality.fetched_at > now
        || now - quality.fetched_at > Duration::seconds(DISCOVERY_V2_TOKEN_QUALITY_TTL_SECONDS)
    {
        return false;
    }
    if shadow.min_token_age_seconds > 0
        && !quality
            .token_age_seconds
            .is_some_and(|age| age >= shadow.min_token_age_seconds)
    {
        return false;
    }
    if shadow.min_holders > 0
        && !quality
            .holders
            .is_some_and(|holders| holders >= shadow.min_holders)
    {
        return false;
    }
    if shadow.min_liquidity_sol > 0.0
        && !quality
            .liquidity_sol
            .is_some_and(|liquidity| liquidity + 1e-12 >= shadow.min_liquidity_sol)
    {
        return false;
    }
    true
}

fn evaluation(
    accepted: bool,
    reject_reason: Option<&'static str>,
    sol_balance: f64,
    token_value_sol: f64,
    snapshot: &LivePortfolioSnapshot,
    tradable_token_positions: u32,
    valuation: crate::DiscoveryV2LiveValuationEvidence,
) -> LivePortfolioEvaluation {
    LivePortfolioEvaluation {
        valuation,
        inventory: DiscoveryV2LiveInventoryEvidence {
            contract_version: INVENTORY_CONTRACT_VERSION,
            classic_accounts: snapshot.classic_accounts,
            token_2022_accounts: snapshot.token_2022_accounts,
            sol_slot: snapshot.sol_slot,
            classic_slot: snapshot.classic_slot,
            token_2022_slot: snapshot.token_2022_slot,
            token_2022_positive_positions: snapshot
                .token_positions
                .iter()
                .filter(|p| p.program == TokenProgram::Token2022)
                .count() as u32,
            unvalued_token_positions: (snapshot.token_positions.len() as u32)
                .saturating_sub(tradable_token_positions),
            known_classic_value_sol: token_value_sol,
            valuation_basis: DiscoveryV2LiveValuationBasis::ClassicObservedPriceQualitySubtotal,
        },
        accepted,
        reject_reason,
        sol_balance,
        token_value_sol,
        token_positions: snapshot.token_positions.len() as u32,
        tradable_token_positions,
    }
}
