use crate::accumulator::WalletAccumulator;
use crate::token_market::{is_sol_buy, sol_leg_token_and_notional, SolLegTrade};
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::{SwapEvent, TokenQualityCacheRow};
use std::collections::{HashMap, VecDeque};

pub(crate) const TOKEN_ROLLING_MARKET_WINDOW_SECONDS: i64 =
    copybot_config::DISCOVERY_V2_TOKEN_ROLLING_MARKET_WINDOW_SECONDS;

#[derive(Debug, Clone, Copy)]
pub(crate) enum BuyTradability {
    Tradable,
    Rejected,
    MissingQualityEvidence,
}

#[derive(Debug, Clone, Default)]
struct TokenRollingState {
    sol_trades_5m: VecDeque<SolLegTrade>,
    sol_volume_5m: f64,
    sol_traders_5m: HashMap<String, u32>,
}

#[derive(Debug, Default)]
pub(crate) struct DiscoveryV2WindowAccumulator {
    wallets: HashMap<String, WalletAccumulator>,
    token_states: HashMap<String, TokenRollingState>,
    token_sol_history: HashMap<String, Vec<SolLegTrade>>,
}

impl DiscoveryV2WindowAccumulator {
    pub(crate) fn observe_swap(
        &mut self,
        swap: &SwapEvent,
        discovery: &DiscoveryConfig,
        shadow: &ShadowConfig,
        token_quality_cache: &HashMap<String, TokenQualityCacheRow>,
    ) {
        if let Some((token, sol_notional)) = sol_leg_token_and_notional(swap) {
            self.token_sol_history
                .entry(token.to_string())
                .or_default()
                .push(SolLegTrade {
                    ts: swap.ts_utc,
                    wallet_id: swap.wallet.clone(),
                    sol_notional: sol_notional.max(0.0),
                });
        }
        let tradability = update_token_state_and_buy_tradability(
            &mut self.token_states,
            token_quality_cache,
            shadow,
            swap,
        );
        let entry = self
            .wallets
            .entry(swap.wallet.clone())
            .or_insert_with(|| WalletAccumulator::new(swap.ts_utc));
        entry.observe_swap(swap, discovery, tradability);
    }

    pub(crate) fn into_parts(
        mut self,
    ) -> (
        HashMap<String, WalletAccumulator>,
        HashMap<String, Vec<SolLegTrade>>,
    ) {
        for trades in self.token_sol_history.values_mut() {
            sort_sol_trades(trades);
        }
        (self.wallets, self.token_sol_history)
    }
}

fn sort_sol_trades(trades: &mut [SolLegTrade]) {
    trades.sort_by(|left, right| {
        left.ts
            .cmp(&right.ts)
            .then_with(|| left.wallet_id.cmp(&right.wallet_id))
    });
}

fn update_token_state_and_buy_tradability(
    token_states: &mut HashMap<String, TokenRollingState>,
    token_quality_cache: &HashMap<String, TokenQualityCacheRow>,
    shadow: &ShadowConfig,
    swap: &SwapEvent,
) -> Option<BuyTradability> {
    let Some((token, sol_notional)) = sol_leg_token_and_notional(swap) else {
        return None;
    };
    let state = token_states.entry(token.to_string()).or_default();
    evict_expired_token_trades(state, swap.ts_utc);
    let trade = SolLegTrade {
        ts: swap.ts_utc,
        wallet_id: swap.wallet.clone(),
        sol_notional: sol_notional.max(0.0),
    };
    state.sol_volume_5m += trade.sol_notional;
    state
        .sol_traders_5m
        .entry(trade.wallet_id.clone())
        .and_modify(|count| *count += 1)
        .or_insert(1);
    state.sol_trades_5m.push_back(trade);
    if is_sol_buy(swap) {
        Some(evaluate_buy_tradability(
            state,
            token_quality_cache.get(token),
            shadow,
        ))
    } else {
        None
    }
}

fn evaluate_buy_tradability(
    state: &TokenRollingState,
    quality: Option<&TokenQualityCacheRow>,
    shadow: &ShadowConfig,
) -> BuyTradability {
    if !shadow.quality_gates_enabled {
        return BuyTradability::Tradable;
    }
    if shadow.min_volume_5m_sol > 0.0 && state.sol_volume_5m + 1e-12 < shadow.min_volume_5m_sol {
        return BuyTradability::Rejected;
    }
    if shadow.min_unique_traders_5m > 0
        && state.sol_traders_5m.len() < shadow.min_unique_traders_5m as usize
    {
        return BuyTradability::Rejected;
    }
    let requires_cache_evidence = shadow.min_token_age_seconds > 0
        || shadow.min_holders > 0
        || shadow.min_liquidity_sol > 0.0;
    if requires_cache_evidence {
        let Some(quality) = quality else {
            return BuyTradability::MissingQualityEvidence;
        };
        if shadow.min_token_age_seconds > 0 && quality.token_age_seconds.is_none() {
            return BuyTradability::MissingQualityEvidence;
        }
        if shadow.min_holders > 0 && quality.holders.is_none() {
            return BuyTradability::MissingQualityEvidence;
        }
        if shadow.min_liquidity_sol > 0.0 && quality.liquidity_sol.is_none() {
            return BuyTradability::MissingQualityEvidence;
        }
        if shadow.min_token_age_seconds > 0
            && quality
                .token_age_seconds
                .is_some_and(|age| age < shadow.min_token_age_seconds)
        {
            return BuyTradability::Rejected;
        }
        if shadow.min_holders > 0
            && quality
                .holders
                .is_some_and(|holders| holders < shadow.min_holders)
        {
            return BuyTradability::Rejected;
        }
        if shadow.min_liquidity_sol > 0.0
            && quality
                .liquidity_sol
                .is_some_and(|liquidity| liquidity + 1e-12 < shadow.min_liquidity_sol)
        {
            return BuyTradability::Rejected;
        }
    }
    BuyTradability::Tradable
}

fn evict_expired_token_trades(state: &mut TokenRollingState, now: DateTime<Utc>) {
    let cutoff = now - Duration::seconds(TOKEN_ROLLING_MARKET_WINDOW_SECONDS);
    while state
        .sol_trades_5m
        .front()
        .is_some_and(|front| front.ts < cutoff)
    {
        let expired = state.sol_trades_5m.pop_front().expect("front checked");
        state.sol_volume_5m = (state.sol_volume_5m - expired.sol_notional).max(0.0);
        if let Some(count) = state.sol_traders_5m.get_mut(&expired.wallet_id) {
            *count -= 1;
            if *count == 0 {
                state.sol_traders_5m.remove(&expired.wallet_id);
            }
        }
    }
}
