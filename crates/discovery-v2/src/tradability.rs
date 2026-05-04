use crate::accumulator::WalletAccumulator;
use crate::token_market::{is_sol_buy, sol_leg_token_and_notional, SolLegTrade};
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::{SwapEvent, TokenQualityCacheRow};
use std::collections::{HashMap, VecDeque};

#[derive(Debug, Clone, Copy)]
pub(crate) enum BuyTradability {
    Tradable,
    Rejected,
}

#[derive(Debug, Clone, Default)]
struct TokenRollingState {
    sol_trades_5m: VecDeque<SolLegTrade>,
    sol_volume_5m: f64,
    sol_traders_5m: HashMap<String, u32>,
}

pub(crate) fn build_wallet_accumulators(
    swaps: &[SwapEvent],
    discovery: &DiscoveryConfig,
    shadow: &ShadowConfig,
    token_quality_cache: &HashMap<String, TokenQualityCacheRow>,
) -> HashMap<String, WalletAccumulator> {
    let mut wallets = HashMap::new();
    let mut token_states = HashMap::new();
    for swap in swaps {
        let tradability = update_token_state_and_buy_tradability(
            &mut token_states,
            token_quality_cache,
            shadow,
            swap,
        );
        let entry = wallets
            .entry(swap.wallet.clone())
            .or_insert_with(|| WalletAccumulator::new(swap.ts_utc));
        entry.observe_swap(swap, discovery, tradability);
    }
    wallets
}

pub(crate) fn build_token_sol_history(swaps: &[SwapEvent]) -> HashMap<String, Vec<SolLegTrade>> {
    let mut history: HashMap<String, Vec<SolLegTrade>> = HashMap::new();
    for swap in swaps {
        let Some((token, sol_notional)) = sol_leg_token_and_notional(swap) else {
            continue;
        };
        history
            .entry(token.to_string())
            .or_default()
            .push(SolLegTrade {
                ts: swap.ts_utc,
                wallet_id: swap.wallet.clone(),
                sol_notional: sol_notional.max(0.0),
            });
    }
    for trades in history.values_mut() {
        trades.sort_by(|left, right| {
            left.ts
                .cmp(&right.ts)
                .then_with(|| left.wallet_id.cmp(&right.wallet_id))
        });
    }
    history
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
    if shadow.min_token_age_seconds > 0
        && quality
            .and_then(|row| row.token_age_seconds)
            .is_none_or(|age| age < shadow.min_token_age_seconds)
    {
        return BuyTradability::Rejected;
    }
    if shadow.min_holders > 0
        && quality
            .and_then(|row| row.holders)
            .is_none_or(|holders| holders < shadow.min_holders)
    {
        return BuyTradability::Rejected;
    }
    if shadow.min_liquidity_sol > 0.0
        && quality
            .and_then(|row| row.liquidity_sol)
            .is_none_or(|liquidity| liquidity + 1e-12 < shadow.min_liquidity_sol)
    {
        return BuyTradability::Rejected;
    }
    BuyTradability::Tradable
}

fn evict_expired_token_trades(state: &mut TokenRollingState, now: DateTime<Utc>) {
    let cutoff = now - Duration::minutes(5);
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
