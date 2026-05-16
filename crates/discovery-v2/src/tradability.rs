use crate::accumulator::WalletAccumulator;
use crate::token_market::{is_sol_buy, is_sol_sell, sol_leg_token_and_notional, SolLegTrade};
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
    sol_traders_5m: HashMap<u32, u32>,
}

#[derive(Debug, Default)]
struct TokenRugState {
    pending_buys: VecDeque<PendingRugBuy>,
}

#[derive(Debug)]
struct PendingRugBuy {
    wallet_id: String,
    window_end: DateTime<Utc>,
    sol_volume: f64,
    unique_traders: HashMap<u32, ()>,
    observed_trades: u32,
    saturated: bool,
}

#[derive(Debug, Default)]
pub(crate) struct DiscoveryV2WindowAccumulator {
    wallets: HashMap<String, WalletAccumulator>,
    trader_ids: HashMap<String, u32>,
    token_states: HashMap<String, TokenRollingState>,
    rug_states: HashMap<String, TokenRugState>,
}

impl DiscoveryV2WindowAccumulator {
    pub(crate) fn observe_swap(
        &mut self,
        swap: &SwapEvent,
        discovery: &DiscoveryConfig,
        shadow: &ShadowConfig,
        token_quality_cache: &HashMap<String, TokenQualityCacheRow>,
    ) {
        let trader_id = self.trader_id_for_wallet(&swap.wallet);
        if let Some((token, _)) = sol_leg_token_and_notional(swap) {
            self.finalize_expired_rug_buys(token, swap.ts_utc, discovery);
        }
        let tradability = update_token_state_and_buy_tradability(
            &mut self.token_states,
            token_quality_cache,
            shadow,
            trader_id,
            swap,
        );
        if is_sol_buy(swap) || (is_sol_sell(swap) && self.wallets.contains_key(&swap.wallet)) {
            let entry = self
                .wallets
                .entry(swap.wallet.clone())
                .or_insert_with(|| WalletAccumulator::new(swap.ts_utc));
            entry.observe_swap(swap, discovery, tradability);
        }
        if let Some((token, sol_notional)) = sol_leg_token_and_notional(swap) {
            if is_sol_buy(swap) && swap.amount_out > 0.0 && swap.amount_in > 0.0 {
                self.observe_pending_rug_buy(
                    token,
                    &swap.wallet,
                    swap.ts_utc,
                    discovery.rug_lookahead_seconds,
                );
            }
            self.observe_rug_trade(token, trader_id, sol_notional.max(0.0), discovery);
        }
    }

    pub(crate) fn wallet_count(&self) -> usize {
        self.wallets.len()
    }

    pub(crate) fn finalize_rug_lookahead(
        &mut self,
        now: DateTime<Utc>,
        discovery: &DiscoveryConfig,
    ) {
        let tokens = self.rug_states.keys().cloned().collect::<Vec<_>>();
        for token in tokens {
            self.finalize_due_rug_buys(&token, now, discovery, true);
        }
    }

    pub(crate) fn into_parts(self) -> (HashMap<String, WalletAccumulator>, HashMap<String, u32>) {
        (self.wallets, self.trader_ids)
    }

    fn trader_id_for_wallet(&mut self, wallet: &str) -> u32 {
        if let Some(id) = self.trader_ids.get(wallet) {
            return *id;
        }
        let next = self
            .trader_ids
            .len()
            .saturating_add(1)
            .min(u32::MAX as usize) as u32;
        self.trader_ids.insert(wallet.to_string(), next);
        next
    }

    fn observe_pending_rug_buy(
        &mut self,
        token: &str,
        wallet_id: &str,
        opened_at: DateTime<Utc>,
        lookahead_seconds: u64,
    ) {
        let lookahead = Duration::seconds(lookahead_seconds.max(1) as i64);
        self.rug_states
            .entry(token.to_string())
            .or_default()
            .pending_buys
            .push_back(PendingRugBuy {
                wallet_id: wallet_id.to_string(),
                window_end: opened_at + lookahead,
                sol_volume: 0.0,
                unique_traders: HashMap::new(),
                observed_trades: 0,
                saturated: false,
            });
    }

    fn observe_rug_trade(
        &mut self,
        token: &str,
        trader_id: u32,
        sol_notional: f64,
        discovery: &DiscoveryConfig,
    ) {
        let mut non_rugged_wallets = Vec::new();
        let trader_cap = discovery.thin_market_min_unique_traders.max(1) as usize;
        let volume_cap = discovery.thin_market_min_volume_sol.max(0.0);
        {
            let Some(state) = self.rug_states.get_mut(token) else {
                return;
            };
            for pending in &mut state.pending_buys {
                pending.observed_trades = pending.observed_trades.saturating_add(1);
                pending.sol_volume = (pending.sol_volume + sol_notional).min(volume_cap);
                if pending.unique_traders.len() < trader_cap {
                    pending.unique_traders.insert(trader_id, ());
                }
                pending.saturated = pending.sol_volume + 1e-12 >= volume_cap
                    && pending.unique_traders.len() >= trader_cap;
                if pending.saturated && pending.observed_trades > 1 {
                    non_rugged_wallets.push(pending.wallet_id.clone());
                }
            }
            if !non_rugged_wallets.is_empty() {
                state
                    .pending_buys
                    .retain(|pending| !(pending.saturated && pending.observed_trades > 1));
            }
        }
        for wallet_id in non_rugged_wallets {
            if let Some(wallet) = self.wallets.get_mut(&wallet_id) {
                wallet.observe_rug_lookahead(false);
            }
        }
    }

    fn finalize_expired_rug_buys(
        &mut self,
        token: &str,
        observed_at: DateTime<Utc>,
        discovery: &DiscoveryConfig,
    ) {
        self.finalize_due_rug_buys(token, observed_at, discovery, false);
    }

    fn finalize_due_rug_buys(
        &mut self,
        token: &str,
        boundary: DateTime<Utc>,
        discovery: &DiscoveryConfig,
        inclusive: bool,
    ) {
        let finalized = {
            let Some(state) = self.rug_states.get_mut(token) else {
                return;
            };
            let mut finalized = Vec::new();
            while state.pending_buys.front().is_some_and(|pending| {
                pending.window_end < boundary || (inclusive && pending.window_end <= boundary)
            }) {
                let pending = state.pending_buys.pop_front().expect("front checked");
                finalized.push((
                    pending.wallet_id,
                    rug_result(pending.sol_volume, pending.unique_traders.len(), discovery),
                ));
            }
            finalized
        };
        for (wallet_id, rugged) in finalized {
            if let Some(wallet) = self.wallets.get_mut(&wallet_id) {
                wallet.observe_rug_lookahead(rugged);
            }
        }
    }
}

fn rug_result(sol_volume: f64, unique_traders: usize, discovery: &DiscoveryConfig) -> bool {
    let thin_volume = sol_volume + 1e-12 < discovery.thin_market_min_volume_sol;
    let thin_traders = unique_traders < discovery.thin_market_min_unique_traders as usize;
    thin_volume || thin_traders
}

fn update_token_state_and_buy_tradability(
    token_states: &mut HashMap<String, TokenRollingState>,
    token_quality_cache: &HashMap<String, TokenQualityCacheRow>,
    shadow: &ShadowConfig,
    trader_id: u32,
    swap: &SwapEvent,
) -> Option<BuyTradability> {
    let Some((token, sol_notional)) = sol_leg_token_and_notional(swap) else {
        return None;
    };
    let state = token_states.entry(token.to_string()).or_default();
    evict_expired_token_trades(state, swap.ts_utc);
    let trade = SolLegTrade {
        ts: swap.ts_utc,
        trader_id,
        sol_notional: sol_notional.max(0.0),
    };
    state.sol_volume_5m += trade.sol_notional;
    state
        .sol_traders_5m
        .entry(trade.trader_id)
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
        if let Some(count) = state.sol_traders_5m.get_mut(&expired.trader_id) {
            *count -= 1;
            if *count == 0 {
                state.sol_traders_5m.remove(&expired.trader_id);
            }
        }
    }
}
