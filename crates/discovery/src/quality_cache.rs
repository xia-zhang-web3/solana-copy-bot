use super::{
    is_sol_buy, is_sol_sell, DiscoveryService, SolLegTrade, TokenRollingState,
    QUALITY_CACHE_TTL_SECONDS, QUALITY_MAX_FETCH_PER_CYCLE, QUALITY_MAX_SIGNATURE_PAGES,
    QUALITY_RPC_BUDGET_MS, QUALITY_RPC_TIMEOUT_MS,
};
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::SwapEvent;
use copybot_storage::{SqliteStore, TokenQualityCacheRow, TokenQualityRpcRow};
use std::collections::{HashMap, HashSet};
use std::time::Instant;
use tracing::{info, warn};

impl DiscoveryService {
    pub(super) fn resolve_token_quality_for_mints(
        &self,
        store: &SqliteStore,
        mints: &HashSet<String>,
        now: DateTime<Utc>,
    ) -> HashMap<String, TokenQualityCacheRow> {
        if mints.is_empty() {
            return HashMap::new();
        }

        let mut out = HashMap::new();
        let ttl = Duration::seconds(QUALITY_CACHE_TTL_SECONDS);
        let mut to_fetch = Vec::new();
        let mut fresh_hits = 0usize;
        let mut stale_hits = 0usize;
        let mut misses = 0usize;

        for mint in mints {
            match store.get_token_quality_cache(mint) {
                Ok(Some(row)) => {
                    if now - row.fetched_at <= ttl {
                        fresh_hits += 1;
                        out.insert(mint.clone(), row);
                    } else {
                        stale_hits += 1;
                        to_fetch.push((mint.clone(), Some(row)));
                    }
                }
                Ok(None) => {
                    misses += 1;
                    to_fetch.push((mint.clone(), None))
                }
                Err(error) => {
                    warn!(error = %error, mint = %mint, "failed reading token quality cache");
                }
            }
        }

        let Some(helius_http_url) = self.helius_http_url.as_deref() else {
            for (mint, stale_row) in to_fetch {
                if let Some(row) = stale_row {
                    out.insert(mint, row);
                }
            }
            info!(
                quality_source = "cache+db_proxy",
                rpc_enabled = false,
                mints_total = mints.len(),
                cache_fresh = fresh_hits,
                cache_stale = stale_hits,
                cache_miss = misses,
                fetched_ok = 0usize,
                fetched_fail = 0usize,
                fallback_from_stale = stale_hits,
                budget_exhausted = 0usize,
                rpc_attempted = 0usize,
                rpc_budget_ms = QUALITY_RPC_BUDGET_MS,
                rpc_spent_ms = 0u64,
                "discovery token quality cache summary"
            );
            return out;
        };

        let mut fetched_ok = 0usize;
        let mut fetched_fail = 0usize;
        let mut fallback_from_stale = 0usize;
        let mut budget_exhausted = 0usize;
        let mut rpc_attempted = 0usize;
        let refresh_started = Instant::now();
        for (mint, stale_row) in to_fetch {
            let mut stale_fallback = stale_row;
            if rpc_attempted >= QUALITY_MAX_FETCH_PER_CYCLE {
                if let Some(stale_row) = stale_fallback.take() {
                    fallback_from_stale += 1;
                    out.insert(mint, stale_row);
                }
                continue;
            }
            if refresh_started.elapsed().as_millis() as u64 >= QUALITY_RPC_BUDGET_MS {
                budget_exhausted += 1;
                if let Some(stale_row) = stale_fallback.take() {
                    fallback_from_stale += 1;
                    out.insert(mint, stale_row);
                }
                continue;
            }

            rpc_attempted += 1;
            match fetch_token_quality_from_helius_guarded(
                helius_http_url,
                &mint,
                QUALITY_RPC_TIMEOUT_MS,
                QUALITY_MAX_SIGNATURE_PAGES,
                Some(self.shadow_quality.min_token_age_seconds),
            ) {
                Ok(fetched) => {
                    if let Err(error) = store.upsert_token_quality_cache(
                        &mint,
                        fetched.holders,
                        fetched.liquidity_sol,
                        fetched.token_age_seconds,
                        now,
                    ) {
                        warn!(error = %error, mint = %mint, "failed updating token quality cache");
                    }
                    match store.get_token_quality_cache(&mint) {
                        Ok(Some(row)) => {
                            fetched_ok += 1;
                            out.insert(mint, row);
                        }
                        Ok(None) => {
                            if let Some(stale_row) = stale_fallback.take() {
                                fallback_from_stale += 1;
                                out.insert(mint, stale_row);
                            } else {
                                warn!(
                                    mint = %mint,
                                    "token quality cache row missing after refresh"
                                );
                            }
                        }
                        Err(error) => {
                            warn!(
                                error = %error,
                                mint = %mint,
                                "failed reading token quality cache after refresh"
                            );
                            if let Some(stale_row) = stale_fallback.take() {
                                fallback_from_stale += 1;
                                out.insert(mint, stale_row);
                            }
                        }
                    }
                }
                Err(error) => {
                    fetched_fail += 1;
                    warn!(
                        error = %error,
                        mint = %mint,
                        "failed to refresh token quality via helius, using fallback"
                    );
                    if let Some(stale_row) = stale_fallback.take() {
                        fallback_from_stale += 1;
                        out.insert(mint, stale_row);
                    }
                }
            }
        }

        let rpc_spent_ms = refresh_started.elapsed().as_millis() as u64;
        info!(
            quality_source = "cache+rpc+db_proxy",
            rpc_enabled = true,
            mints_total = mints.len(),
            cache_fresh = fresh_hits,
            cache_stale = stale_hits,
            cache_miss = misses,
            fetched_ok,
            fetched_fail,
            fallback_from_stale,
            budget_exhausted,
            rpc_attempted,
            rpc_budget_ms = QUALITY_RPC_BUDGET_MS,
            rpc_spent_ms,
            "discovery token quality cache summary"
        );

        out
    }

    pub(super) fn update_token_quality_state(
        &self,
        token_states: &mut HashMap<String, TokenRollingState>,
        token_sol_history: &mut HashMap<String, Vec<SolLegTrade>>,
        token_quality_cache: &HashMap<String, TokenQualityCacheRow>,
        swap: &SwapEvent,
    ) -> Option<bool> {
        self.touch_token_state(token_states, &swap.token_in, &swap.wallet, swap.ts_utc);
        self.touch_token_state(token_states, &swap.token_out, &swap.wallet, swap.ts_utc);

        let (token, sol_notional) = if is_sol_buy(swap) {
            (swap.token_out.as_str(), swap.amount_in)
        } else if is_sol_sell(swap) {
            (swap.token_in.as_str(), swap.amount_out)
        } else {
            return None;
        };

        self.push_sol_leg_trade(
            token_states,
            token_sol_history,
            token,
            swap.wallet.as_str(),
            swap.ts_utc,
            sol_notional.max(0.0),
        );

        if !is_sol_buy(swap) {
            return None;
        }

        let state = token_states
            .get_mut(token)
            .expect("token state is initialized when push_sol_leg_trade is called");
        Self::evict_expired_5m(state, swap.ts_utc);
        Some(self.is_tradable_token(state, token_quality_cache.get(token), swap.ts_utc))
    }

    fn touch_token_state(
        &self,
        token_states: &mut HashMap<String, TokenRollingState>,
        token: &str,
        wallet_id: &str,
        ts: DateTime<Utc>,
    ) {
        let state = token_states.entry(token.to_string()).or_default();
        state.first_seen = Some(
            state
                .first_seen
                .map(|current| current.min(ts))
                .unwrap_or(ts),
        );
        state.wallets_seen.insert(wallet_id.to_string());
    }

    fn push_sol_leg_trade(
        &self,
        token_states: &mut HashMap<String, TokenRollingState>,
        token_sol_history: &mut HashMap<String, Vec<SolLegTrade>>,
        token: &str,
        wallet_id: &str,
        ts: DateTime<Utc>,
        sol_notional: f64,
    ) {
        let trade = SolLegTrade {
            ts,
            wallet_id: wallet_id.to_string(),
            sol_notional,
        };
        token_sol_history
            .entry(token.to_string())
            .or_default()
            .push(trade.clone());

        let state = token_states.entry(token.to_string()).or_default();
        Self::evict_expired_5m(state, ts);
        state.sol_volume_5m += trade.sol_notional;
        state
            .sol_traders_5m
            .entry(trade.wallet_id.clone())
            .and_modify(|count| *count += 1)
            .or_insert(1);
        state.sol_trades_5m.push_back(trade);
    }

    fn evict_expired_5m(state: &mut TokenRollingState, now: DateTime<Utc>) {
        let cutoff = now - Duration::minutes(5);
        while let Some(front) = state.sol_trades_5m.front() {
            if front.ts >= cutoff {
                break;
            }
            let expired = state
                .sol_trades_5m
                .pop_front()
                .expect("checked front exists above");
            state.sol_volume_5m = (state.sol_volume_5m - expired.sol_notional).max(0.0);
            if let Some(count) = state.sol_traders_5m.get_mut(&expired.wallet_id) {
                *count -= 1;
                if *count == 0 {
                    state.sol_traders_5m.remove(&expired.wallet_id);
                }
            }
        }
    }

    fn is_tradable_token(
        &self,
        state: &TokenRollingState,
        rpc_quality: Option<&TokenQualityCacheRow>,
        _signal_ts: DateTime<Utc>,
    ) -> bool {
        let liquidity_proxy = state
            .sol_trades_5m
            .iter()
            .map(|trade| trade.sol_notional)
            .fold(0.0, f64::max);
        if self.shadow_quality.min_volume_5m_sol > 0.0
            && state.sol_volume_5m + 1e-12 < self.shadow_quality.min_volume_5m_sol
        {
            return false;
        }
        if self.shadow_quality.min_unique_traders_5m > 0
            && state.sol_traders_5m.len() < self.shadow_quality.min_unique_traders_5m as usize
        {
            return false;
        }

        // Discovery should never hard-fail on missing RPC fields.
        // Age/holders/liquidity are enforced here only when RPC provided them.
        if let Some(row) = rpc_quality {
            if self.shadow_quality.min_token_age_seconds > 0 {
                if let Some(token_age_seconds) = row.token_age_seconds {
                    if token_age_seconds < self.shadow_quality.min_token_age_seconds {
                        return false;
                    }
                }
            }
            if self.shadow_quality.min_holders > 0 {
                if let Some(holders) = row.holders {
                    if holders < self.shadow_quality.min_holders {
                        return false;
                    }
                }
            }
            if self.shadow_quality.min_liquidity_sol > 0.0 {
                if let Some(liquidity_sol) = row.liquidity_sol {
                    if liquidity_sol + 1e-12 < self.shadow_quality.min_liquidity_sol {
                        return false;
                    }
                } else if liquidity_proxy + 1e-12 < self.shadow_quality.min_liquidity_sol {
                    return false;
                }
            }
        }
        true
    }
}

fn fetch_token_quality_from_helius_guarded(
    helius_http_url: &str,
    mint: &str,
    timeout_ms: u64,
    max_signature_pages: u32,
    min_age_hint_seconds: Option<u64>,
) -> Result<TokenQualityRpcRow> {
    SqliteStore::fetch_token_quality_from_helius(
        helius_http_url,
        mint,
        timeout_ms,
        max_signature_pages,
        min_age_hint_seconds,
    )
}
