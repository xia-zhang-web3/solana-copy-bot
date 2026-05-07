use super::*;

impl DiscoveryService {
    pub(crate) fn update_token_quality_state(
        &self,
        token_states: &mut HashMap<String, TokenRollingState>,
        token_sol_history: &mut HashMap<String, Vec<SolLegTrade>>,
        token_quality_cache: &HashMap<String, TokenQualityResolution>,
        swap: &SwapEvent,
    ) -> Option<BuyTradability> {
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
        Some(self.evaluate_buy_tradability(state, token_quality_cache.get(token), swap.ts_utc))
    }

    pub(crate) fn update_token_quality_state_streaming(
        &self,
        token_states: &mut HashMap<String, TokenRollingState>,
        token_recent_sol_trades: &mut HashMap<String, VecDeque<SolLegTrade>>,
        token_quality_cache: &HashMap<String, TokenQualityResolution>,
        swap: &SwapEvent,
    ) -> Option<BuyTradability> {
        self.touch_token_state(token_states, &swap.token_in, &swap.wallet, swap.ts_utc);
        self.touch_token_state(token_states, &swap.token_out, &swap.wallet, swap.ts_utc);

        let (token, sol_notional) = if is_sol_buy(swap) {
            (swap.token_out.as_str(), swap.amount_in)
        } else if is_sol_sell(swap) {
            (swap.token_in.as_str(), swap.amount_out)
        } else {
            return None;
        };

        let trade = SolLegTrade {
            ts: swap.ts_utc,
            wallet_id: swap.wallet.clone(),
            sol_notional: sol_notional.max(0.0),
        };
        token_recent_sol_trades
            .entry(token.to_string())
            .or_default()
            .push_back(trade.clone());

        let state = token_states.entry(token.to_string()).or_default();
        Self::evict_expired_5m(state, swap.ts_utc);
        state.sol_volume_5m += trade.sol_notional;
        state
            .sol_traders_5m
            .entry(trade.wallet_id.clone())
            .and_modify(|count| *count += 1)
            .or_insert(1);
        state.sol_trades_5m.push_back(trade);

        if !is_sol_buy(swap) {
            return None;
        }

        let state = token_states
            .get_mut(token)
            .expect("token state is initialized when streaming trade is recorded");
        Self::evict_expired_5m(state, swap.ts_utc);
        Some(self.evaluate_buy_tradability(state, token_quality_cache.get(token), swap.ts_utc))
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
}
