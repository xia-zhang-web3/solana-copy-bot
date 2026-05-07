use super::*;

impl DiscoveryService {
    pub(crate) fn finalize_streaming_rug_metrics_up_to(
        &self,
        by_wallet: &mut HashMap<String, WalletAccumulator>,
        _token: &str,
        token_recent_sol_trades: &mut HashMap<String, VecDeque<SolLegTrade>>,
        pending_rug_checks: &mut VecDeque<PendingBuyRugCheck>,
        token_pending_buy_starts: &mut HashMap<String, VecDeque<DateTime<Utc>>>,
        up_to_ts: DateTime<Utc>,
        lookahead: Duration,
        now: DateTime<Utc>,
    ) {
        while pending_rug_checks
            .front()
            .map(|buy| buy.buy_ts + lookahead <= up_to_ts)
            .unwrap_or(false)
        {
            let pending = pending_rug_checks
                .pop_front()
                .expect("checked pending buy exists above");
            let rug_status = self.compute_streaming_buy_rug_status(
                token_recent_sol_trades
                    .get(&pending.token)
                    .expect("recent rug trades are initialized before pending buys are recorded"),
                pending.buy_ts,
                lookahead,
                now,
            );
            if let Some(wallet) = by_wallet.get_mut(&pending.wallet_id) {
                wallet.note_streaming_buy_rug_status(rug_status);
            }
            let Some(pending_starts) = token_pending_buy_starts.get_mut(&pending.token) else {
                continue;
            };
            let _ = pending_starts.pop_front();
            let next_needed_ts = pending_starts
                .front()
                .copied()
                .unwrap_or(up_to_ts - lookahead);
            self.evict_streaming_rug_trade_history(
                token_recent_sol_trades,
                &pending.token,
                next_needed_ts,
            );
            if pending_starts.is_empty() {
                token_pending_buy_starts.remove(&pending.token);
            }
        }
    }

    pub(crate) fn finalize_all_streaming_rug_metrics(
        &self,
        by_wallet: &mut HashMap<String, WalletAccumulator>,
        token_recent_sol_trades: &mut HashMap<String, VecDeque<SolLegTrade>>,
        pending_rug_checks: &mut VecDeque<PendingBuyRugCheck>,
        token_pending_buy_starts: &mut HashMap<String, VecDeque<DateTime<Utc>>>,
        now: DateTime<Utc>,
        lookahead: Duration,
    ) {
        self.finalize_streaming_rug_metrics_up_to(
            by_wallet,
            "",
            token_recent_sol_trades,
            pending_rug_checks,
            token_pending_buy_starts,
            now,
            lookahead,
            now,
        );
        while let Some(pending) = pending_rug_checks.pop_front() {
            if let Some(wallet) = by_wallet.get_mut(&pending.wallet_id) {
                wallet.note_streaming_buy_rug_status(BuyFactRugStatus::Unevaluated);
            }
        }
        token_pending_buy_starts.clear();
    }

    pub(crate) fn evict_streaming_rug_trade_history(
        &self,
        token_recent_sol_trades: &mut HashMap<String, VecDeque<SolLegTrade>>,
        token: &str,
        min_ts: DateTime<Utc>,
    ) {
        let Some(recent_trades) = token_recent_sol_trades.get_mut(token) else {
            return;
        };
        while recent_trades
            .front()
            .map(|trade| trade.ts < min_ts)
            .unwrap_or(false)
        {
            recent_trades.pop_front();
        }
        if recent_trades.is_empty() {
            token_recent_sol_trades.remove(token);
        }
    }

    pub(crate) fn evict_idle_streaming_rug_trade_history(
        &self,
        token_recent_sol_trades: &mut HashMap<String, VecDeque<SolLegTrade>>,
        token_pending_buy_starts: &HashMap<String, VecDeque<DateTime<Utc>>>,
        min_ts: DateTime<Utc>,
    ) {
        let tokens: Vec<String> = token_recent_sol_trades.keys().cloned().collect();
        for token in tokens {
            if token_pending_buy_starts.contains_key(&token) {
                continue;
            }
            self.evict_streaming_rug_trade_history(token_recent_sol_trades, &token, min_ts);
        }
    }

    pub(crate) fn compute_streaming_buy_rug_status(
        &self,
        recent_trades: &VecDeque<SolLegTrade>,
        buy_ts: DateTime<Utc>,
        lookahead: Duration,
        now: DateTime<Utc>,
    ) -> BuyFactRugStatus {
        let window_end = buy_ts + lookahead;
        if window_end > now {
            return BuyFactRugStatus::Unevaluated;
        }

        let mut volume_sol = 0.0;
        let mut unique_traders = HashSet::new();
        for trade in recent_trades {
            if trade.ts < buy_ts || trade.ts > window_end {
                continue;
            }
            volume_sol += trade.sol_notional;
            unique_traders.insert(trade.wallet_id.as_str());
        }
        let thin_volume = volume_sol + 1e-12 < self.config.thin_market_min_volume_sol;
        let thin_traders =
            unique_traders.len() < self.config.thin_market_min_unique_traders as usize;
        if thin_volume || thin_traders {
            BuyFactRugStatus::Rugged
        } else {
            BuyFactRugStatus::Healthy
        }
    }

}
