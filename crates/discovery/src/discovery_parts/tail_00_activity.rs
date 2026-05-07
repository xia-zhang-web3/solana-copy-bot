use super::*;

impl WalletAccumulator {
    pub(super) fn reset_activity_summary_for_exact_backfill(&mut self) {
        self.first_seen = None;
        self.last_seen = None;
        self.trades = 0;
        self.exact_active_day_count = None;
        self.active_days.clear();
        self.tx_per_minute.clear();
        self.suspicious = false;
    }

    pub(super) fn observe_activity_only(&mut self, swap: &SwapEvent, max_tx_per_minute: u32) {
        self.trades = self.trades.saturating_add(1);
        self.first_seen = Some(
            self.first_seen
                .map(|current| current.min(swap.ts_utc))
                .unwrap_or(swap.ts_utc),
        );
        self.last_seen = Some(
            self.last_seen
                .map(|current| current.max(swap.ts_utc))
                .unwrap_or(swap.ts_utc),
        );
        self.exact_active_day_count = None;
        self.active_days.insert(swap.ts_utc.date_naive());
        self.mark_tx_minute(swap.ts_utc.timestamp() / 60, max_tx_per_minute);
    }

    pub(super) fn observe_swap(
        &mut self,
        swap: &SwapEvent,
        max_tx_per_minute: u32,
        buy_tradability: Option<BuyTradability>,
        buy_quality_requires_retry: bool,
    ) {
        self.observe_activity_only(swap, max_tx_per_minute);

        if is_sol_buy(swap) {
            self.observe_buy(
                swap.token_out.as_str(),
                swap.amount_out,
                swap.amount_in,
                swap.ts_utc,
                buy_tradability.unwrap_or(BuyTradability::Rejected),
                buy_quality_requires_retry,
            );
            return;
        }
        if is_sol_sell(swap) {
            self.observe_sell(
                swap.token_in.as_str(),
                swap.amount_in,
                swap.amount_out,
                swap.ts_utc,
            );
        }
    }

    pub(super) fn observe_swap_streaming(
        &mut self,
        swap: &SwapEvent,
        max_tx_per_minute: u32,
        buy_tradability: Option<BuyTradability>,
        buy_quality_requires_retry: bool,
    ) {
        self.observe_activity_only(swap, max_tx_per_minute);

        if is_sol_buy(swap) {
            self.observe_buy_streaming(
                swap.token_out.as_str(),
                swap.amount_out,
                swap.amount_in,
                swap.ts_utc,
                buy_tradability.unwrap_or(BuyTradability::Rejected),
                buy_quality_requires_retry,
            );
            return;
        }
        if is_sol_sell(swap) {
            self.observe_sell(
                swap.token_in.as_str(),
                swap.amount_in,
                swap.amount_out,
                swap.ts_utc,
            );
        }
    }
}
