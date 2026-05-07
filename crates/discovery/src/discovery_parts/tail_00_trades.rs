use super::*;

impl WalletAccumulator {
    pub(super) fn observe_buy(
        &mut self,
        token: &str,
        qty: f64,
        cost_sol: f64,
        ts: DateTime<Utc>,
        tradability: BuyTradability,
        quality_requires_retry: bool,
    ) {
        if qty <= 0.0 || cost_sol <= 0.0 {
            return;
        }
        let (tradable, quality_resolved) = match tradability {
            BuyTradability::Tradable => (true, true),
            BuyTradability::Rejected => (false, true),
            BuyTradability::Deferred => (false, false),
        };
        self.buy_total = self.buy_total.saturating_add(1);
        if quality_resolved {
            self.quality_resolved_buys = self.quality_resolved_buys.saturating_add(1);
        }
        if tradable {
            self.tradable_buys = self.tradable_buys.saturating_add(1);
        }
        if quality_requires_retry && !tradable {
            self.publish_pending_quality_retry_buy_count = self
                .publish_pending_quality_retry_buy_count
                .saturating_add(1);
        }
        self.buy_mints.insert(token.to_string());
        self.buy_observations.push(BuyObservation {
            token: token.to_string(),
            ts,
            tradable,
            quality_resolved,
        });
        self.spent_sol += cost_sol;
        if cost_sol > self.max_buy_notional_sol {
            self.max_buy_notional_sol = cost_sol;
        }
        self.positions
            .entry(token.to_string())
            .or_default()
            .push_back(Lot {
                qty,
                cost_sol,
                opened_at: ts,
            });
    }

    pub(super) fn observe_buy_streaming(
        &mut self,
        token: &str,
        qty: f64,
        cost_sol: f64,
        ts: DateTime<Utc>,
        tradability: BuyTradability,
        quality_requires_retry: bool,
    ) {
        if qty <= 0.0 || cost_sol <= 0.0 {
            return;
        }
        let (tradable, quality_resolved) = match tradability {
            BuyTradability::Tradable => (true, true),
            BuyTradability::Rejected => (false, true),
            BuyTradability::Deferred => (false, false),
        };
        self.buy_total = self.buy_total.saturating_add(1);
        if quality_resolved {
            self.quality_resolved_buys = self.quality_resolved_buys.saturating_add(1);
        }
        if tradable {
            self.tradable_buys = self.tradable_buys.saturating_add(1);
        }
        if quality_requires_retry && !tradable {
            self.publish_pending_quality_retry_buy_count = self
                .publish_pending_quality_retry_buy_count
                .saturating_add(1);
        }
        self.buy_mints.insert(token.to_string());
        self.spent_sol += cost_sol;
        if cost_sol > self.max_buy_notional_sol {
            self.max_buy_notional_sol = cost_sol;
        }
        self.positions
            .entry(token.to_string())
            .or_default()
            .push_back(Lot {
                qty,
                cost_sol,
                opened_at: ts,
            });
    }

    pub(super) fn note_streaming_buy_rug_status(&mut self, rug_status: BuyFactRugStatus) {
        match rug_status {
            BuyFactRugStatus::Healthy => {
                self.rug_metrics.evaluated = self.rug_metrics.evaluated.saturating_add(1);
            }
            BuyFactRugStatus::Rugged => {
                self.rug_metrics.evaluated = self.rug_metrics.evaluated.saturating_add(1);
                self.rug_metrics.rugged = self.rug_metrics.rugged.saturating_add(1);
            }
            BuyFactRugStatus::Unevaluated => {
                self.rug_metrics.unevaluated = self.rug_metrics.unevaluated.saturating_add(1);
            }
        }
    }

    pub(super) fn observe_sell(
        &mut self,
        token: &str,
        qty: f64,
        proceeds_sol: f64,
        ts: DateTime<Utc>,
    ) {
        if qty <= 0.0 || proceeds_sol <= 0.0 {
            return;
        }
        let Some(lots) = self.positions.get_mut(token) else {
            return;
        };

        let mut qty_remaining = qty;
        let mut matched_qty = 0.0;
        let mut sell_pnl = 0.0;
        while qty_remaining > 1e-12 {
            if lots.front().is_none() {
                break;
            }
            if lots.front().map(|lot| lot.qty <= 1e-12).unwrap_or(false) {
                let _ = lots.pop_front();
                continue;
            }

            let (take_qty, cost_part, opened_at, should_remove) = {
                let front_lot = lots.front_mut().expect("checked non-empty above");
                let take_qty = qty_remaining.min(front_lot.qty);
                let original_qty = front_lot.qty;
                let opened_at = front_lot.opened_at;
                let lot_fraction = take_qty / original_qty;
                let cost_part = front_lot.cost_sol * lot_fraction;
                front_lot.qty -= take_qty;
                front_lot.cost_sol -= cost_part;
                let should_remove = front_lot.qty <= 1e-12;
                (take_qty, cost_part, opened_at, should_remove)
            };
            if should_remove {
                let _ = lots.pop_front();
            }

            let proceeds_part = proceeds_sol * (take_qty / qty);
            sell_pnl += proceeds_part - cost_part;
            matched_qty += take_qty;
            qty_remaining -= take_qty;

            let hold_sec = (ts - opened_at).num_seconds().max(0);
            self.hold_samples_sec.push(hold_sec);
            if should_remove && lots.is_empty() {
                break;
            }
        }

        if matched_qty <= 1e-12 {
            return;
        }
        self.realized_pnl_sol += sell_pnl;
        self.closed_trades = self.closed_trades.saturating_add(1);
        if sell_pnl > 0.0 {
            self.wins = self.wins.saturating_add(1);
        }
        *self
            .realized_pnl_by_day
            .entry(ts.date_naive())
            .or_insert(0.0) += sell_pnl;
    }

    pub(super) fn mark_tx_minute(&mut self, minute_bucket: i64, max_tx_per_minute: u32) {
        let next = self
            .tx_per_minute
            .entry(minute_bucket)
            .and_modify(|value| *value += 1)
            .or_insert(1);
        if *next > max_tx_per_minute.max(1) {
            self.suspicious = true;
        }
    }
}
