use super::*;

impl WalletAccumulator {
    #[cfg(test)]
    pub(crate) fn has_open_positions(&self) -> bool {
        self.positions
            .values()
            .flatten()
            .any(|lot| lot.qty > 1e-12 && lot.cost_sol > 1e-12)
    }

    pub(crate) fn actionable_open_position_max_age_seconds(
        &self,
        metric_snapshot_interval_seconds: u64,
    ) -> Option<i64> {
        if self.hold_samples_sec.len() < ACTIONABLE_OPEN_POSITION_MIN_HOLD_SAMPLES {
            return None;
        }
        let cadence_floor_seconds =
            i64::try_from(metric_snapshot_interval_seconds.max(1)).unwrap_or(i64::MAX);
        let historical_hold_allowance_seconds = self
            .hold_samples_sec
            .iter()
            .copied()
            .max()
            .unwrap_or(0)
            .saturating_mul(ACTIONABLE_OPEN_POSITION_HOLD_MULTIPLIER);
        Some(cadence_floor_seconds.max(historical_hold_allowance_seconds))
    }

    pub(crate) fn has_actionable_open_positions(
        &self,
        now: DateTime<Utc>,
        metric_snapshot_interval_seconds: u64,
    ) -> bool {
        let max_open_age_seconds =
            self.actionable_open_position_max_age_seconds(metric_snapshot_interval_seconds);
        self.positions.values().flatten().any(|lot| {
            lot.qty > 1e-12
                && lot.cost_sol > 1e-12
                && max_open_age_seconds
                    .is_none_or(|age_limit| (now - lot.opened_at).num_seconds().max(0) <= age_limit)
        })
    }

    pub(crate) fn observe_position_only(&mut self, swap: &SwapEvent) {
        if is_sol_buy(swap) {
            self.observe_position_only_buy(
                swap.token_out.as_str(),
                swap.amount_out,
                swap.amount_in,
                swap.ts_utc,
            );
            return;
        }
        if is_sol_sell(swap) {
            self.observe_position_only_sell(swap.token_in.as_str(), swap.amount_in);
        }
    }

    pub(crate) fn observe_position_only_buy(
        &mut self,
        token: &str,
        qty: f64,
        cost_sol: f64,
        ts: DateTime<Utc>,
    ) {
        if qty <= 0.0 || cost_sol <= 0.0 {
            return;
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

    pub(crate) fn observe_position_only_sell(&mut self, token: &str, qty: f64) {
        if qty <= 0.0 {
            return;
        }
        let Some(lots) = self.positions.get_mut(token) else {
            return;
        };

        let mut qty_remaining = qty;
        while qty_remaining > 1e-12 {
            if lots.front().is_none() {
                break;
            }
            if lots.front().map(|lot| lot.qty <= 1e-12).unwrap_or(false) {
                let _ = lots.pop_front();
                continue;
            }

            let should_remove = {
                let front_lot = lots.front_mut().expect("checked non-empty above");
                let take_qty = qty_remaining.min(front_lot.qty);
                let original_qty = front_lot.qty;
                let lot_fraction = take_qty / original_qty;
                let cost_part = front_lot.cost_sol * lot_fraction;
                front_lot.qty -= take_qty;
                front_lot.cost_sol -= cost_part;
                qty_remaining -= take_qty;
                front_lot.qty <= 1e-12
            };
            if should_remove {
                let _ = lots.pop_front();
            }
        }

        if lots.is_empty() {
            self.positions.remove(token);
        }
    }
}
