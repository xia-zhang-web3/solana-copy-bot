impl DiscoveryService {
    #[cfg(test)]
    fn snapshot_from_components(
        &self,
        wallet_id: String,
        first_seen: DateTime<Utc>,
        last_seen: DateTime<Utc>,
        trades: u32,
        active_days: u32,
        spent_sol: f64,
        realized_pnl_sol: f64,
        max_buy_notional_sol: f64,
        wins: u32,
        closed_trades: u32,
        hold_samples_sec: &[i64],
        realized_pnl_by_day: &HashMap<NaiveDate, f64>,
        suspicious: bool,
        buy_total: u32,
        quality_resolved_buys: u32,
        tradable_buys: u32,
        rug_metrics: RugMetrics,
        now: DateTime<Utc>,
    ) -> WalletSnapshot {
        self.snapshot_outcome_from_components(
            wallet_id,
            first_seen,
            last_seen,
            trades,
            active_days,
            spent_sol,
            realized_pnl_sol,
            max_buy_notional_sol,
            wins,
            closed_trades,
            hold_samples_sec,
            realized_pnl_by_day,
            suspicious,
            buy_total,
            quality_resolved_buys,
            tradable_buys,
            rug_metrics,
            now,
        )
        .snapshot
    }

    fn snapshot_outcome_from_components(
        &self,
        wallet_id: String,
        first_seen: DateTime<Utc>,
        last_seen: DateTime<Utc>,
        trades: u32,
        active_days: u32,
        spent_sol: f64,
        realized_pnl_sol: f64,
        max_buy_notional_sol: f64,
        wins: u32,
        closed_trades: u32,
        hold_samples_sec: &[i64],
        realized_pnl_by_day: &HashMap<NaiveDate, f64>,
        suspicious: bool,
        buy_total: u32,
        quality_resolved_buys: u32,
        tradable_buys: u32,
        rug_metrics: RugMetrics,
        now: DateTime<Utc>,
    ) -> WalletSnapshotOutcome {
        let resolved_buy_ratio = if buy_total > 0 {
            quality_resolved_buys as f64 / buy_total as f64
        } else {
            0.0
        };
        let tradable_ratio = if quality_resolved_buys > 0 {
            (tradable_buys as f64 / quality_resolved_buys as f64) * resolved_buy_ratio.sqrt()
        } else {
            0.0
        };
        let rug_ratio = if buy_total > 0 {
            (rug_metrics.rugged.saturating_add(rug_metrics.unevaluated)) as f64 / buy_total as f64
        } else {
            0.0
        };
        let win_rate = if closed_trades > 0 {
            wins as f64 / closed_trades as f64
        } else {
            0.0
        };
        let hold_median_seconds = median_i64(hold_samples_sec).unwrap_or(0);
        let consistency_ratio = if active_days > 0 {
            let positive_days = realized_pnl_by_day
                .values()
                .filter(|value| **value > 0.0)
                .count() as f64;
            positive_days / active_days as f64
        } else {
            0.0
        };
        let roi = if spent_sol > 1e-9 {
            realized_pnl_sol / spent_sol
        } else {
            0.0
        };
        let win_sample_factor = (closed_trades as f64 / 8.0).min(1.0);
        let hold_quality = hold_time_quality_score(hold_median_seconds);
        let pnl_component = tanh01(realized_pnl_sol / 2.0);
        let roi_component = tanh01(roi * 3.0);
        let win_component = (win_rate * win_sample_factor).clamp(0.0, 1.0);
        let consistency_component = consistency_ratio.clamp(0.0, 1.0);
        let penalty_component = if suspicious { 0.0 } else { 1.0 };
        let base_score = (0.35 * pnl_component)
            + (0.20 * roi_component)
            + (0.15 * win_component)
            + (0.15 * hold_quality)
            + (0.10 * consistency_component)
            + (0.05 * penalty_component);
        let tradable_penalty = tradable_ratio.powf(1.5);
        let rug_checks_disabled = self.config.max_rug_ratio >= 1.0;
        let rug_penalty = if rug_checks_disabled {
            1.0
        } else {
            (1.0 - rug_ratio).clamp(0.0, 1.0).powi(2)
        };
        let raw_score = (base_score * tradable_penalty * rug_penalty).clamp(0.0, 1.0);
        let decay_cutoff = now - Duration::days(self.config.decay_window_days.max(1) as i64);
        let insufficient_trades = trades < self.config.min_trades;
        let insufficient_active_days = active_days < self.config.min_active_days;
        let suspicious_activity = suspicious;
        let low_notional = max_buy_notional_sol < self.config.min_leader_notional_sol;
        let stale_last_seen = last_seen < decay_cutoff;
        let insufficient_buy_count = buy_total < self.config.min_buy_count;
        let low_tradable_ratio = tradable_ratio < self.config.min_tradable_ratio;
        let rug_gate = !rug_checks_disabled && rug_ratio > self.config.max_rug_ratio;
        let eligible = !insufficient_trades
            && !insufficient_active_days
            && !suspicious_activity
            && !low_notional
            && !stale_last_seen
            && !insufficient_buy_count
            && !low_tradable_ratio
            && !rug_gate;
        let score = if eligible { raw_score } else { 0.0 };

        WalletSnapshotOutcome {
            snapshot: WalletSnapshot {
                wallet_id,
                first_seen,
                last_seen,
                pnl_sol: realized_pnl_sol,
                win_rate,
                trades,
                closed_trades,
                hold_median_seconds,
                score,
                buy_total,
                tradable_ratio,
                rug_ratio,
                eligible,
            },
        }
    }

}
