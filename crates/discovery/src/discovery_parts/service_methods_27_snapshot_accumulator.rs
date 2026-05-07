use super::*;

impl DiscoveryService {
    #[cfg(test)]
    pub(crate) fn snapshot_from_accumulator(
        &self,
        wallet_id: String,
        acc: WalletAccumulator,
        now: DateTime<Utc>,
        token_sol_history: &HashMap<String, Vec<SolLegTrade>>,
    ) -> WalletSnapshot {
        self.snapshot_from_accumulator_with_persisted_active_days(
            wallet_id,
            acc,
            now,
            token_sol_history,
            0,
        )
    }

    pub(crate) fn snapshot_from_accumulator_with_persisted_active_days(
        &self,
        wallet_id: String,
        acc: WalletAccumulator,
        now: DateTime<Utc>,
        token_sol_history: &HashMap<String, Vec<SolLegTrade>>,
        persisted_active_days: u32,
    ) -> WalletSnapshot {
        self.snapshot_outcome_from_accumulator_with_persisted_active_days_internal(
            wallet_id,
            acc,
            now,
            token_sol_history,
            persisted_active_days,
            true,
            false,
        )
        .snapshot
    }

    pub(crate) fn snapshot_outcome_from_accumulator_with_persisted_active_days_internal(
        &self,
        wallet_id: String,
        acc: WalletAccumulator,
        now: DateTime<Utc>,
        token_sol_history: &HashMap<String, Vec<SolLegTrade>>,
        persisted_active_days: u32,
        apply_open_position_gate: bool,
        ignore_rug_gate: bool,
    ) -> WalletSnapshotOutcome {
        let first_seen = acc.first_seen.unwrap_or(now);
        let last_seen = acc.last_seen.unwrap_or(now);
        let active_days = acc
            .exact_active_day_count
            .unwrap_or(acc.active_days.len() as u32);
        let has_actionable_open_positions =
            acc.has_actionable_open_positions(now, self.config.metric_snapshot_interval_seconds);
        let eligibility_active_days = active_days.max(persisted_active_days);
        let (buy_total, quality_resolved_buys, tradable_buys, rug_metrics) =
            if acc.buy_observations.is_empty() {
                (
                    acc.buy_total,
                    acc.quality_resolved_buys,
                    acc.tradable_buys,
                    acc.rug_metrics,
                )
            } else {
                let buy_total = acc.buy_observations.len() as u32;
                let quality_resolved_buys = acc
                    .buy_observations
                    .iter()
                    .filter(|buy| buy.quality_resolved)
                    .count() as u32;
                let tradable_buys = acc
                    .buy_observations
                    .iter()
                    .filter(|buy| buy.quality_resolved && buy.tradable)
                    .count() as u32;
                let rug_metrics =
                    self.compute_rug_metrics(&acc.buy_observations, token_sol_history, now);
                (buy_total, quality_resolved_buys, tradable_buys, rug_metrics)
            };
        let mut outcome = self.snapshot_outcome_from_components(
            wallet_id,
            first_seen,
            last_seen,
            acc.trades,
            eligibility_active_days,
            acc.spent_sol,
            acc.realized_pnl_sol,
            acc.max_buy_notional_sol,
            acc.wins,
            acc.closed_trades,
            &acc.hold_samples_sec,
            &acc.realized_pnl_by_day,
            acc.suspicious,
            buy_total,
            quality_resolved_buys,
            tradable_buys,
            if ignore_rug_gate {
                RugMetrics::default()
            } else {
                rug_metrics
            },
            now,
        );
        if apply_open_position_gate
            && self.config.require_open_positions_for_publication
            && outcome.snapshot.eligible
            && !has_actionable_open_positions
        {
            outcome.snapshot.eligible = false;
            outcome.snapshot.score = 0.0;
        }
        outcome
    }

    pub(crate) fn snapshot_from_persisted_metrics(
        &self,
        row: PersistedWalletMetricSnapshotRow,
        decay_cutoff: DateTime<Utc>,
    ) -> WalletSnapshot {
        let eligible = row.score > 0.0 && row.last_seen >= decay_cutoff;
        WalletSnapshot {
            wallet_id: row.wallet_id,
            first_seen: row.first_seen,
            last_seen: row.last_seen,
            pnl_sol: row.pnl,
            win_rate: row.win_rate,
            trades: row.trades,
            closed_trades: row.closed_trades,
            hold_median_seconds: row.hold_median_seconds,
            score: row.score,
            buy_total: row.buy_total,
            tradable_ratio: row.tradable_ratio,
            rug_ratio: row.rug_ratio,
            eligible,
        }
    }

    pub(crate) fn compute_rug_metrics(
        &self,
        buys: &[BuyObservation],
        token_sol_history: &HashMap<String, Vec<SolLegTrade>>,
        now: DateTime<Utc>,
    ) -> RugMetrics {
        if buys.is_empty() {
            return RugMetrics::default();
        }
        let lookahead = Duration::seconds(self.config.rug_lookahead_seconds.max(1) as i64);
        let mut metrics = RugMetrics::default();

        for buy in buys {
            let window_end = buy.ts + lookahead;
            if window_end > now {
                metrics.unevaluated = metrics.unevaluated.saturating_add(1);
                continue;
            }
            metrics.evaluated = metrics.evaluated.saturating_add(1);
            let Some(trades) = token_sol_history.get(&buy.token) else {
                metrics.rugged = metrics.rugged.saturating_add(1);
                continue;
            };

            let start_idx = trades.partition_point(|trade| trade.ts < buy.ts);
            let end_idx = trades.partition_point(|trade| trade.ts <= window_end);

            let mut volume_sol = 0.0;
            let mut unique_traders = HashSet::new();
            for trade in &trades[start_idx..end_idx] {
                volume_sol += trade.sol_notional;
                unique_traders.insert(trade.wallet_id.as_str());
            }
            let thin_volume = volume_sol + 1e-12 < self.config.thin_market_min_volume_sol;
            let thin_traders =
                unique_traders.len() < self.config.thin_market_min_unique_traders as usize;
            if thin_volume || thin_traders {
                metrics.rugged = metrics.rugged.saturating_add(1);
            }
        }

        metrics
    }
}
