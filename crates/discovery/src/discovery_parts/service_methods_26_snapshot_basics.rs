use super::*;

impl DiscoveryService {
    pub(crate) fn metrics_window_start(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        let interval_seconds = self.config.metric_snapshot_interval_seconds.max(1) as i64;
        let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
        let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
        bucketed_now - Duration::days(self.config.scoring_window_days.max(1) as i64)
    }

    pub(crate) fn record_live_publish(&self, now: DateTime<Utc>) {
        let mut state = match self.window_state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("discovery window mutex poisoned while recording publish time; continuing");
                poisoned.into_inner()
            }
        };
        state.last_publish_at = Some(now);
    }

    pub fn effective_startup_trusted_selection_state(
        &self,
        status: &StartupTrustedSelectionGateStatus,
        now: DateTime<Utc>,
    ) -> Option<TrustedSelectionState> {
        status.effective_selection_state(
            now,
            self.config.scoring_window_days as i64,
            self.config.metric_snapshot_interval_seconds,
            self.config.max_bootstrap_snapshot_age_seconds,
        )
    }

    pub fn effective_startup_trusted_selection_fail_closed(
        &self,
        status: &StartupTrustedSelectionGateStatus,
        now: DateTime<Utc>,
    ) -> bool {
        status.effective_startup_fail_closed(
            now,
            self.config.scoring_window_days as i64,
            self.config.metric_snapshot_interval_seconds,
            self.config.max_bootstrap_snapshot_age_seconds,
        )
    }

    pub(crate) fn wallet_snapshots_from_persisted_metric_rows(
        &self,
        now: DateTime<Utc>,
        rows: Vec<PersistedWalletMetricSnapshotRow>,
    ) -> Vec<WalletSnapshot> {
        let decay_cutoff = now - Duration::days(self.config.decay_window_days.max(1) as i64);
        rows.into_iter()
            .map(|row| self.snapshot_from_persisted_metrics(row, decay_cutoff))
            .collect()
    }

    pub(crate) fn persist_trusted_selection_state(
        &self,
        store: &SqliteStore,
        selection_state: TrustedSelectionState,
        active_snapshot_id: Option<String>,
        active_snapshot_window_start: Option<DateTime<Utc>>,
        source_kind: Option<TrustedSnapshotSourceKind>,
        bootstrap_required: bool,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<()> {
        store.set_discovery_trusted_selection_state(&DiscoveryTrustedSelectionStateUpdate {
            bootstrap_required,
            reason: reason.to_string(),
            selection_state,
            active_snapshot_id,
            active_snapshot_window_start,
            last_bootstrap_source_kind: source_kind,
            last_bootstrap_at: Some(now),
        })
    }

    pub(crate) fn persist_trusted_selection_state_from_snapshot(
        &self,
        store: &SqliteStore,
        snapshot_write: &TrustedWalletMetricsSnapshotWrite,
        bootstrap_required: bool,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<()> {
        self.persist_trusted_selection_state(
            store,
            snapshot_write.trust_state,
            Some(snapshot_write.snapshot_id.clone()),
            Some(snapshot_write.effective_window_start),
            Some(snapshot_write.source_kind),
            bootstrap_required,
            reason,
            now,
        )
    }

    pub(crate) fn build_wallet_snapshots_from_cached(
        &self,
        store: &SqliteStore,
        swaps: &VecDeque<SwapEvent>,
        now: DateTime<Utc>,
    ) -> Result<Vec<WalletSnapshot>> {
        let mut ordered_swaps: Vec<&SwapEvent> = swaps.iter().collect();
        if ordered_swaps
            .windows(2)
            .any(|pair| cmp_swap_order(pair[1], pair[0]) == Ordering::Less)
        {
            ordered_swaps.sort_by(|a, b| cmp_swap_order(a, b));
            warn!(
                swaps_window = swaps.len(),
                "discovery swap order invariant violated before snapshot rebuild; normalizing cached window"
            );
        }
        let mut by_wallet: HashMap<String, WalletAccumulator> = HashMap::new();
        let mut seen_buy_mints = HashSet::new();
        let mut unique_buy_mints = Vec::new();
        for swap in ordered_swaps
            .iter()
            .copied()
            .filter(|swap| is_sol_buy(swap))
        {
            if seen_buy_mints.insert(swap.token_out.clone()) {
                unique_buy_mints.push(swap.token_out.clone());
            }
        }
        let token_quality_cache =
            self.resolve_token_quality_for_mints(store, &unique_buy_mints, now)?;
        let mut token_states: HashMap<String, TokenRollingState> = HashMap::new();
        let mut token_sol_history: HashMap<String, Vec<SolLegTrade>> = HashMap::new();
        for swap in ordered_swaps.iter().copied() {
            let buy_quality = self.update_token_quality_state(
                &mut token_states,
                &mut token_sol_history,
                &token_quality_cache,
                swap,
            );
            let buy_quality_requires_retry = is_sol_buy(swap)
                && Self::token_quality_resolution_requires_publish_pending_retry(
                    token_quality_cache.get(&swap.token_out),
                );
            let entry = by_wallet.entry(swap.wallet.clone()).or_default();
            entry.observe_swap(
                swap,
                self.config.max_tx_per_minute,
                buy_quality,
                buy_quality_requires_retry,
            );
        }

        self.wallet_snapshots_from_accumulators(store, by_wallet, now, &token_sol_history)
    }

}
