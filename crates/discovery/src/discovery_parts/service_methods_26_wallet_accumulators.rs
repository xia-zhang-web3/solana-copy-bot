use super::*;

impl DiscoveryService {
    pub(crate) fn wallet_snapshots_from_accumulators(
        &self,
        store: &SqliteStore,
        by_wallet: HashMap<String, WalletAccumulator>,
        now: DateTime<Utc>,
        token_sol_history: &HashMap<String, Vec<SolLegTrade>>,
    ) -> Result<Vec<WalletSnapshot>> {
        Ok(self
            .wallet_snapshot_outcomes_from_accumulators(store, by_wallet, now, token_sol_history)?
            .into_iter()
            .map(|outcome| outcome.snapshot)
            .collect())
    }

    pub(crate) fn wallet_snapshot_outcomes_from_accumulators(
        &self,
        store: &SqliteStore,
        by_wallet: HashMap<String, WalletAccumulator>,
        now: DateTime<Utc>,
        token_sol_history: &HashMap<String, Vec<SolLegTrade>>,
    ) -> Result<Vec<WalletSnapshotOutcome>> {
        let wallet_ids: Vec<String> = by_wallet.keys().cloned().collect();
        let persisted_active_day_counts =
            self.load_persisted_active_day_counts_for_wallet_ids(store, &wallet_ids, now)?;

        let mut outcomes = Vec::with_capacity(by_wallet.len());
        let mut snapshot_indexes_by_wallet = HashMap::with_capacity(by_wallet.len());
        let mut pre_gate_candidate_wallet_ids = Vec::new();
        for (wallet_id, acc) in &by_wallet {
            let persisted_active_days = persisted_active_day_counts
                .get(wallet_id)
                .copied()
                .unwrap_or(0);
            let outcome = self
                .snapshot_outcome_from_accumulator_with_persisted_active_days_internal(
                    wallet_id.clone(),
                    acc.clone(),
                    now,
                    token_sol_history,
                    persisted_active_days,
                    false,
                    false,
                );
            if outcome.snapshot.eligible {
                pre_gate_candidate_wallet_ids.push(wallet_id.clone());
            }
            snapshot_indexes_by_wallet.insert(wallet_id.clone(), outcomes.len());
            outcomes.push(outcome);
        }
        if !self.config.require_open_positions_for_publication
            || pre_gate_candidate_wallet_ids.is_empty()
        {
            return Ok(outcomes);
        }

        let reconstructed_positions_by_wallet = self
            .reconstruct_retention_window_positions_for_wallets(
                store,
                &pre_gate_candidate_wallet_ids,
                now,
            )?;
        for wallet_id in pre_gate_candidate_wallet_ids {
            let Some(acc) = by_wallet.get(&wallet_id) else {
                continue;
            };
            let mut gated_acc = acc.clone();
            if let Some(reconstructed_positions) = reconstructed_positions_by_wallet.get(&wallet_id)
            {
                gated_acc.positions = reconstructed_positions.clone();
            }
            let persisted_active_days = persisted_active_day_counts
                .get(&wallet_id)
                .copied()
                .unwrap_or(0);
            let gated_outcome = self
                .snapshot_outcome_from_accumulator_with_persisted_active_days_internal(
                    wallet_id.clone(),
                    gated_acc,
                    now,
                    token_sol_history,
                    persisted_active_days,
                    true,
                    false,
                );
            if let Some(index) = snapshot_indexes_by_wallet.get(&wallet_id).copied() {
                outcomes[index] = gated_outcome;
            }
        }

        Ok(outcomes)
    }

    pub(crate) fn load_persisted_active_day_counts_for_wallet_ids(
        &self,
        store: &SqliteStore,
        wallet_ids: &[String],
        now: DateTime<Utc>,
    ) -> Result<HashMap<String, u32>> {
        self.load_persisted_active_day_counts_for_wallet_ids_since(
            store,
            wallet_ids,
            now - Duration::days(self.config.scoring_window_days.max(1) as i64),
        )
    }

    pub(crate) fn load_persisted_active_day_counts_for_wallet_ids_since(
        &self,
        store: &SqliteStore,
        wallet_ids: &[String],
        window_start: DateTime<Utc>,
    ) -> Result<HashMap<String, u32>> {
        match store.wallet_active_day_counts_since(wallet_ids, window_start) {
            Ok(counts) => Ok(counts),
            Err(error) => {
                if discovery_wallet_activity_day_count_error_requires_abort(&error) {
                    return Err(error).context(
                        "failed loading persisted wallet activity-day counts with fatal sqlite I/O",
                    );
                }
                warn!(
                    error = %error,
                    wallet_count = wallet_ids.len(),
                    "failed loading persisted wallet activity-day counts; falling back to cached discovery window"
                );
                Ok(HashMap::new())
            }
        }
    }

}
