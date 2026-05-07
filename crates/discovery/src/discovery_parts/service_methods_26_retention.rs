use super::*;

impl DiscoveryService {
    pub(crate) fn discovery_critical_target_buy_mints_from_accumulators(
        &self,
        store: &SqliteStore,
        by_wallet: &HashMap<String, WalletAccumulator>,
        now: DateTime<Utc>,
    ) -> Result<Vec<String>> {
        if by_wallet.is_empty() {
            return Ok(Vec::new());
        }

        let wallet_ids: Vec<String> = by_wallet.keys().cloned().collect();
        let persisted_active_day_counts =
            self.load_persisted_active_day_counts_for_wallet_ids(store, &wallet_ids, now)?;
        let empty_token_sol_history = HashMap::new();
        let mut counterfactual_open_gate_candidate_wallet_ids = Vec::new();
        for (wallet_id, acc) in by_wallet {
            let persisted_active_days = persisted_active_day_counts
                .get(wallet_id)
                .copied()
                .unwrap_or(0);
            let snapshot = self
                .snapshot_outcome_from_accumulator_with_persisted_active_days_internal(
                    wallet_id.clone(),
                    acc.clone(),
                    now,
                    &empty_token_sol_history,
                    persisted_active_days,
                    false,
                    true,
                )
                .snapshot;
            if snapshot.eligible && snapshot.score >= self.config.min_score {
                counterfactual_open_gate_candidate_wallet_ids.push(wallet_id.clone());
            }
        }

        if counterfactual_open_gate_candidate_wallet_ids.is_empty() {
            return Ok(Vec::new());
        }

        let reconstructed_positions_by_wallet = self
            .reconstruct_retention_window_positions_for_wallets(
                store,
                &counterfactual_open_gate_candidate_wallet_ids,
                now,
            )?;
        let mut target_buy_mints = BTreeSet::new();
        for wallet_id in counterfactual_open_gate_candidate_wallet_ids {
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
            let snapshot = self
                .snapshot_outcome_from_accumulator_with_persisted_active_days_internal(
                    wallet_id.clone(),
                    gated_acc,
                    now,
                    &empty_token_sol_history,
                    persisted_active_days,
                    true,
                    true,
                )
                .snapshot;
            if snapshot.eligible && snapshot.score >= self.config.min_score {
                target_buy_mints.extend(acc.buy_mints.iter().cloned());
            }
        }

        Ok(target_buy_mints.into_iter().collect())
    }

    pub(crate) fn reconstruct_retention_window_positions_for_wallets(
        &self,
        store: &SqliteStore,
        wallet_ids: &[String],
        now: DateTime<Utc>,
    ) -> Result<HashMap<String, HashMap<String, VecDeque<Lot>>>> {
        let retention_days = self.config.observed_swaps_retention_days.max(1);
        let scoring_days = self.config.scoring_window_days.max(1);
        if retention_days <= scoring_days || wallet_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let retention_window_start = now - Duration::days(retention_days as i64);
        let wallet_filter: HashSet<&str> = wallet_ids.iter().map(String::as_str).collect();
        let mut positions_by_wallet: HashMap<String, WalletAccumulator> = HashMap::new();
        store.for_each_observed_swap_in_window_paged(
            retention_window_start,
            now,
            OBSERVED_SWAP_WINDOW_PAGED_READ_LIMIT,
            |swap| {
                if wallet_filter.contains(swap.wallet.as_str()) {
                    positions_by_wallet
                        .entry(swap.wallet.clone())
                        .or_default()
                        .observe_position_only(&swap);
                }
                Ok(())
            },
        )?;

        Ok(positions_by_wallet
            .into_iter()
            .map(|(wallet_id, acc)| (wallet_id, acc.positions))
            .collect())
    }
}
