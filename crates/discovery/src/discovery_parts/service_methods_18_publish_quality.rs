use super::*;

impl DiscoveryService {
    pub(crate) fn publish_pending_snapshots(state: &PersistedStreamRebuildState) -> Vec<WalletSnapshot> {
        state.payload.completed_snapshots.clone()
    }

    pub(crate) fn publish_pending_requested_wallet_ids_from_snapshots(
        &self,
        snapshots: &[WalletSnapshot],
    ) -> Vec<String> {
        let ranked = rank_follow_candidates(snapshots, self.config.min_score);
        desired_wallets(&ranked, self.config.follow_top_n)
    }

    pub(crate) fn publish_pending_requested_wallet_ids_from_state(
        &self,
        state: &PersistedStreamRebuildState,
    ) -> Vec<String> {
        state
            .payload
            .publish_pending_requested_wallet_ids
            .clone()
            .unwrap_or_else(|| {
                self.publish_pending_requested_wallet_ids_from_snapshots(
                    &state.payload.completed_snapshots,
                )
            })
    }

    pub(crate) fn publish_pending_quality_retry_mints_from_state(
        state: &PersistedStreamRebuildState,
    ) -> Vec<String> {
        state
            .payload
            .publish_pending_quality_retry_mints
            .clone()
            .unwrap_or_default()
    }

    pub(crate) fn unresolved_publish_quality_mints(
        payload: &PersistedStreamRebuildPayload,
        now: DateTime<Utc>,
    ) -> Vec<String> {
        payload
            .unique_buy_mints
            .iter()
            .filter(|mint| {
                !payload
                    .token_quality_cache
                    .get(*mint)
                    .is_some_and(|resolution| {
                        Self::token_quality_resolution_is_reusable_for_resume(resolution, now)
                    })
            })
            .cloned()
            .collect()
    }

    pub(crate) fn token_quality_resolution_requires_publish_pending_retry(
        resolution: Option<&quality_cache::TokenQualityResolution>,
    ) -> bool {
        !matches!(
            resolution,
            Some(quality_cache::TokenQualityResolution::Fresh(_))
        )
    }

    pub(crate) fn quality_resolution_can_change_publish_set(&self) -> bool {
        let quality_sensitive_tradability = self.shadow_quality.quality_gates_enabled
            && (self.shadow_quality.min_token_age_seconds > 0
                || self.shadow_quality.min_holders > 0
                || self.shadow_quality.min_liquidity_sol > 0.0);
        quality_sensitive_tradability
            && (self.config.min_tradable_ratio > 0.0 || self.config.min_score > 0.0)
    }

    pub(crate) fn zero_publish_set_is_likely_quality_blocked(&self, snapshots: &[WalletSnapshot]) -> bool {
        snapshots.iter().any(|snapshot| {
            snapshot.buy_total > 0
                && snapshot.score == 0.0
                && snapshot.tradable_ratio < 1.0
                && (snapshot.tradable_ratio < self.config.min_tradable_ratio
                    || self.config.min_score > 0.0)
        })
    }

    pub(crate) fn legacy_publish_pending_quality_retry_backfill_mints(
        &self,
        state: &PersistedStreamRebuildState,
    ) -> Vec<String> {
        let likely_quality_blocked =
            self.zero_publish_set_is_likely_quality_blocked(&state.payload.completed_snapshots);
        if likely_quality_blocked {
            state.payload.unique_buy_mints.clone()
        } else {
            Vec::new()
        }
    }

    pub(crate) fn replay_completion_publish_quality_retry_mints(
        &self,
        store: &SqliteStore,
        by_wallet: &HashMap<String, WalletAccumulator>,
        unresolved_mints: &[String],
        snapshots: &[WalletSnapshot],
        now: DateTime<Utc>,
    ) -> Result<Vec<String>> {
        if unresolved_mints.is_empty()
            || !self.quality_resolution_can_change_publish_set()
            || !self.zero_publish_set_is_likely_quality_blocked(snapshots)
        {
            return Ok(Vec::new());
        }

        let candidate_wallet_ids: Vec<String> = snapshots
            .iter()
            .filter(|snapshot| {
                snapshot.buy_total > 0
                    && (!snapshot.eligible || snapshot.score < self.config.min_score)
            })
            .map(|snapshot| snapshot.wallet_id.clone())
            .collect();
        if candidate_wallet_ids.is_empty() {
            return Ok(Vec::new());
        }

        let persisted_active_day_counts = match store.wallet_active_day_counts_since(
            &candidate_wallet_ids,
            now - Duration::days(self.config.scoring_window_days.max(1) as i64),
        ) {
            Ok(counts) => counts,
            Err(error) => {
                if discovery_wallet_activity_day_count_error_requires_abort(&error) {
                    return Err(error).context(
                        "failed loading persisted wallet activity-day counts while deciding whether publish-pending exact quality retry is causally required",
                    );
                }
                warn!(
                    error = %error,
                    wallet_count = candidate_wallet_ids.len(),
                    "failed loading persisted wallet activity-day counts while deciding whether publish-pending exact quality retry is causally required; falling back to buffered replay window activity only"
                );
                HashMap::new()
            }
        };
        let empty_token_sol_history = HashMap::new();

        let retry_needed = candidate_wallet_ids.iter().any(|wallet_id| {
            let Some(acc) = by_wallet.get(wallet_id) else {
                return false;
            };
            let retry_buy_count = acc
                .publish_pending_quality_retry_buy_count
                .min(acc.buy_total);
            if retry_buy_count == 0 {
                return false;
            }
            let mut counterfactual = acc.clone();
            counterfactual.buy_observations.clear();
            counterfactual.quality_resolved_buys = counterfactual.buy_total;
            counterfactual.tradable_buys = counterfactual
                .tradable_buys
                .saturating_add(retry_buy_count)
                .min(counterfactual.buy_total);
            let persisted_active_days = persisted_active_day_counts
                .get(wallet_id)
                .copied()
                .unwrap_or(0);
            let counterfactual_snapshot = self
                .snapshot_from_accumulator_with_persisted_active_days(
                    wallet_id.clone(),
                    counterfactual,
                    now,
                    &empty_token_sol_history,
                    persisted_active_days,
                );
            counterfactual_snapshot.eligible
                && counterfactual_snapshot.score >= self.config.min_score
        });

        Ok(if retry_needed {
            unresolved_mints.to_vec()
        } else {
            Vec::new()
        })
    }

}
