use super::*;

impl DiscoveryService {
    pub(super) fn build_shadow_signal_evidence(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        recent_cycles: usize,
        explicit_lookback_seconds: Option<u64>,
        audit: &WalletFreshnessAuditReport,
    ) -> Result<WalletFreshnessShadowSignalEvidence> {
        let selected_wallet_ids = audit.active_follow_wallet_ids.clone();
        let default_evidence_window_seconds = self
            .config
            .refresh_seconds
            .max(1)
            .saturating_mul(recent_cycles.max(1) as u64);
        let evidence_window_seconds = explicit_lookback_seconds
            .unwrap_or(default_evidence_window_seconds)
            .max(1);
        let recent_window_start = now - Duration::seconds(evidence_window_seconds as i64);
        let mut recent_raw_activity_by_wallet = store
            .recent_observed_swap_counts_for_wallets(recent_window_start, &selected_wallet_ids)?;
        recent_raw_activity_by_wallet.sort_by(compare_wallet_recent_activity_rows);
        let mut recent_shadow_signal_by_wallet = store
            .recent_copy_signal_counts_for_wallets_by_status(
                recent_window_start,
                &selected_wallet_ids,
                "shadow_recorded",
            )?;
        recent_shadow_signal_by_wallet.sort_by(compare_wallet_recent_activity_rows);

        let recent_raw_swap_count = recent_raw_activity_by_wallet
            .iter()
            .map(|row| row.row_count)
            .sum();
        let recent_shadow_signal_count = recent_shadow_signal_by_wallet
            .iter()
            .map(|row| row.row_count)
            .sum();
        let recent_raw_activity_wallet_ids = recent_raw_activity_by_wallet
            .iter()
            .map(|row| row.wallet_id.clone())
            .collect::<Vec<_>>();
        let recent_shadow_signal_wallet_ids = recent_shadow_signal_by_wallet
            .iter()
            .map(|row| row.wallet_id.clone())
            .collect::<Vec<_>>();
        let raw_activity_top_wallet_share = dominant_wallet_share(
            recent_raw_activity_by_wallet.as_slice(),
            recent_raw_swap_count,
        );
        let shadow_signal_top_wallet_share = dominant_wallet_share(
            recent_shadow_signal_by_wallet.as_slice(),
            recent_shadow_signal_count,
        );
        let raw_activity_broadly_distributed = activity_broadly_distributed(
            selected_wallet_ids.len(),
            recent_raw_activity_wallet_ids.len(),
            raw_activity_top_wallet_share,
        );
        let shadow_signal_broadly_distributed = activity_broadly_distributed(
            selected_wallet_ids.len(),
            recent_shadow_signal_wallet_ids.len(),
            shadow_signal_top_wallet_share,
        );

        let (verdict, reason) = if selected_wallet_ids.is_empty() {
            (
                WalletShadowSignalVerdict::NoSelectedWallets,
                "no_active_follow_wallets_selected".to_string(),
            )
        } else if recent_raw_swap_count == 0 {
            (
                WalletShadowSignalVerdict::NoRecentSelectedRawActivity,
                "no_recent_observed_swaps_from_selected_wallets".to_string(),
            )
        } else if recent_shadow_signal_count == 0 {
            (
                WalletShadowSignalVerdict::RecentSelectedRawActivityWithoutShadowSignals,
                "selected_wallets_emit_recent_raw_activity_but_no_shadow_signals".to_string(),
            )
        } else if shadow_signal_broadly_distributed {
            (
                WalletShadowSignalVerdict::ShadowSignalsPresentAndDistributed,
                "recent_shadow_signals_present_across_multiple_selected_wallets".to_string(),
            )
        } else {
            (
                WalletShadowSignalVerdict::ShadowSignalsPresentButConcentrated,
                "recent_shadow_signals_present_but_concentrated_in_few_selected_wallets"
                    .to_string(),
            )
        };

        Ok(WalletFreshnessShadowSignalEvidence {
            recent_window_start,
            recent_window_end: now,
            evidence_lookback_seconds: Some(evidence_window_seconds),
            selected_wallet_ids,
            selected_wallet_count: audit.active_follow_wallet_ids.len(),
            selected_wallets_with_recent_raw_activity: recent_raw_activity_wallet_ids.len(),
            selected_wallets_with_recent_shadow_signal: recent_shadow_signal_wallet_ids.len(),
            recent_raw_swap_count,
            recent_shadow_signal_count,
            recent_raw_activity_wallet_ids,
            recent_shadow_signal_wallet_ids,
            recent_raw_activity_by_wallet,
            recent_shadow_signal_by_wallet,
            raw_activity_top_wallet_share,
            shadow_signal_top_wallet_share,
            raw_activity_broadly_distributed,
            shadow_signal_broadly_distributed,
            verdict,
            reason,
        })
    }

    pub(super) fn classify_raw_truth_status(
        &self,
        raw_coverage: ObservedSwapsCoverageSnapshot,
        window_start: DateTime<Utc>,
        now: DateTime<Utc>,
        current_raw: &RawTruthSample,
    ) -> WalletFreshnessRawTruthStatus {
        let short_retention_configured = self.config.observed_swaps_retention_days.max(1)
            < self.config.scoring_window_days.max(1);
        let runtime_freshness_lag_seconds = self.config.refresh_seconds.max(1);
        let covered_through_lag_seconds =
            raw_coverage.covered_through_cursor.as_ref().map(|cursor| {
                now.signed_duration_since(cursor.ts_utc)
                    .num_seconds()
                    .max(0) as u64
            });
        let tail_fresh_within_runtime_lag = covered_through_lag_seconds
            .is_some_and(|lag_seconds| lag_seconds <= runtime_freshness_lag_seconds);
        let (sufficient, reason) = if short_retention_configured {
            (
                false,
                "observed_swaps_retention_shorter_than_scoring_window".to_string(),
            )
        } else if raw_coverage.row_count == 0 {
            (false, "no_observed_swaps_present".to_string())
        } else if current_raw.observed_swaps_loaded == 0 {
            (false, "no_observed_swaps_in_scoring_window".to_string())
        } else if raw_coverage
            .covered_since
            .map(|covered_since| covered_since > window_start)
            .unwrap_or(true)
        {
            (
                false,
                "observed_swaps_coverage_starts_after_scoring_window_start".to_string(),
            )
        } else if !tail_fresh_within_runtime_lag {
            (
                false,
                "observed_swaps_coverage_ends_before_freshness_gate".to_string(),
            )
        } else {
            (true, "full_scoring_window_raw_truth_available".to_string())
        };

        WalletFreshnessRawTruthStatus {
            sufficient,
            reason,
            observed_swaps_loaded: current_raw.observed_swaps_loaded,
            eligible_wallet_count: current_raw.eligible_wallet_count,
            top_wallet_count: current_raw.top_wallet_ids.len(),
            short_retention_configured,
            covered_since: raw_coverage.covered_since,
            covered_through_cursor: raw_coverage.covered_through_cursor,
            covered_through_lag_seconds,
            tail_fresh_within_runtime_lag,
            runtime_freshness_lag_seconds,
            total_observed_swaps_rows: raw_coverage.row_count,
        }
    }
}
