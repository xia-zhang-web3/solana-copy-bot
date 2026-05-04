impl DiscoveryService {
    fn wallet_freshness_audit_with_cycle_points(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        recent_cycles: usize,
        raw_truth_cycle_points: &[RawTruthCyclePoint],
    ) -> Result<WalletFreshnessAuditReport> {
        let current_raw = raw_truth_cycle_points
            .first()
            .map(|point| &point.sample)
            .context("wallet freshness audit requires at least one raw-truth cycle sample")?;
        let window_start = current_raw.window_start;
        let publication_state = store.discovery_publication_state_read_only()?;
        let publication_truth = publication_truth_for_audit(publication_state.as_ref());
        let publication_recent_under_gate = publication_state
            .as_ref()
            .is_some_and(|state| state.is_fresh_under_gate(self.publication_freshness_gate(), now));
        let active_follow_wallet_ids =
            sorted_wallets_from_iter(store.list_active_follow_wallets()?);
        let raw_coverage = store.observed_swaps_coverage_snapshot()?;
        let raw_truth =
            self.classify_raw_truth_status(raw_coverage, window_start, now, current_raw);

        let published_wallet_ids = publication_truth
            .as_ref()
            .map(|truth| truth.published_wallet_ids.clone())
            .unwrap_or_default();
        let published_vs_current_raw =
            compare_wallet_universes(&published_wallet_ids, &current_raw.top_wallet_ids);
        let active_follow_vs_current_raw =
            compare_wallet_universes(&active_follow_wallet_ids, &current_raw.top_wallet_ids);
        let active_follow_vs_published =
            compare_wallet_universes(&active_follow_wallet_ids, &published_wallet_ids);
        let rotation =
            self.build_rotation_signal_from_cycle_points(raw_truth_cycle_points, recent_cycles);

        let verdict = if publication_truth.is_none() {
            WalletFreshnessVerdict::FailClosedNoPublicationTruth
        } else if !raw_truth.sufficient {
            WalletFreshnessVerdict::InsufficientRawTruth
        } else if published_vs_current_raw.exact_match && active_follow_vs_published.exact_match {
            WalletFreshnessVerdict::FreshCurrent
        } else if publication_recent_under_gate && active_follow_vs_published.exact_match {
            WalletFreshnessVerdict::DriftingButAcceptable
        } else {
            WalletFreshnessVerdict::StalePublicationTruth
        };

        let reason = match verdict {
            WalletFreshnessVerdict::FailClosedNoPublicationTruth => {
                "no_complete_publication_truth".to_string()
            }
            WalletFreshnessVerdict::InsufficientRawTruth => raw_truth.reason.clone(),
            WalletFreshnessVerdict::FreshCurrent => {
                "published_and_active_match_current_raw_top_n".to_string()
            }
            WalletFreshnessVerdict::DriftingButAcceptable => {
                "publication_recent_under_gate_but_current_raw_top_n_has_rotated".to_string()
            }
            WalletFreshnessVerdict::StalePublicationTruth => {
                if !active_follow_vs_published.exact_match {
                    "active_follow_universe_drifted_from_published_truth".to_string()
                } else if !publication_recent_under_gate {
                    "publication_truth_not_recent_under_gate".to_string()
                } else {
                    "published_universe_drifted_from_current_raw_top_n".to_string()
                }
            }
        };

        Ok(WalletFreshnessAuditReport {
            now,
            window_start,
            verdict,
            reason,
            follow_top_n: self.config.follow_top_n as usize,
            publication_truth_available: publication_truth.is_some(),
            publication_runtime_mode: publication_truth
                .as_ref()
                .map(|truth| truth.runtime_mode.as_str().to_string()),
            publication_recent_under_gate,
            latest_publication_ts: publication_truth
                .as_ref()
                .map(|truth| truth.last_published_at),
            publication_age_seconds: publication_truth.as_ref().map(|truth| {
                now.signed_duration_since(truth.last_published_at)
                    .num_seconds()
                    .max(0) as u64
            }),
            latest_publication_window_start: publication_truth
                .as_ref()
                .map(|truth| truth.last_published_window_start),
            published_scoring_source: publication_truth
                .as_ref()
                .and_then(|truth| truth.published_scoring_source.clone()),
            published_wallet_ids,
            active_follow_wallet_ids,
            current_raw_top_wallet_ids: current_raw.top_wallet_ids.clone(),
            published_vs_current_raw,
            active_follow_vs_current_raw,
            active_follow_vs_published,
            raw_truth,
            rotation,
        })
    }
}
