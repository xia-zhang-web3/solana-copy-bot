impl DiscoveryService {
    fn build_rotation_signal_from_cycle_points(
        &self,
        raw_truth_cycle_points: &[RawTruthCyclePoint],
        recent_cycles: usize,
    ) -> WalletFreshnessRotationSignal {
        let cycles_requested = recent_cycles.max(1);
        let sample_interval_seconds = self.config.refresh_seconds.max(1);
        let samples = raw_truth_cycle_points
            .iter()
            .map(|point| WalletFreshnessRawCycleSample {
                sample_now: point.sample_now,
                window_start: point.sample.window_start,
                observed_swaps_loaded: point.sample.observed_swaps_loaded,
                eligible_wallet_count: point.sample.eligible_wallet_count,
                top_wallet_ids: point.sample.top_wallet_ids.clone(),
            })
            .collect::<Vec<_>>();

        if samples.len() < 2 {
            return WalletFreshnessRotationSignal {
                signal_available: false,
                reason: Some("fewer_than_two_raw_truth_cycle_samples".to_string()),
                cycles_requested,
                cycles_completed: samples.len(),
                sample_interval_seconds,
                overlap_with_previous_cycle: None,
                entered_since_previous_cycle: Vec::new(),
                left_since_previous_cycle: Vec::new(),
                stable_wallets_across_cycles: samples
                    .first()
                    .map(|sample| sample.top_wallet_ids.clone())
                    .unwrap_or_default(),
                unique_wallet_count_across_cycles: samples
                    .iter()
                    .flat_map(|sample| sample.top_wallet_ids.iter().cloned())
                    .collect::<BTreeSet<_>>()
                    .len(),
                samples,
            };
        }

        let current = &samples[0].top_wallet_ids;
        let previous = &samples[1].top_wallet_ids;
        let current_set: BTreeSet<_> = current.iter().cloned().collect();
        let previous_set: BTreeSet<_> = previous.iter().cloned().collect();
        let overlap_with_previous_cycle = current_set.intersection(&previous_set).count();
        let entered_since_previous_cycle = current_set
            .difference(&previous_set)
            .cloned()
            .collect::<Vec<_>>();
        let left_since_previous_cycle = previous_set
            .difference(&current_set)
            .cloned()
            .collect::<Vec<_>>();
        let stable_wallets_across_cycles = stable_wallets(&samples);
        let unique_wallet_count_across_cycles = samples
            .iter()
            .flat_map(|sample| sample.top_wallet_ids.iter().cloned())
            .collect::<BTreeSet<_>>()
            .len();

        WalletFreshnessRotationSignal {
            signal_available: true,
            reason: None,
            cycles_requested,
            cycles_completed: samples.len(),
            sample_interval_seconds,
            overlap_with_previous_cycle: Some(overlap_with_previous_cycle),
            entered_since_previous_cycle,
            left_since_previous_cycle,
            stable_wallets_across_cycles,
            unique_wallet_count_across_cycles,
            samples,
        }
    }
}
