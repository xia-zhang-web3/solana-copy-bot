use super::*;

impl DiscoveryService {
    pub fn wallet_freshness_audit(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        recent_cycles: usize,
    ) -> Result<WalletFreshnessAuditReport> {
        let recent_cycles = recent_cycles.max(1);
        let raw_truth_cycle_points =
            self.collect_raw_truth_cycle_points(store, now, recent_cycles)?;
        self.wallet_freshness_audit_with_cycle_points(
            store,
            now,
            recent_cycles,
            raw_truth_cycle_points.as_slice(),
        )
    }

    pub fn wallet_freshness_capture_snapshot(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        recent_cycles: usize,
    ) -> Result<WalletFreshnessCaptureSnapshot> {
        Ok(self
            .wallet_freshness_capture_snapshot_measured(store, now, recent_cycles)?
            .snapshot)
    }

    pub fn wallet_freshness_capture_snapshot_measured(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        recent_cycles: usize,
    ) -> Result<WalletFreshnessCaptureComputation> {
        self.wallet_freshness_capture_snapshot_measured_with_lookback(
            store,
            now,
            recent_cycles,
            None,
        )
    }

    pub fn wallet_freshness_capture_snapshot_measured_with_lookback(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        recent_cycles: usize,
        shadow_evidence_lookback_seconds: Option<u64>,
    ) -> Result<WalletFreshnessCaptureComputation> {
        let recent_cycles = recent_cycles.max(1);
        let raw_truth_started = Instant::now();
        let raw_truth_cycle_points =
            self.collect_raw_truth_cycle_points(store, now, recent_cycles)?;
        let raw_truth_build_duration_ms = raw_truth_started.elapsed().as_millis() as u64;
        let audit = self.wallet_freshness_audit_with_cycle_points(
            store,
            now,
            recent_cycles,
            raw_truth_cycle_points.as_slice(),
        )?;
        let shadow_signal_started = Instant::now();
        let shadow_signal = self.build_shadow_signal_evidence(
            store,
            now,
            recent_cycles,
            shadow_evidence_lookback_seconds,
            &audit,
        )?;
        let shadow_signal_duration_ms = shadow_signal_started.elapsed().as_millis() as u64;
        Ok(WalletFreshnessCaptureComputation {
            snapshot: WalletFreshnessCaptureSnapshot {
                capture_id: None,
                captured_at: now,
                capture_age_seconds: None,
                within_recent_horizon: None,
                recent_cycles,
                audit,
                shadow_signal,
            },
            raw_truth_build_duration_ms,
            shadow_signal_duration_ms,
        })
    }

    pub fn wallet_freshness_capture_snapshot_from_precomputed_current_raw(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        current_raw: PrecomputedWalletFreshnessCurrentRawTruth,
        shadow_evidence_lookback_seconds: Option<u64>,
    ) -> Result<WalletFreshnessCaptureComputation> {
        let audit = self.wallet_freshness_audit_with_cycle_points(
            store,
            now,
            1,
            &[RawTruthCyclePoint {
                sample_now: now,
                sample: RawTruthSample {
                    window_start: current_raw.window_start,
                    observed_swaps_loaded: current_raw.observed_swaps_loaded,
                    eligible_wallet_count: current_raw.eligible_wallet_count,
                    top_wallet_ids: current_raw.top_wallet_ids,
                },
            }],
        )?;
        let shadow_signal_started = Instant::now();
        let shadow_signal = self.build_shadow_signal_evidence(
            store,
            now,
            1,
            shadow_evidence_lookback_seconds,
            &audit,
        )?;
        let shadow_signal_duration_ms = shadow_signal_started.elapsed().as_millis() as u64;
        Ok(WalletFreshnessCaptureComputation {
            snapshot: WalletFreshnessCaptureSnapshot {
                capture_id: None,
                captured_at: now,
                capture_age_seconds: None,
                within_recent_horizon: None,
                recent_cycles: 1,
                audit,
                shadow_signal,
            },
            raw_truth_build_duration_ms: 0,
            shadow_signal_duration_ms,
        })
    }

    pub fn wallet_freshness_history_report(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        capture_limit: usize,
    ) -> Result<WalletFreshnessHistoryReport> {
        let recent_horizon_seconds =
            self.default_wallet_freshness_history_recent_horizon_seconds(capture_limit);
        self.wallet_freshness_history_report_with_horizon(
            store,
            now,
            capture_limit,
            recent_horizon_seconds,
        )
    }

    pub fn wallet_freshness_history_report_with_horizon(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        capture_limit: usize,
        recent_horizon_seconds: u64,
    ) -> Result<WalletFreshnessHistoryReport> {
        let capture_limit = capture_limit.max(1);
        let captures = store
            .list_discovery_wallet_freshness_captures(capture_limit)?
            .into_iter()
            .map(wallet_freshness_capture_from_row)
            .collect::<Result<Vec<_>>>()?;
        Ok(summarize_wallet_freshness_history(
            now,
            capture_limit,
            recent_horizon_seconds.max(1),
            captures,
        ))
    }

    pub fn default_wallet_freshness_history_recent_horizon_seconds(
        &self,
        capture_limit: usize,
    ) -> u64 {
        self.config
            .refresh_seconds
            .max(1)
            .saturating_mul(capture_limit.max(1) as u64)
            .saturating_mul(DEFAULT_HISTORY_RECENT_HORIZON_MULTIPLIER)
    }

    fn current_raw_truth_sample(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<RawTruthSample> {
        #[cfg(test)]
        CURRENT_RAW_TRUTH_SAMPLE_CALLS.fetch_add(1, Ordering::Relaxed);
        let window_start = now - Duration::days(self.config.scoring_window_days.max(1) as i64);
        let (snapshots, observed_swaps_loaded) =
            self.build_wallet_snapshots_from_persisted_stream_one_shot(store, window_start, now)?;
        let ranked = rank_follow_candidates(&snapshots, self.config.min_score);
        let top_wallet_ids = desired_wallets(&ranked, self.config.follow_top_n);
        Ok(RawTruthSample {
            window_start,
            observed_swaps_loaded,
            eligible_wallet_count: ranked.len(),
            top_wallet_ids,
        })
    }

    fn collect_raw_truth_cycle_points(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        recent_cycles: usize,
    ) -> Result<Vec<RawTruthCyclePoint>> {
        let cycles_requested = recent_cycles.max(1);
        let sample_interval_seconds = self.config.refresh_seconds.max(1);
        let mut cycle_points = Vec::with_capacity(cycles_requested);
        for idx in 0..cycles_requested {
            let sample_now =
                now - Duration::seconds(sample_interval_seconds.saturating_mul(idx as u64) as i64);
            cycle_points.push(RawTruthCyclePoint {
                sample_now,
                sample: self.current_raw_truth_sample(store, sample_now)?,
            });
        }
        Ok(cycle_points)
    }
}
