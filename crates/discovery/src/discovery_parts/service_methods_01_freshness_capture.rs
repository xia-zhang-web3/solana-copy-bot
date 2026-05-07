use super::*;

impl DiscoveryService {
    pub(crate) fn maybe_persist_in_band_wallet_freshness_capture(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        capture_due: bool,
        runtime_mode: DiscoveryRuntimeMode,
        current_raw: Option<PrecomputedWalletFreshnessCurrentRawTruth>,
    ) -> InBandWalletFreshnessCaptureTelemetry {
        if !capture_due {
            return InBandWalletFreshnessCaptureTelemetry {
                state: "skipped_due_cadence",
                reason: Some("capture_cadence_not_due".to_string()),
                ..InBandWalletFreshnessCaptureTelemetry::default()
            };
        }
        if runtime_mode != DiscoveryRuntimeMode::Healthy {
            return InBandWalletFreshnessCaptureTelemetry {
                state: "skipped_runtime_mode",
                reason: Some(format!(
                    "runtime_mode_{}_does_not_publish_exact_stage3_raw_truth",
                    runtime_mode.as_str()
                )),
                ..InBandWalletFreshnessCaptureTelemetry::default()
            };
        }
        let Some(current_raw) = current_raw else {
            return InBandWalletFreshnessCaptureTelemetry {
                state: "skipped_missing_current_raw_truth",
                reason: Some("exact_current_raw_truth_not_cached_for_capture".to_string()),
                ..InBandWalletFreshnessCaptureTelemetry::default()
            };
        };

        let computed = match self.wallet_freshness_capture_snapshot_from_precomputed_current_raw(
            store,
            now,
            current_raw,
            Some(self.in_band_wallet_freshness_shadow_evidence_lookback_seconds()),
        ) {
            Ok(computed) => computed,
            Err(error) => {
                let error_text = format!("{error:#}");
                warn!(
                    error = %error,
                    "in-band Stage 3 wallet freshness capture build failed; continuing discovery refresh without persisted capture"
                );
                return InBandWalletFreshnessCaptureTelemetry {
                    state: "build_failed",
                    reason: Some(error_text),
                    ..InBandWalletFreshnessCaptureTelemetry::default()
                };
            }
        };

        let write = match computed.snapshot.to_storage_write() {
            Ok(write) => write,
            Err(error) => {
                let error_text = format!("{error:#}");
                warn!(
                    error = %error,
                    "in-band Stage 3 wallet freshness capture serialization failed; continuing discovery refresh without persisted capture"
                );
                return InBandWalletFreshnessCaptureTelemetry {
                    state: "build_failed",
                    reason: Some(error_text),
                    ..InBandWalletFreshnessCaptureTelemetry::default()
                };
            }
        };

        match store.append_discovery_wallet_freshness_capture(&write) {
            Ok(row) => InBandWalletFreshnessCaptureTelemetry {
                state: "persisted",
                reason: None,
                capture_id: Some(row.capture_id),
                captured_at: Some(row.captured_at),
            },
            Err(error) => {
                let error_text = format!("{error:#}");
                warn!(
                    error = %error,
                    "in-band Stage 3 wallet freshness capture persistence failed; continuing discovery refresh without persisted capture"
                );
                InBandWalletFreshnessCaptureTelemetry {
                    state: "persistence_failed",
                    reason: Some(error_text),
                    ..InBandWalletFreshnessCaptureTelemetry::default()
                }
            }
        }
    }

    pub(crate) fn persist_cached_zero_universe_wallet_freshness_capture(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        summary: &DiscoverySummary,
        current_raw: &CachedCurrentRawTruthSample,
    ) -> InBandWalletFreshnessCaptureTelemetry {
        let write = match self.cached_zero_universe_wallet_freshness_capture_write(
            now,
            summary,
            current_raw,
        ) {
            Ok(write) => write,
            Err(error) => {
                let error_text = format!("{error:#}");
                warn!(
                    error = %error,
                    "cheap Stage 3 zero-universe wallet freshness capture build failed; continuing discovery refresh without persisted capture"
                );
                return InBandWalletFreshnessCaptureTelemetry {
                    state: "build_failed",
                    reason: Some(error_text),
                    ..InBandWalletFreshnessCaptureTelemetry::default()
                };
            }
        };

        match store.append_discovery_wallet_freshness_capture(&write) {
            Ok(row) => InBandWalletFreshnessCaptureTelemetry {
                state: "persisted_zero_universe_evidence",
                reason: None,
                capture_id: Some(row.capture_id),
                captured_at: Some(row.captured_at),
            },
            Err(error) => {
                let error_text = format!("{error:#}");
                warn!(
                    error = %error,
                    "cheap Stage 3 zero-universe wallet freshness capture persistence failed; continuing discovery refresh without persisted capture"
                );
                InBandWalletFreshnessCaptureTelemetry {
                    state: "persistence_failed",
                    reason: Some(error_text),
                    ..InBandWalletFreshnessCaptureTelemetry::default()
                }
            }
        }
    }

    pub(crate) fn cached_zero_universe_wallet_freshness_capture_write(
        &self,
        now: DateTime<Utc>,
        summary: &DiscoverySummary,
        current_raw: &CachedCurrentRawTruthSample,
    ) -> Result<DiscoveryWalletFreshnessCaptureWrite> {
        if summary.runtime_mode != DiscoveryRuntimeMode::FailClosed
            || summary.scoring_source != "raw_window"
            || summary.eligible_wallets != 0
            || !summary.top_wallets.is_empty()
            || summary.active_follow_wallets != 0
            || summary.wallets_seen == 0
            || current_raw.observed_swaps_loaded == 0
            || current_raw.eligible_wallet_count != 0
            || !current_raw.top_wallet_ids.is_empty()
        {
            return Err(anyhow!(
                "raw_window_zero_publishable_universe_capture_requires_exact_empty_cached_truth"
            ));
        }

        let empty_comparison = serde_json::json!({
            "left_count": 0,
            "right_count": 0,
            "overlap_count": 0,
            "exact_match": true,
            "only_left": [],
            "only_right": [],
        });
        let raw_truth = serde_json::json!({
            "sufficient": false,
            "reason": RAW_WINDOW_ZERO_PUBLISHABLE_UNIVERSE_REASON,
            "observed_swaps_loaded": current_raw.observed_swaps_loaded,
            "eligible_wallet_count": 0,
            "top_wallet_count": 0,
            "wallets_seen": summary.wallets_seen,
            "short_retention_configured": false,
            "covered_since": null,
            "covered_through_cursor": null,
            "covered_through_lag_seconds": null,
            "tail_fresh_within_runtime_lag": false,
            "runtime_freshness_lag_seconds": self.config.refresh_seconds.max(1),
            "total_observed_swaps_rows": current_raw.observed_swaps_loaded,
        });
        let rotation = serde_json::json!({
            "signal_available": false,
            "reason": "fewer_than_two_raw_truth_cycle_samples",
            "cycles_requested": 1,
            "cycles_completed": 1,
            "sample_interval_seconds": self.config.refresh_seconds.max(1),
            "overlap_with_previous_cycle": null,
            "entered_since_previous_cycle": [],
            "left_since_previous_cycle": [],
            "stable_wallets_across_cycles": [],
            "unique_wallet_count_across_cycles": 0,
            "samples": [{
                "sample_now": now,
                "window_start": current_raw.window_start,
                "observed_swaps_loaded": current_raw.observed_swaps_loaded,
                "eligible_wallet_count": 0,
                "top_wallet_ids": [],
            }],
        });
        let audit = serde_json::json!({
            "now": now,
            "window_start": current_raw.window_start,
            "verdict": "insufficient_raw_truth",
            "reason": RAW_WINDOW_ZERO_PUBLISHABLE_UNIVERSE_REASON,
            "follow_top_n": self.config.follow_top_n as usize,
            "publication_truth_available": false,
            "publication_runtime_mode": "fail_closed",
            "publication_recent_under_gate": false,
            "latest_publication_ts": null,
            "publication_age_seconds": null,
            "latest_publication_window_start": null,
            "published_scoring_source": "raw_window",
            "published_wallet_ids": [],
            "active_follow_wallet_ids": [],
            "current_raw_top_wallet_ids": [],
            "published_vs_current_raw": empty_comparison.clone(),
            "active_follow_vs_current_raw": empty_comparison.clone(),
            "active_follow_vs_published": empty_comparison,
            "raw_truth": raw_truth,
            "rotation": rotation,
        });
        let shadow_signal = serde_json::json!({
            "recent_window_start": now,
            "recent_window_end": now,
            "evidence_lookback_seconds": self.in_band_wallet_freshness_shadow_evidence_lookback_seconds(),
            "selected_wallet_ids": [],
            "selected_wallet_count": 0,
            "selected_wallets_with_recent_raw_activity": 0,
            "selected_wallets_with_recent_shadow_signal": 0,
            "recent_raw_swap_count": 0,
            "recent_shadow_signal_count": 0,
            "recent_raw_activity_wallet_ids": [],
            "recent_shadow_signal_wallet_ids": [],
            "recent_raw_activity_by_wallet": [],
            "recent_shadow_signal_by_wallet": [],
            "raw_activity_top_wallet_share": null,
            "shadow_signal_top_wallet_share": null,
            "raw_activity_broadly_distributed": false,
            "shadow_signal_broadly_distributed": false,
            "verdict": "no_selected_wallets",
            "reason": "no_active_follow_wallets_selected",
        });

        Ok(DiscoveryWalletFreshnessCaptureWrite {
            captured_at: now,
            recent_cycles: 1,
            verdict: "insufficient_raw_truth".to_string(),
            reason: RAW_WINDOW_ZERO_PUBLISHABLE_UNIVERSE_REASON.to_string(),
            publication_age_seconds: None,
            raw_truth_sufficient: false,
            raw_truth_reason: RAW_WINDOW_ZERO_PUBLISHABLE_UNIVERSE_REASON.to_string(),
            shadow_signal_verdict: "no_selected_wallets".to_string(),
            shadow_signal_reason: "no_active_follow_wallets_selected".to_string(),
            published_wallet_ids: Vec::new(),
            active_follow_wallet_ids: Vec::new(),
            current_raw_top_wallet_ids: Vec::new(),
            audit_json: serde_json::to_string(&audit)
                .context("failed serializing cheap zero-universe freshness audit")?,
            shadow_signal_json: serde_json::to_string(&shadow_signal)
                .context("failed serializing cheap zero-universe freshness shadow signal")?,
        })
    }
}
