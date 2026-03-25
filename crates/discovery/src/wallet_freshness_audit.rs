use crate::{
    followlist::{desired_wallets, rank_follow_candidates},
    DiscoveryService, RuntimePublishedUniverseTruth,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_storage::{
    DiscoveryPublicationStateRow, DiscoveryRuntimeCursor, DiscoveryWalletFreshnessCaptureRow,
    DiscoveryWalletFreshnessCaptureWrite, ObservedSwapsCoverageSnapshot, SqliteStore,
    WalletRecentActivityCountRow,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, time::Instant};

pub const DEFAULT_RECENT_CYCLES: usize = 3;
pub const DEFAULT_HISTORY_CAPTURE_LIMIT: usize = 5;
pub const DEFAULT_HISTORY_RECENT_HORIZON_MULTIPLIER: u64 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalletFreshnessVerdict {
    FreshCurrent,
    DriftingButAcceptable,
    StalePublicationTruth,
    InsufficientRawTruth,
    FailClosedNoPublicationTruth,
}

impl WalletFreshnessVerdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FreshCurrent => "fresh_current",
            Self::DriftingButAcceptable => "drifting_but_acceptable",
            Self::StalePublicationTruth => "stale_publication_truth",
            Self::InsufficientRawTruth => "insufficient_raw_truth",
            Self::FailClosedNoPublicationTruth => "fail_closed_no_publication_truth",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletUniverseComparison {
    pub left_count: usize,
    pub right_count: usize,
    pub overlap_count: usize,
    pub exact_match: bool,
    pub only_left: Vec<String>,
    pub only_right: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletFreshnessRawTruthStatus {
    pub sufficient: bool,
    pub reason: String,
    pub observed_swaps_loaded: usize,
    pub eligible_wallet_count: usize,
    pub top_wallet_count: usize,
    pub short_retention_configured: bool,
    pub covered_since: Option<DateTime<Utc>>,
    pub covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    pub covered_through_lag_seconds: Option<u64>,
    pub tail_fresh_within_runtime_lag: bool,
    pub runtime_freshness_lag_seconds: u64,
    pub total_observed_swaps_rows: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletFreshnessRawCycleSample {
    pub sample_now: DateTime<Utc>,
    pub window_start: DateTime<Utc>,
    pub observed_swaps_loaded: usize,
    pub eligible_wallet_count: usize,
    pub top_wallet_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletFreshnessRotationSignal {
    pub signal_available: bool,
    pub reason: Option<String>,
    pub cycles_requested: usize,
    pub cycles_completed: usize,
    pub sample_interval_seconds: u64,
    pub overlap_with_previous_cycle: Option<usize>,
    pub entered_since_previous_cycle: Vec<String>,
    pub left_since_previous_cycle: Vec<String>,
    pub stable_wallets_across_cycles: Vec<String>,
    pub unique_wallet_count_across_cycles: usize,
    pub samples: Vec<WalletFreshnessRawCycleSample>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletFreshnessAuditReport {
    pub now: DateTime<Utc>,
    pub window_start: DateTime<Utc>,
    pub verdict: WalletFreshnessVerdict,
    pub reason: String,
    pub follow_top_n: usize,
    pub publication_truth_available: bool,
    pub publication_runtime_mode: Option<String>,
    pub publication_recent_under_gate: bool,
    pub latest_publication_ts: Option<DateTime<Utc>>,
    pub publication_age_seconds: Option<u64>,
    pub latest_publication_window_start: Option<DateTime<Utc>>,
    pub published_scoring_source: Option<String>,
    pub published_wallet_ids: Vec<String>,
    pub active_follow_wallet_ids: Vec<String>,
    pub current_raw_top_wallet_ids: Vec<String>,
    pub published_vs_current_raw: WalletUniverseComparison,
    pub active_follow_vs_current_raw: WalletUniverseComparison,
    pub active_follow_vs_published: WalletUniverseComparison,
    pub raw_truth: WalletFreshnessRawTruthStatus,
    pub rotation: WalletFreshnessRotationSignal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalletShadowSignalVerdict {
    NoSelectedWallets,
    NoRecentSelectedRawActivity,
    RecentSelectedRawActivityWithoutShadowSignals,
    ShadowSignalsPresentButConcentrated,
    ShadowSignalsPresentAndDistributed,
}

impl WalletShadowSignalVerdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoSelectedWallets => "no_selected_wallets",
            Self::NoRecentSelectedRawActivity => "no_recent_selected_raw_activity",
            Self::RecentSelectedRawActivityWithoutShadowSignals => {
                "recent_selected_raw_activity_without_shadow_signals"
            }
            Self::ShadowSignalsPresentButConcentrated => "shadow_signals_present_but_concentrated",
            Self::ShadowSignalsPresentAndDistributed => "shadow_signals_present_and_distributed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletFreshnessShadowSignalEvidence {
    pub recent_window_start: DateTime<Utc>,
    pub recent_window_end: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evidence_lookback_seconds: Option<u64>,
    pub selected_wallet_ids: Vec<String>,
    pub selected_wallet_count: usize,
    pub selected_wallets_with_recent_raw_activity: usize,
    pub selected_wallets_with_recent_shadow_signal: usize,
    pub recent_raw_swap_count: usize,
    pub recent_shadow_signal_count: usize,
    pub recent_raw_activity_wallet_ids: Vec<String>,
    pub recent_shadow_signal_wallet_ids: Vec<String>,
    pub recent_raw_activity_by_wallet: Vec<WalletRecentActivityCountRow>,
    pub recent_shadow_signal_by_wallet: Vec<WalletRecentActivityCountRow>,
    pub raw_activity_top_wallet_share: Option<f64>,
    pub shadow_signal_top_wallet_share: Option<f64>,
    pub raw_activity_broadly_distributed: bool,
    pub shadow_signal_broadly_distributed: bool,
    pub verdict: WalletShadowSignalVerdict,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletFreshnessCaptureSnapshot {
    pub capture_id: Option<i64>,
    pub captured_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capture_age_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub within_recent_horizon: Option<bool>,
    pub recent_cycles: usize,
    pub audit: WalletFreshnessAuditReport,
    pub shadow_signal: WalletFreshnessShadowSignalEvidence,
}

#[derive(Debug, Clone)]
pub struct WalletFreshnessCaptureComputation {
    pub snapshot: WalletFreshnessCaptureSnapshot,
    pub raw_truth_build_duration_ms: u64,
    pub shadow_signal_duration_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalletFreshnessHistoryVerdict {
    ValidatedCurrent,
    PartiallyValidatedButLowRotation,
    PublicationDrifting,
    InsufficientEvidence,
    RawTruthInsufficient,
}

impl WalletFreshnessHistoryVerdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ValidatedCurrent => "validated_current",
            Self::PartiallyValidatedButLowRotation => "partially_validated_but_low_rotation",
            Self::PublicationDrifting => "publication_drifting",
            Self::InsufficientEvidence => "insufficient_evidence",
            Self::RawTruthInsufficient => "raw_truth_insufficient",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletFreshnessHistoryReport {
    pub generated_at: DateTime<Utc>,
    pub captures_requested: usize,
    pub captures_loaded: usize,
    pub captures_considered: usize,
    pub captures_within_recent_horizon: usize,
    pub recent_horizon_seconds: u64,
    pub latest_capture_age_seconds: Option<u64>,
    pub stale_captures_excluded_from_verdict: bool,
    pub stale_captures_excluded_count: usize,
    pub verdict: WalletFreshnessHistoryVerdict,
    pub reason: String,
    pub fresh_capture_count: usize,
    pub drifting_capture_count: usize,
    pub stale_capture_count: usize,
    pub insufficient_raw_capture_count: usize,
    pub no_publication_truth_capture_count: usize,
    pub exact_published_current_match_count: usize,
    pub exact_active_current_match_count: usize,
    pub active_follow_change_count: usize,
    pub current_raw_change_count: usize,
    pub rotation_evidence_capture_count: usize,
    pub shadow_signal_present_capture_count: usize,
    pub broad_shadow_signal_capture_count: usize,
    pub captures: Vec<WalletFreshnessCaptureSnapshot>,
}

#[derive(Debug, Clone)]
struct RawTruthSample {
    window_start: DateTime<Utc>,
    observed_swaps_loaded: usize,
    eligible_wallet_count: usize,
    top_wallet_ids: Vec<String>,
}

#[derive(Debug, Clone)]
struct RawTruthCyclePoint {
    sample_now: DateTime<Utc>,
    sample: RawTruthSample,
}

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

    fn build_shadow_signal_evidence(
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

    fn classify_raw_truth_status(
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

fn publication_truth_for_audit(
    publication_state: Option<&DiscoveryPublicationStateRow>,
) -> Option<RuntimePublishedUniverseTruth> {
    let publication_state = publication_state?;
    DiscoveryService::runtime_publication_truth_from_state(publication_state.clone())
}

fn compare_wallet_universes(left: &[String], right: &[String]) -> WalletUniverseComparison {
    let left_set: BTreeSet<_> = left.iter().cloned().collect();
    let right_set: BTreeSet<_> = right.iter().cloned().collect();
    WalletUniverseComparison {
        left_count: left_set.len(),
        right_count: right_set.len(),
        overlap_count: left_set.intersection(&right_set).count(),
        exact_match: left_set == right_set,
        only_left: left_set.difference(&right_set).cloned().collect(),
        only_right: right_set.difference(&left_set).cloned().collect(),
    }
}

fn sorted_wallets_from_iter<I>(wallets: I) -> Vec<String>
where
    I: IntoIterator<Item = String>,
{
    let mut wallets: Vec<String> = wallets.into_iter().collect();
    wallets.sort();
    wallets.dedup();
    wallets
}

fn stable_wallets(samples: &[WalletFreshnessRawCycleSample]) -> Vec<String> {
    let mut intersection = samples
        .first()
        .map(|sample| {
            sample
                .top_wallet_ids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    for sample in &samples[1..] {
        let sample_set: BTreeSet<_> = sample.top_wallet_ids.iter().cloned().collect();
        intersection = intersection
            .intersection(&sample_set)
            .cloned()
            .collect::<BTreeSet<_>>();
    }
    intersection.into_iter().collect()
}

fn compare_wallet_recent_activity_rows(
    left: &WalletRecentActivityCountRow,
    right: &WalletRecentActivityCountRow,
) -> std::cmp::Ordering {
    right
        .row_count
        .cmp(&left.row_count)
        .then_with(|| right.latest_ts.cmp(&left.latest_ts))
        .then_with(|| left.wallet_id.cmp(&right.wallet_id))
}

fn dominant_wallet_share(
    counts: &[WalletRecentActivityCountRow],
    total_count: usize,
) -> Option<f64> {
    if counts.is_empty() || total_count == 0 {
        return None;
    }
    let dominant = counts.iter().map(|row| row.row_count).max().unwrap_or(0);
    Some(dominant as f64 / total_count as f64)
}

fn activity_broadly_distributed(
    selected_wallet_count: usize,
    active_wallet_count: usize,
    dominant_share: Option<f64>,
) -> bool {
    if active_wallet_count == 0 {
        return false;
    }
    if selected_wallet_count <= 1 {
        return active_wallet_count == 1;
    }
    active_wallet_count >= selected_wallet_count.min(2)
        && dominant_share.is_some_and(|share| share <= 0.80)
}

impl WalletFreshnessCaptureSnapshot {
    pub fn to_storage_write(&self) -> Result<DiscoveryWalletFreshnessCaptureWrite> {
        Ok(DiscoveryWalletFreshnessCaptureWrite {
            captured_at: self.captured_at,
            recent_cycles: self.recent_cycles,
            verdict: self.audit.verdict.as_str().to_string(),
            reason: self.audit.reason.clone(),
            publication_age_seconds: self.audit.publication_age_seconds,
            raw_truth_sufficient: self.audit.raw_truth.sufficient,
            raw_truth_reason: self.audit.raw_truth.reason.clone(),
            shadow_signal_verdict: self.shadow_signal.verdict.as_str().to_string(),
            shadow_signal_reason: self.shadow_signal.reason.clone(),
            published_wallet_ids: self.audit.published_wallet_ids.clone(),
            active_follow_wallet_ids: self.audit.active_follow_wallet_ids.clone(),
            current_raw_top_wallet_ids: self.audit.current_raw_top_wallet_ids.clone(),
            audit_json: serde_json::to_string(&self.audit)
                .context("failed serializing wallet freshness audit report")?,
            shadow_signal_json: serde_json::to_string(&self.shadow_signal)
                .context("failed serializing wallet freshness shadow evidence")?,
        })
    }
}

pub fn wallet_freshness_capture_from_row(
    row: DiscoveryWalletFreshnessCaptureRow,
) -> Result<WalletFreshnessCaptureSnapshot> {
    let audit: WalletFreshnessAuditReport = serde_json::from_str(&row.audit_json)
        .context("failed deserializing persisted wallet freshness audit json")?;
    let shadow_signal: WalletFreshnessShadowSignalEvidence =
        serde_json::from_str(&row.shadow_signal_json)
            .context("failed deserializing persisted wallet freshness shadow evidence json")?;
    Ok(WalletFreshnessCaptureSnapshot {
        capture_id: Some(row.capture_id),
        captured_at: row.captured_at,
        capture_age_seconds: None,
        within_recent_horizon: None,
        recent_cycles: row.recent_cycles,
        audit,
        shadow_signal,
    })
}

fn capture_has_rotation_evidence(capture: &WalletFreshnessCaptureSnapshot) -> bool {
    capture.audit.rotation.signal_available
        && (!capture
            .audit
            .rotation
            .entered_since_previous_cycle
            .is_empty()
            || !capture.audit.rotation.left_since_previous_cycle.is_empty()
            || capture.audit.rotation.unique_wallet_count_across_cycles
                > capture.audit.current_raw_top_wallet_ids.len())
}

fn shadow_signal_present(capture: &WalletFreshnessCaptureSnapshot) -> bool {
    matches!(
        capture.shadow_signal.verdict,
        WalletShadowSignalVerdict::ShadowSignalsPresentButConcentrated
            | WalletShadowSignalVerdict::ShadowSignalsPresentAndDistributed
    )
}

fn summarize_wallet_freshness_history(
    generated_at: DateTime<Utc>,
    captures_requested: usize,
    recent_horizon_seconds: u64,
    captures: Vec<WalletFreshnessCaptureSnapshot>,
) -> WalletFreshnessHistoryReport {
    let captures_loaded = captures.len();
    let latest_capture_age_seconds = captures.first().map(|capture| {
        generated_at
            .signed_duration_since(capture.captured_at)
            .num_seconds()
            .max(0) as u64
    });
    let annotated_captures = captures
        .into_iter()
        .map(|mut capture| {
            let capture_age_seconds = generated_at
                .signed_duration_since(capture.captured_at)
                .num_seconds()
                .max(0) as u64;
            let within_recent_horizon = capture_age_seconds <= recent_horizon_seconds;
            capture.capture_age_seconds = Some(capture_age_seconds);
            capture.within_recent_horizon = Some(within_recent_horizon);
            capture
        })
        .collect::<Vec<_>>();
    let captures = annotated_captures
        .iter()
        .filter(|capture| capture.within_recent_horizon == Some(true))
        .cloned()
        .collect::<Vec<_>>();
    let captures_within_recent_horizon = captures.len();
    let stale_captures_excluded_count =
        captures_loaded.saturating_sub(captures_within_recent_horizon);

    let mut fresh_capture_count = 0usize;
    let mut drifting_capture_count = 0usize;
    let mut stale_capture_count = 0usize;
    let mut insufficient_raw_capture_count = 0usize;
    let mut no_publication_truth_capture_count = 0usize;
    let mut exact_published_current_match_count = 0usize;
    let mut exact_active_current_match_count = 0usize;
    let mut rotation_evidence_capture_count = 0usize;
    let mut shadow_signal_present_capture_count = 0usize;
    let mut broad_shadow_signal_capture_count = 0usize;
    let mut active_follow_change_count = 0usize;
    let mut current_raw_change_count = 0usize;

    for (index, capture) in captures.iter().enumerate() {
        match capture.audit.verdict {
            WalletFreshnessVerdict::FreshCurrent => fresh_capture_count += 1,
            WalletFreshnessVerdict::DriftingButAcceptable => drifting_capture_count += 1,
            WalletFreshnessVerdict::StalePublicationTruth => stale_capture_count += 1,
            WalletFreshnessVerdict::InsufficientRawTruth => insufficient_raw_capture_count += 1,
            WalletFreshnessVerdict::FailClosedNoPublicationTruth => {
                no_publication_truth_capture_count += 1;
            }
        }
        if capture.audit.published_vs_current_raw.exact_match {
            exact_published_current_match_count += 1;
        }
        if capture.audit.active_follow_vs_current_raw.exact_match {
            exact_active_current_match_count += 1;
        }
        if capture_has_rotation_evidence(capture) {
            rotation_evidence_capture_count += 1;
        }
        if shadow_signal_present(capture) {
            shadow_signal_present_capture_count += 1;
        }
        if capture.shadow_signal.shadow_signal_broadly_distributed {
            broad_shadow_signal_capture_count += 1;
        }

        if index > 0 {
            let previous = &captures[index - 1];
            if !compare_wallet_universes(
                &capture.audit.active_follow_wallet_ids,
                &previous.audit.active_follow_wallet_ids,
            )
            .exact_match
            {
                active_follow_change_count += 1;
            }
            if !compare_wallet_universes(
                &capture.audit.current_raw_top_wallet_ids,
                &previous.audit.current_raw_top_wallet_ids,
            )
            .exact_match
            {
                current_raw_change_count += 1;
            }
        }
    }

    let captures_considered = captures.len();
    let (verdict, reason) = if captures.is_empty() {
        (
            WalletFreshnessHistoryVerdict::InsufficientEvidence,
            if captures_loaded == 0 {
                "no_persisted_wallet_freshness_captures".to_string()
            } else {
                "no_recent_wallet_freshness_captures_within_horizon".to_string()
            },
        )
    } else if insufficient_raw_capture_count > 0 {
        (
            WalletFreshnessHistoryVerdict::RawTruthInsufficient,
            "recent_captures_include_insufficient_raw_truth".to_string(),
        )
    } else if no_publication_truth_capture_count > 0 {
        (
            WalletFreshnessHistoryVerdict::InsufficientEvidence,
            "recent_captures_include_missing_publication_truth".to_string(),
        )
    } else if stale_capture_count > 0 || drifting_capture_count > 0 {
        (
            WalletFreshnessHistoryVerdict::PublicationDrifting,
            "recent_captures_show_publication_drift_from_current_raw_truth".to_string(),
        )
    } else if captures_considered < 3 {
        (
            WalletFreshnessHistoryVerdict::InsufficientEvidence,
            "fewer_than_three_recent_wallet_freshness_captures".to_string(),
        )
    } else if shadow_signal_present_capture_count == 0 {
        (
            WalletFreshnessHistoryVerdict::InsufficientEvidence,
            "no_recent_shadow_signal_evidence_from_selected_wallets".to_string(),
        )
    } else if active_follow_change_count == 0
        && current_raw_change_count == 0
        && rotation_evidence_capture_count == 0
    {
        (
            WalletFreshnessHistoryVerdict::PartiallyValidatedButLowRotation,
            "recent_captures_remain_fresh_but_show_low_rotation".to_string(),
        )
    } else {
        (
            WalletFreshnessHistoryVerdict::ValidatedCurrent,
            "recent_captures_show_current_wallet_selection_and_shadow_signal_evidence".to_string(),
        )
    };

    WalletFreshnessHistoryReport {
        generated_at,
        captures_requested,
        captures_loaded,
        captures_considered,
        captures_within_recent_horizon,
        recent_horizon_seconds,
        latest_capture_age_seconds,
        stale_captures_excluded_from_verdict: stale_captures_excluded_count > 0,
        stale_captures_excluded_count,
        verdict,
        reason,
        fresh_capture_count,
        drifting_capture_count,
        stale_capture_count,
        insufficient_raw_capture_count,
        no_publication_truth_capture_count,
        exact_published_current_match_count,
        exact_active_current_match_count,
        active_follow_change_count,
        current_raw_change_count,
        rotation_evidence_capture_count,
        shadow_signal_present_capture_count,
        broad_shadow_signal_capture_count,
        captures: annotated_captures,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        compare_wallet_universes, WalletFreshnessAuditReport, WalletFreshnessCaptureSnapshot,
        WalletFreshnessHistoryVerdict, WalletFreshnessRawCycleSample,
        WalletFreshnessRawTruthStatus, WalletFreshnessRotationSignal,
        WalletFreshnessShadowSignalEvidence, WalletFreshnessVerdict, WalletShadowSignalVerdict,
        WalletUniverseComparison, DEFAULT_RECENT_CYCLES,
    };
    use anyhow::{Context, Result};
    use chrono::{DateTime, Duration, Utc};
    use copybot_config::{DiscoveryConfig, ShadowConfig};
    use copybot_core_types::{CopySignalRow, SwapEvent, COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE};
    use copybot_storage::{
        DiscoveryPublicationStateUpdate, DiscoveryRuntimeCursor, DiscoveryRuntimeMode, SqliteStore,
        WalletRecentActivityCountRow,
    };
    use std::path::Path;
    use tempfile::tempdir;

    use crate::DiscoveryService;

    #[test]
    fn fresh_exact_published_universe_matches_current_raw_truth() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-freshness-fresh.db");
        let store = open_store(&db_path)?;
        let now = ts("2026-03-25T12:00:00Z");
        let config = freshness_test_config();
        seed_ranked_wallet_window(
            &store,
            now,
            &[
                ("wallet-alpha", "mint-a", 4, 0),
                ("wallet-beta", "mint-b", 3, 10),
                ("wallet-gamma", "mint-c", 1, 20),
            ],
        )?;
        seed_publication_truth(
            &store,
            &config,
            now - Duration::seconds(60),
            now,
            DiscoveryRuntimeMode::Healthy,
            &["wallet-alpha", "wallet-beta"],
        )?;
        store.activate_follow_wallet("wallet-alpha", now, "test-follow")?;
        store.activate_follow_wallet("wallet-beta", now, "test-follow")?;

        let discovery = DiscoveryService::new(config, ShadowConfig::default());
        let report = discovery.wallet_freshness_audit(&store, now, DEFAULT_RECENT_CYCLES)?;

        assert_eq!(report.verdict, WalletFreshnessVerdict::FreshCurrent);
        assert!(report.raw_truth.sufficient);
        assert_eq!(
            report.current_raw_top_wallet_ids,
            vec!["wallet-alpha".to_string(), "wallet-beta".to_string()]
        );
        assert!(report.published_vs_current_raw.exact_match);
        assert!(report.active_follow_vs_published.exact_match);
        Ok(())
    }

    #[test]
    fn stale_published_universe_with_meaningful_drift_is_detected() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-freshness-stale.db");
        let store = open_store(&db_path)?;
        let now = ts("2026-03-25T12:00:00Z");
        let config = freshness_test_config();
        seed_ranked_wallet_window(
            &store,
            now,
            &[
                ("wallet-alpha", "mint-a", 4, 0),
                ("wallet-beta", "mint-b", 3, 10),
                ("wallet-gamma", "mint-c", 1, 20),
            ],
        )?;
        seed_publication_truth(
            &store,
            &config,
            now - Duration::hours(3),
            now - Duration::hours(3),
            DiscoveryRuntimeMode::BootstrapDegraded,
            &["wallet-beta", "wallet-legacy"],
        )?;
        store.activate_follow_wallet("wallet-beta", now, "test-follow")?;
        store.activate_follow_wallet("wallet-legacy", now, "test-follow")?;

        let discovery = DiscoveryService::new(config, ShadowConfig::default());
        let report = discovery.wallet_freshness_audit(&store, now, DEFAULT_RECENT_CYCLES)?;

        assert_eq!(
            report.verdict,
            WalletFreshnessVerdict::StalePublicationTruth
        );
        assert!(report.raw_truth.sufficient);
        assert_eq!(report.reason, "publication_truth_not_recent_under_gate");
        assert_eq!(
            report.published_vs_current_raw.only_left,
            vec!["wallet-legacy".to_string()]
        );
        assert_eq!(
            report.published_vs_current_raw.only_right,
            vec!["wallet-alpha".to_string()]
        );
        Ok(())
    }

    #[test]
    fn fail_closed_publication_state_with_preserved_exact_set_is_audited_as_truth() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("wallet-freshness-fail-closed-preserved.db");
        let store = open_store(&db_path)?;
        let now = ts("2026-03-25T12:00:00Z");
        let config = freshness_test_config();
        seed_ranked_wallet_window(
            &store,
            now,
            &[
                ("wallet-alpha", "mint-a", 4, 0),
                ("wallet-beta", "mint-b", 3, 10),
            ],
        )?;
        seed_publication_truth(
            &store,
            &config,
            now - Duration::seconds(60),
            now,
            DiscoveryRuntimeMode::FailClosed,
            &["wallet-alpha", "wallet-beta"],
        )?;
        store.activate_follow_wallet("wallet-alpha", now, "test-follow")?;
        store.activate_follow_wallet("wallet-beta", now, "test-follow")?;

        let discovery = DiscoveryService::new(config, ShadowConfig::default());
        let report = discovery.wallet_freshness_audit(&store, now, DEFAULT_RECENT_CYCLES)?;

        assert_eq!(report.verdict, WalletFreshnessVerdict::FreshCurrent);
        assert!(report.publication_truth_available);
        assert_eq!(
            report.publication_runtime_mode.as_deref(),
            Some("fail_closed")
        );
        Ok(())
    }

    #[test]
    fn no_publication_truth_returns_fail_closed_verdict() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-freshness-no-publication.db");
        let store = open_store(&db_path)?;
        let now = ts("2026-03-25T12:00:00Z");
        let config = freshness_test_config();
        seed_ranked_wallet_window(&store, now, &[("wallet-alpha", "mint-a", 3, 0)])?;

        let discovery = DiscoveryService::new(config, ShadowConfig::default());
        let report = discovery.wallet_freshness_audit(&store, now, DEFAULT_RECENT_CYCLES)?;

        assert_eq!(
            report.verdict,
            WalletFreshnessVerdict::FailClosedNoPublicationTruth
        );
        assert_eq!(report.reason, "no_complete_publication_truth");
        Ok(())
    }

    #[test]
    fn insufficient_recent_raw_truth_is_reported_explicitly() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-freshness-insufficient.db");
        let store = open_store(&db_path)?;
        let now = ts("2026-03-25T12:00:00Z");
        let config = freshness_test_config();
        seed_recent_only_ranked_wallet_window(&store, now, &[("wallet-alpha", "mint-a", 3, 5)])?;
        seed_publication_truth(
            &store,
            &config,
            now - Duration::seconds(60),
            now,
            DiscoveryRuntimeMode::Healthy,
            &["wallet-alpha"],
        )?;
        store.activate_follow_wallet("wallet-alpha", now, "test-follow")?;

        let discovery = DiscoveryService::new(config, ShadowConfig::default());
        let report = discovery.wallet_freshness_audit(&store, now, DEFAULT_RECENT_CYCLES)?;

        assert_eq!(report.verdict, WalletFreshnessVerdict::InsufficientRawTruth);
        assert_eq!(
            report.raw_truth.reason,
            "observed_swaps_coverage_starts_after_scoring_window_start"
        );
        assert!(!report.raw_truth.sufficient);
        assert!(report.raw_truth.tail_fresh_within_runtime_lag);
        Ok(())
    }

    #[test]
    fn stale_raw_tail_freshness_is_reported_as_insufficient_raw_truth() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-freshness-stale-tail.db");
        let store = open_store(&db_path)?;
        let now = ts("2026-03-25T12:00:00Z");
        let config = freshness_test_config();
        let refresh_seconds = config.refresh_seconds;
        seed_ranked_wallet_window(
            &store,
            now - Duration::minutes(25),
            &[
                ("wallet-alpha", "mint-a", 4, 0),
                ("wallet-beta", "mint-b", 3, 10),
            ],
        )?;
        seed_publication_truth(
            &store,
            &config,
            now - Duration::seconds(60),
            now,
            DiscoveryRuntimeMode::Healthy,
            &["wallet-alpha", "wallet-beta"],
        )?;
        store.activate_follow_wallet("wallet-alpha", now, "test-follow")?;
        store.activate_follow_wallet("wallet-beta", now, "test-follow")?;

        let discovery = DiscoveryService::new(config, ShadowConfig::default());
        let report = discovery.wallet_freshness_audit(&store, now, DEFAULT_RECENT_CYCLES)?;

        assert_eq!(report.verdict, WalletFreshnessVerdict::InsufficientRawTruth);
        assert_eq!(
            report.raw_truth.reason,
            "observed_swaps_coverage_ends_before_freshness_gate"
        );
        assert!(!report.raw_truth.tail_fresh_within_runtime_lag);
        assert!(report
            .raw_truth
            .covered_through_lag_seconds
            .is_some_and(|lag| lag > refresh_seconds));
        Ok(())
    }

    #[test]
    fn active_follow_residue_does_not_masquerade_as_fresh_publication_truth() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-freshness-active-residue.db");
        let store = open_store(&db_path)?;
        let now = ts("2026-03-25T12:00:00Z");
        let config = freshness_test_config();
        seed_ranked_wallet_window(
            &store,
            now,
            &[
                ("wallet-alpha", "mint-a", 3, 0),
                ("wallet-beta", "mint-b", 2, 10),
            ],
        )?;
        store.activate_follow_wallet("wallet-alpha", now, "test-follow")?;
        store.activate_follow_wallet("wallet-beta", now, "test-follow")?;

        let discovery = DiscoveryService::new(config, ShadowConfig::default());
        let report = discovery.wallet_freshness_audit(&store, now, DEFAULT_RECENT_CYCLES)?;

        assert_eq!(
            report.verdict,
            WalletFreshnessVerdict::FailClosedNoPublicationTruth
        );
        assert!(report.active_follow_vs_current_raw.exact_match);
        assert!(!report.publication_truth_available);
        Ok(())
    }

    #[test]
    fn fail_closed_preserved_set_with_raw_drift_is_not_reported_as_missing_publication_truth(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-freshness-fail-closed-drift.db");
        let store = open_store(&db_path)?;
        let now = ts("2026-03-25T12:00:00Z");
        let config = freshness_test_config();
        seed_ranked_wallet_window(
            &store,
            now,
            &[
                ("wallet-alpha", "mint-a", 4, 0),
                ("wallet-beta", "mint-b", 3, 10),
                ("wallet-gamma", "mint-c", 1, 20),
            ],
        )?;
        seed_publication_truth(
            &store,
            &config,
            now - Duration::hours(3),
            now - Duration::hours(3),
            DiscoveryRuntimeMode::FailClosed,
            &["wallet-beta", "wallet-legacy"],
        )?;
        store.activate_follow_wallet("wallet-beta", now, "test-follow")?;
        store.activate_follow_wallet("wallet-legacy", now, "test-follow")?;

        let discovery = DiscoveryService::new(config, ShadowConfig::default());
        let report = discovery.wallet_freshness_audit(&store, now, DEFAULT_RECENT_CYCLES)?;

        assert_eq!(
            report.verdict,
            WalletFreshnessVerdict::StalePublicationTruth
        );
        assert!(report.publication_truth_available);
        assert_eq!(
            report.publication_runtime_mode.as_deref(),
            Some("fail_closed")
        );
        assert_ne!(report.reason, "no_complete_publication_truth");
        Ok(())
    }

    #[test]
    fn aggregate_recovery_state_does_not_affect_verdict() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-freshness-aggregate-ignored.db");
        let store = open_store(&db_path)?;
        let now = ts("2026-03-25T12:00:00Z");
        let config = freshness_test_config();
        let window_start = now - Duration::days(config.scoring_window_days as i64);
        seed_ranked_wallet_window(
            &store,
            now,
            &[
                ("wallet-alpha", "mint-a", 3, 0),
                ("wallet-beta", "mint-b", 2, 10),
            ],
        )?;
        seed_publication_truth(
            &store,
            &config,
            now - Duration::seconds(60),
            now,
            DiscoveryRuntimeMode::Healthy,
            &["wallet-alpha", "wallet-beta"],
        )?;
        store.activate_follow_wallet("wallet-alpha", now, "test-follow")?;
        store.activate_follow_wallet("wallet-beta", now, "test-follow")?;
        store.set_discovery_scoring_covered_since(window_start + Duration::hours(12))?;
        store.set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
            ts_utc: now,
            slot: 99,
            signature: "aggregate-sig".to_string(),
        })?;
        store.set_discovery_scoring_materialization_gap_cursor(&DiscoveryRuntimeCursor {
            ts_utc: now - Duration::hours(1),
            slot: 88,
            signature: "aggregate-gap".to_string(),
        })?;

        let discovery = DiscoveryService::new(config, ShadowConfig::default());
        let report = discovery.wallet_freshness_audit(&store, now, DEFAULT_RECENT_CYCLES)?;

        assert_eq!(report.verdict, WalletFreshnessVerdict::FreshCurrent);
        Ok(())
    }

    #[test]
    fn capture_snapshot_includes_shadow_signal_evidence_for_selected_wallets() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-freshness-shadow-present.db");
        let store = open_store(&db_path)?;
        let now = ts("2026-03-25T12:00:00Z");
        let config = freshness_test_config();
        seed_ranked_wallet_window(
            &store,
            now,
            &[
                ("wallet-alpha", "mint-a", 4, 0),
                ("wallet-beta", "mint-b", 3, 10),
            ],
        )?;
        seed_publication_truth(
            &store,
            &config,
            now - Duration::seconds(60),
            now,
            DiscoveryRuntimeMode::Healthy,
            &["wallet-alpha", "wallet-beta"],
        )?;
        store.activate_follow_wallet("wallet-alpha", now, "test-follow")?;
        store.activate_follow_wallet("wallet-beta", now, "test-follow")?;
        store.insert_copy_signal(&CopySignalRow {
            signal_id: "shadow:sig:wallet-alpha".to_string(),
            wallet_id: "wallet-alpha".to_string(),
            side: "buy".to_string(),
            token: "mint-a".to_string(),
            notional_sol: 0.2,
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
            ts: now - Duration::seconds(45),
            status: "shadow_recorded".to_string(),
        })?;

        let discovery = DiscoveryService::new(config, ShadowConfig::default());
        let capture =
            discovery.wallet_freshness_capture_snapshot(&store, now, DEFAULT_RECENT_CYCLES)?;

        assert_eq!(
            capture.shadow_signal.verdict,
            WalletShadowSignalVerdict::ShadowSignalsPresentButConcentrated
        );
        assert_eq!(capture.shadow_signal.selected_wallet_count, 2);
        assert_eq!(
            capture
                .shadow_signal
                .selected_wallets_with_recent_raw_activity,
            2
        );
        assert_eq!(
            capture
                .shadow_signal
                .selected_wallets_with_recent_shadow_signal,
            1
        );
        assert_eq!(
            capture.shadow_signal.recent_shadow_signal_wallet_ids,
            vec!["wallet-alpha".to_string()]
        );
        Ok(())
    }

    #[test]
    fn capture_snapshot_reports_missing_shadow_signal_evidence() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-freshness-shadow-missing.db");
        let store = open_store(&db_path)?;
        let now = ts("2026-03-25T12:00:00Z");
        let config = freshness_test_config();
        seed_ranked_wallet_window(
            &store,
            now,
            &[
                ("wallet-alpha", "mint-a", 4, 0),
                ("wallet-beta", "mint-b", 3, 10),
            ],
        )?;
        seed_publication_truth(
            &store,
            &config,
            now - Duration::seconds(60),
            now,
            DiscoveryRuntimeMode::Healthy,
            &["wallet-alpha", "wallet-beta"],
        )?;
        store.activate_follow_wallet("wallet-alpha", now, "test-follow")?;
        store.activate_follow_wallet("wallet-beta", now, "test-follow")?;

        let discovery = DiscoveryService::new(config, ShadowConfig::default());
        let capture =
            discovery.wallet_freshness_capture_snapshot(&store, now, DEFAULT_RECENT_CYCLES)?;

        assert_eq!(
            capture.shadow_signal.verdict,
            WalletShadowSignalVerdict::RecentSelectedRawActivityWithoutShadowSignals
        );
        assert_eq!(
            capture
                .shadow_signal
                .selected_wallets_with_recent_raw_activity,
            2
        );
        assert_eq!(
            capture
                .shadow_signal
                .selected_wallets_with_recent_shadow_signal,
            0
        );
        Ok(())
    }

    #[test]
    fn measured_capture_snapshot_keeps_exact_stage_three_semantics_for_scheduled_mode() -> Result<()>
    {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-freshness-scheduled-capture.db");
        let store = open_store(&db_path)?;
        let now = ts("2026-03-25T12:00:00Z");
        let config = freshness_test_config();
        seed_ranked_wallet_window(
            &store,
            now,
            &[
                ("wallet-alpha", "mint-a", 4, 0),
                ("wallet-beta", "mint-b", 3, 10),
            ],
        )?;
        seed_publication_truth(
            &store,
            &config,
            now - Duration::seconds(60),
            now,
            DiscoveryRuntimeMode::Healthy,
            &["wallet-alpha", "wallet-beta"],
        )?;
        store.activate_follow_wallet("wallet-alpha", now, "test-follow")?;
        store.activate_follow_wallet("wallet-beta", now, "test-follow")?;
        store.insert_copy_signal(&CopySignalRow {
            signal_id: "shadow:sig:wallet-alpha".to_string(),
            wallet_id: "wallet-alpha".to_string(),
            side: "buy".to_string(),
            token: "mint-a".to_string(),
            notional_sol: 0.2,
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
            ts: now - Duration::seconds(45),
            status: "shadow_recorded".to_string(),
        })?;

        let discovery = DiscoveryService::new(config, ShadowConfig::default());
        let capture = discovery.wallet_freshness_capture_snapshot_measured_with_lookback(
            &store,
            now,
            1,
            Some(960),
        )?;

        assert_eq!(
            capture.snapshot.audit.verdict,
            WalletFreshnessVerdict::FreshCurrent
        );
        assert_eq!(
            capture.snapshot.shadow_signal.verdict,
            WalletShadowSignalVerdict::ShadowSignalsPresentButConcentrated
        );
        assert_eq!(capture.snapshot.recent_cycles, 1);
        assert!(!capture.snapshot.audit.rotation.signal_available);
        assert_eq!(
            capture.snapshot.shadow_signal.evidence_lookback_seconds,
            Some(960)
        );
        assert_eq!(capture.snapshot.shadow_signal.selected_wallet_count, 2);
        assert_eq!(
            capture
                .snapshot
                .shadow_signal
                .selected_wallets_with_recent_shadow_signal,
            1
        );
        Ok(())
    }

    #[test]
    fn explicit_shadow_evidence_lookback_covers_scheduled_timer_gap_without_blind_spot(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-freshness-scheduled-gap-free.db");
        let store = open_store(&db_path)?;
        let now = ts("2026-03-25T12:15:00Z");
        let config = freshness_test_config();
        seed_ranked_wallet_window(
            &store,
            now,
            &[
                ("wallet-alpha", "mint-a", 4, 0),
                ("wallet-beta", "mint-b", 3, 10),
            ],
        )?;
        seed_publication_truth(
            &store,
            &config,
            now - Duration::seconds(60),
            now,
            DiscoveryRuntimeMode::Healthy,
            &["wallet-alpha", "wallet-beta"],
        )?;
        store.activate_follow_wallet("wallet-alpha", now, "test-follow")?;
        store.activate_follow_wallet("wallet-beta", now, "test-follow")?;
        store.insert_copy_signal(&CopySignalRow {
            signal_id: "shadow:sig:wallet-alpha-gap".to_string(),
            wallet_id: "wallet-alpha".to_string(),
            side: "buy".to_string(),
            token: "mint-a".to_string(),
            notional_sol: 0.2,
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
            // This lands in the 5 minute blind interval that existed between
            // a 15 minute timer cadence and the old 10 minute evidence window.
            ts: now - Duration::minutes(11),
            status: "shadow_recorded".to_string(),
        })?;

        let discovery = DiscoveryService::new(config, ShadowConfig::default());
        let capture = discovery.wallet_freshness_capture_snapshot_measured_with_lookback(
            &store,
            now,
            1,
            Some(960),
        )?;

        assert_eq!(
            capture.snapshot.shadow_signal.evidence_lookback_seconds,
            Some(960)
        );
        assert_eq!(
            capture.snapshot.shadow_signal.recent_window_start,
            now - Duration::seconds(960)
        );
        assert_eq!(
            capture
                .snapshot
                .shadow_signal
                .selected_wallets_with_recent_shadow_signal,
            1
        );
        assert_eq!(
            capture
                .snapshot
                .shadow_signal
                .recent_shadow_signal_wallet_ids,
            vec!["wallet-alpha".to_string()]
        );
        Ok(())
    }

    #[test]
    fn persisted_capture_with_gap_free_lookback_remains_visible_to_history_report() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("wallet-freshness-scheduled-gap-history.db");
        let store = open_store(&db_path)?;
        let now = ts("2026-03-25T12:15:00Z");
        let config = freshness_test_config();
        seed_ranked_wallet_window(
            &store,
            now,
            &[
                ("wallet-alpha", "mint-a", 4, 0),
                ("wallet-beta", "mint-b", 3, 10),
            ],
        )?;
        seed_publication_truth(
            &store,
            &config,
            now - Duration::seconds(60),
            now,
            DiscoveryRuntimeMode::Healthy,
            &["wallet-alpha", "wallet-beta"],
        )?;
        store.activate_follow_wallet("wallet-alpha", now, "test-follow")?;
        store.activate_follow_wallet("wallet-beta", now, "test-follow")?;
        store.insert_copy_signal(&CopySignalRow {
            signal_id: "shadow:sig:wallet-alpha-gap-history".to_string(),
            wallet_id: "wallet-alpha".to_string(),
            side: "buy".to_string(),
            token: "mint-a".to_string(),
            notional_sol: 0.2,
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
            ts: now - Duration::minutes(11),
            status: "shadow_recorded".to_string(),
        })?;

        let discovery = DiscoveryService::new(config, ShadowConfig::default());
        let capture = discovery.wallet_freshness_capture_snapshot_measured_with_lookback(
            &store,
            now,
            1,
            Some(960),
        )?;
        persist_capture(&store, capture.snapshot)?;

        let report =
            discovery.wallet_freshness_history_report_with_horizon(&store, now, 5, 3_600)?;

        assert_eq!(report.captures_loaded, 1);
        assert_eq!(report.shadow_signal_present_capture_count, 1);
        assert_eq!(
            report.captures[0].shadow_signal.evidence_lookback_seconds,
            Some(960)
        );
        assert_eq!(
            report.captures[0]
                .shadow_signal
                .recent_shadow_signal_wallet_ids,
            vec!["wallet-alpha".to_string()]
        );
        Ok(())
    }

    #[test]
    fn history_report_validates_current_selection_across_multiple_captures() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-freshness-history-validated.db");
        let store = open_store(&db_path)?;
        let discovery = DiscoveryService::new(freshness_test_config(), ShadowConfig::default());
        persist_capture(
            &store,
            sample_capture_snapshot(
                ts("2026-03-25T12:00:00Z"),
                WalletFreshnessVerdict::FreshCurrent,
                WalletShadowSignalVerdict::ShadowSignalsPresentAndDistributed,
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha", "wallet-gamma"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-beta"],
                &["wallet-gamma"],
            ),
        )?;
        persist_capture(
            &store,
            sample_capture_snapshot(
                ts("2026-03-25T11:50:00Z"),
                WalletFreshnessVerdict::FreshCurrent,
                WalletShadowSignalVerdict::ShadowSignalsPresentButConcentrated,
                &["wallet-alpha", "wallet-gamma"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha", "wallet-gamma"],
                &["wallet-alpha", "wallet-gamma"],
                &["wallet-alpha", "wallet-gamma"],
                &["wallet-gamma"],
                &["wallet-beta"],
            ),
        )?;
        persist_capture(
            &store,
            sample_capture_snapshot(
                ts("2026-03-25T11:40:00Z"),
                WalletFreshnessVerdict::FreshCurrent,
                WalletShadowSignalVerdict::ShadowSignalsPresentAndDistributed,
                &["wallet-beta", "wallet-gamma"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-beta", "wallet-gamma"],
                &["wallet-beta", "wallet-gamma"],
                &["wallet-beta", "wallet-gamma"],
                &["wallet-alpha"],
                &["wallet-beta"],
            ),
        )?;

        let report =
            discovery.wallet_freshness_history_report(&store, ts("2026-03-25T12:05:00Z"), 5)?;

        assert_eq!(
            report.verdict,
            WalletFreshnessHistoryVerdict::ValidatedCurrent
        );
        assert_eq!(report.captures_loaded, 3);
        assert_eq!(report.captures_considered, 3);
        assert_eq!(report.captures_within_recent_horizon, 3);
        assert!(!report.stale_captures_excluded_from_verdict);
        assert_eq!(report.fresh_capture_count, 3);
        assert!(report.active_follow_change_count > 0);
        assert!(report.current_raw_change_count > 0);
        assert!(report.shadow_signal_present_capture_count >= 1);
        Ok(())
    }

    #[test]
    fn history_report_detects_publication_drift_across_recent_captures() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-freshness-history-drift.db");
        let store = open_store(&db_path)?;
        let discovery = DiscoveryService::new(freshness_test_config(), ShadowConfig::default());
        persist_capture(
            &store,
            sample_capture_snapshot(
                ts("2026-03-25T12:00:00Z"),
                WalletFreshnessVerdict::StalePublicationTruth,
                WalletShadowSignalVerdict::ShadowSignalsPresentButConcentrated,
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-legacy", "wallet-beta"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha"],
                &[],
            ),
        )?;
        persist_capture(
            &store,
            sample_capture_snapshot(
                ts("2026-03-25T11:50:00Z"),
                WalletFreshnessVerdict::FreshCurrent,
                WalletShadowSignalVerdict::ShadowSignalsPresentButConcentrated,
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha"],
                &[],
            ),
        )?;
        let report =
            discovery.wallet_freshness_history_report(&store, ts("2026-03-25T12:05:00Z"), 5)?;
        assert_eq!(
            report.verdict,
            WalletFreshnessHistoryVerdict::PublicationDrifting
        );
        assert_eq!(report.stale_capture_count, 1);
        Ok(())
    }

    #[test]
    fn history_report_flags_low_rotation_despite_fresh_truth() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-freshness-history-low-rotation.db");
        let store = open_store(&db_path)?;
        let discovery = DiscoveryService::new(freshness_test_config(), ShadowConfig::default());
        for captured_at in [
            ts("2026-03-25T12:00:00Z"),
            ts("2026-03-25T11:50:00Z"),
            ts("2026-03-25T11:40:00Z"),
        ] {
            persist_capture(
                &store,
                sample_capture_snapshot(
                    captured_at,
                    WalletFreshnessVerdict::FreshCurrent,
                    WalletShadowSignalVerdict::ShadowSignalsPresentButConcentrated,
                    &["wallet-alpha", "wallet-beta"],
                    &["wallet-alpha", "wallet-beta"],
                    &["wallet-alpha", "wallet-beta"],
                    &["wallet-alpha", "wallet-beta"],
                    &["wallet-alpha", "wallet-beta"],
                    &[],
                    &[],
                ),
            )?;
        }

        let report =
            discovery.wallet_freshness_history_report(&store, ts("2026-03-25T12:05:00Z"), 5)?;
        assert_eq!(
            report.verdict,
            WalletFreshnessHistoryVerdict::PartiallyValidatedButLowRotation
        );
        assert_eq!(report.captures_within_recent_horizon, 3);
        assert_eq!(report.rotation_evidence_capture_count, 0);
        Ok(())
    }

    #[test]
    fn history_report_requires_shadow_signal_evidence() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("wallet-freshness-history-shadow-missing.db");
        let store = open_store(&db_path)?;
        let discovery = DiscoveryService::new(freshness_test_config(), ShadowConfig::default());
        for captured_at in [
            ts("2026-03-25T12:00:00Z"),
            ts("2026-03-25T11:50:00Z"),
            ts("2026-03-25T11:40:00Z"),
        ] {
            persist_capture(
                &store,
                sample_capture_snapshot(
                    captured_at,
                    WalletFreshnessVerdict::FreshCurrent,
                    WalletShadowSignalVerdict::RecentSelectedRawActivityWithoutShadowSignals,
                    &["wallet-alpha", "wallet-gamma"],
                    &["wallet-alpha", "wallet-beta"],
                    &["wallet-alpha", "wallet-gamma"],
                    &["wallet-alpha", "wallet-gamma"],
                    &["wallet-alpha", "wallet-gamma"],
                    &["wallet-beta"],
                    &[],
                ),
            )?;
        }

        let report =
            discovery.wallet_freshness_history_report(&store, ts("2026-03-25T12:05:00Z"), 5)?;
        assert_eq!(
            report.verdict,
            WalletFreshnessHistoryVerdict::InsufficientEvidence
        );
        assert_eq!(report.shadow_signal_present_capture_count, 0);
        Ok(())
    }

    #[test]
    fn history_report_marks_raw_truth_insufficient_across_recent_captures() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("wallet-freshness-history-raw-insufficient.db");
        let store = open_store(&db_path)?;
        let discovery = DiscoveryService::new(freshness_test_config(), ShadowConfig::default());
        persist_capture(
            &store,
            sample_capture_snapshot(
                ts("2026-03-25T12:00:00Z"),
                WalletFreshnessVerdict::InsufficientRawTruth,
                WalletShadowSignalVerdict::ShadowSignalsPresentButConcentrated,
                &["wallet-alpha"],
                &["wallet-alpha"],
                &["wallet-alpha"],
                &["wallet-alpha"],
                &["wallet-alpha"],
                &[],
                &[],
            ),
        )?;
        let report =
            discovery.wallet_freshness_history_report(&store, ts("2026-03-25T12:05:00Z"), 5)?;
        assert_eq!(
            report.verdict,
            WalletFreshnessHistoryVerdict::RawTruthInsufficient
        );
        assert_eq!(report.insufficient_raw_capture_count, 1);
        Ok(())
    }

    #[test]
    fn fail_closed_preserved_publication_truth_stays_auditable_through_history_path() -> Result<()>
    {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-freshness-history-fail-closed.db");
        let store = open_store(&db_path)?;
        let now = ts("2026-03-25T12:00:00Z");
        let config = freshness_test_config();
        seed_ranked_wallet_window(
            &store,
            now,
            &[
                ("wallet-alpha", "mint-a", 4, 0),
                ("wallet-beta", "mint-b", 3, 10),
            ],
        )?;
        seed_publication_truth(
            &store,
            &config,
            now - Duration::seconds(60),
            now,
            DiscoveryRuntimeMode::FailClosed,
            &["wallet-alpha", "wallet-beta"],
        )?;
        store.activate_follow_wallet("wallet-alpha", now, "test-follow")?;
        store.activate_follow_wallet("wallet-beta", now, "test-follow")?;
        store.insert_copy_signal(&CopySignalRow {
            signal_id: "shadow:sig:wallet-alpha".to_string(),
            wallet_id: "wallet-alpha".to_string(),
            side: "buy".to_string(),
            token: "mint-a".to_string(),
            notional_sol: 0.2,
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
            ts: now - Duration::seconds(30),
            status: "shadow_recorded".to_string(),
        })?;

        let discovery = DiscoveryService::new(config, ShadowConfig::default());
        let capture =
            discovery.wallet_freshness_capture_snapshot(&store, now, DEFAULT_RECENT_CYCLES)?;
        persist_capture(&store, capture.clone())?;
        let report = discovery.wallet_freshness_history_report(&store, now, 5)?;

        assert!(capture.audit.publication_truth_available);
        assert_eq!(
            capture.audit.publication_runtime_mode.as_deref(),
            Some("fail_closed")
        );
        assert_eq!(report.no_publication_truth_capture_count, 0);
        Ok(())
    }

    #[test]
    fn stale_historical_captures_cannot_validate_current_selection() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-freshness-history-stale-only.db");
        let store = open_store(&db_path)?;
        let discovery = DiscoveryService::new(freshness_test_config(), ShadowConfig::default());
        for captured_at in [
            ts("2026-03-24T09:00:00Z"),
            ts("2026-03-24T08:50:00Z"),
            ts("2026-03-24T08:40:00Z"),
        ] {
            persist_capture(
                &store,
                sample_capture_snapshot(
                    captured_at,
                    WalletFreshnessVerdict::FreshCurrent,
                    WalletShadowSignalVerdict::ShadowSignalsPresentAndDistributed,
                    &["wallet-alpha", "wallet-beta"],
                    &["wallet-alpha", "wallet-beta"],
                    &["wallet-alpha", "wallet-beta"],
                    &["wallet-alpha", "wallet-beta"],
                    &["wallet-alpha", "wallet-beta"],
                    &["wallet-gamma"],
                    &["wallet-beta"],
                ),
            )?;
        }

        let report = discovery.wallet_freshness_history_report_with_horizon(
            &store,
            ts("2026-03-25T12:00:00Z"),
            5,
            3_600,
        )?;

        assert_eq!(
            report.verdict,
            WalletFreshnessHistoryVerdict::InsufficientEvidence
        );
        assert_eq!(
            report.reason,
            "no_recent_wallet_freshness_captures_within_horizon"
        );
        assert_eq!(report.captures_loaded, 3);
        assert_eq!(report.captures_considered, 0);
        assert_eq!(report.captures_within_recent_horizon, 0);
        assert!(report.stale_captures_excluded_from_verdict);
        assert_eq!(report.stale_captures_excluded_count, 3);
        Ok(())
    }

    #[test]
    fn mixed_recent_and_stale_captures_only_validate_from_recent_subset() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("wallet-freshness-history-mixed-recency.db");
        let store = open_store(&db_path)?;
        let discovery = DiscoveryService::new(freshness_test_config(), ShadowConfig::default());
        persist_capture(
            &store,
            sample_capture_snapshot(
                ts("2026-03-25T12:00:00Z"),
                WalletFreshnessVerdict::FreshCurrent,
                WalletShadowSignalVerdict::ShadowSignalsPresentAndDistributed,
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-gamma"],
                &["wallet-beta"],
            ),
        )?;
        persist_capture(
            &store,
            sample_capture_snapshot(
                ts("2026-03-25T11:50:00Z"),
                WalletFreshnessVerdict::FreshCurrent,
                WalletShadowSignalVerdict::ShadowSignalsPresentAndDistributed,
                &["wallet-alpha", "wallet-gamma"],
                &["wallet-alpha", "wallet-beta"],
                &["wallet-alpha", "wallet-gamma"],
                &["wallet-alpha", "wallet-gamma"],
                &["wallet-alpha", "wallet-gamma"],
                &["wallet-beta"],
                &["wallet-alpha"],
            ),
        )?;
        persist_capture(
            &store,
            sample_capture_snapshot(
                ts("2026-03-24T08:40:00Z"),
                WalletFreshnessVerdict::FreshCurrent,
                WalletShadowSignalVerdict::ShadowSignalsPresentAndDistributed,
                &["wallet-beta", "wallet-gamma"],
                &["wallet-beta", "wallet-gamma"],
                &["wallet-beta", "wallet-gamma"],
                &["wallet-beta", "wallet-gamma"],
                &["wallet-beta", "wallet-gamma"],
                &["wallet-alpha"],
                &["wallet-beta"],
            ),
        )?;

        let report = discovery.wallet_freshness_history_report_with_horizon(
            &store,
            ts("2026-03-25T12:05:00Z"),
            5,
            1_800,
        )?;

        assert_eq!(
            report.verdict,
            WalletFreshnessHistoryVerdict::InsufficientEvidence
        );
        assert_eq!(
            report.reason,
            "fewer_than_three_recent_wallet_freshness_captures"
        );
        assert_eq!(report.captures_loaded, 3);
        assert_eq!(report.captures_considered, 2);
        assert_eq!(report.captures_within_recent_horizon, 2);
        assert_eq!(report.stale_captures_excluded_count, 1);
        Ok(())
    }

    #[test]
    fn recent_valid_captures_can_still_validate_with_stale_history_present() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("wallet-freshness-history-recent-subset-valid.db");
        let store = open_store(&db_path)?;
        let discovery = DiscoveryService::new(freshness_test_config(), ShadowConfig::default());
        for captured_at in [
            ts("2026-03-25T12:00:00Z"),
            ts("2026-03-25T11:50:00Z"),
            ts("2026-03-25T11:40:00Z"),
        ] {
            persist_capture(
                &store,
                sample_capture_snapshot(
                    captured_at,
                    WalletFreshnessVerdict::FreshCurrent,
                    WalletShadowSignalVerdict::ShadowSignalsPresentAndDistributed,
                    &["wallet-alpha", "wallet-beta"],
                    &["wallet-alpha", "wallet-beta"],
                    &["wallet-alpha", "wallet-beta"],
                    &["wallet-alpha", "wallet-beta"],
                    &["wallet-alpha", "wallet-beta"],
                    &["wallet-gamma"],
                    &["wallet-beta"],
                ),
            )?;
        }
        persist_capture(
            &store,
            sample_capture_snapshot(
                ts("2026-03-24T08:40:00Z"),
                WalletFreshnessVerdict::FreshCurrent,
                WalletShadowSignalVerdict::ShadowSignalsPresentAndDistributed,
                &["wallet-old-a", "wallet-old-b"],
                &["wallet-old-a", "wallet-old-b"],
                &["wallet-old-a", "wallet-old-b"],
                &["wallet-old-a", "wallet-old-b"],
                &["wallet-old-a", "wallet-old-b"],
                &["wallet-old-c"],
                &["wallet-old-b"],
            ),
        )?;

        let report = discovery.wallet_freshness_history_report_with_horizon(
            &store,
            ts("2026-03-25T12:05:00Z"),
            5,
            3_600,
        )?;

        assert_eq!(
            report.verdict,
            WalletFreshnessHistoryVerdict::ValidatedCurrent
        );
        assert_eq!(report.captures_loaded, 4);
        assert_eq!(report.captures_considered, 3);
        assert_eq!(report.captures_within_recent_horizon, 3);
        assert_eq!(report.stale_captures_excluded_count, 1);
        assert!(report.stale_captures_excluded_from_verdict);
        Ok(())
    }

    fn open_store(path: &Path) -> Result<SqliteStore> {
        let mut store = SqliteStore::open(path)?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;
        Ok(store)
    }

    fn freshness_test_config() -> DiscoveryConfig {
        let mut config = DiscoveryConfig::default();
        config.scoring_window_days = 5;
        config.decay_window_days = 5;
        config.observed_swaps_retention_days = 14;
        config.refresh_seconds = 600;
        config.metric_snapshot_interval_seconds = 60;
        config.follow_top_n = 2;
        config.min_score = 0.0;
        config.min_trades = 1;
        config.min_active_days = 1;
        config.min_leader_notional_sol = 0.0;
        config.min_buy_count = 1;
        config.min_tradable_ratio = 0.0;
        config.max_rug_ratio = 1.0;
        config.max_window_swaps_in_memory = 256;
        config.max_fetch_swaps_per_cycle = 256;
        config.max_fetch_pages_per_cycle = 8;
        config.fetch_time_budget_ms = 1_000;
        config.thin_market_min_unique_traders = 1;
        config
    }

    fn expected_metrics_window_start(
        config: &DiscoveryConfig,
        now: DateTime<Utc>,
    ) -> DateTime<Utc> {
        let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
        let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
        let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
        bucketed_now - Duration::days(config.scoring_window_days.max(1) as i64)
    }

    fn seed_publication_truth(
        store: &SqliteStore,
        config: &DiscoveryConfig,
        last_published_at: DateTime<Utc>,
        now: DateTime<Utc>,
        runtime_mode: DiscoveryRuntimeMode,
        wallet_ids: &[&str],
    ) -> Result<()> {
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode,
            reason: "test-publication".to_string(),
            last_published_at: Some(last_published_at),
            last_published_window_start: Some(expected_metrics_window_start(config, now)),
            published_scoring_source: Some("raw_window_persisted_stream".to_string()),
            published_wallet_ids: Some(
                wallet_ids
                    .iter()
                    .map(|wallet| (*wallet).to_string())
                    .collect(),
            ),
        })
    }

    fn seed_ranked_wallet_window(
        store: &SqliteStore,
        now: DateTime<Utc>,
        wallets: &[(&str, &str, usize, i64)],
    ) -> Result<()> {
        let coverage_start = now - Duration::days(5);
        for (wallet_idx, (wallet_id, mint, trades, offset_minutes)) in wallets.iter().enumerate() {
            if *trades == 0 {
                continue;
            }
            store.insert_observed_swap(&buy_swap(
                &format!("{wallet_id}-head"),
                wallet_id,
                mint,
                10_000 + wallet_idx as u64 * 100,
                coverage_start + Duration::minutes(wallet_idx as i64),
            ))?;
            for trade_idx in 1..*trades {
                let ts = now
                    - Duration::minutes(*offset_minutes)
                    - Duration::minutes(trade_idx as i64)
                    - Duration::minutes((wallet_idx as i64) * 2);
                store.insert_observed_swap(&buy_swap(
                    &format!("{wallet_id}-{trade_idx}"),
                    wallet_id,
                    mint,
                    10_000 + wallet_idx as u64 * 100 + trade_idx as u64,
                    ts,
                ))?;
            }
        }
        Ok(())
    }

    fn seed_recent_only_ranked_wallet_window(
        store: &SqliteStore,
        now: DateTime<Utc>,
        wallets: &[(&str, &str, usize, i64)],
    ) -> Result<()> {
        for (wallet_idx, (wallet_id, mint, trades, offset_minutes)) in wallets.iter().enumerate() {
            for trade_idx in 0..*trades {
                let ts = now
                    - Duration::minutes(*offset_minutes)
                    - Duration::minutes(trade_idx as i64)
                    - Duration::minutes((wallet_idx as i64) * 2);
                store.insert_observed_swap(&buy_swap(
                    &format!("{wallet_id}-recent-{trade_idx}"),
                    wallet_id,
                    mint,
                    20_000 + wallet_idx as u64 * 100 + trade_idx as u64,
                    ts,
                ))?;
            }
        }
        Ok(())
    }

    fn persist_capture(store: &SqliteStore, capture: WalletFreshnessCaptureSnapshot) -> Result<()> {
        store.append_discovery_wallet_freshness_capture(&capture.to_storage_write()?)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn sample_capture_snapshot(
        captured_at: DateTime<Utc>,
        audit_verdict: WalletFreshnessVerdict,
        shadow_verdict: WalletShadowSignalVerdict,
        published_wallet_ids: &[&str],
        active_follow_wallet_ids: &[&str],
        current_raw_top_wallet_ids: &[&str],
        selected_raw_wallet_ids: &[&str],
        selected_shadow_wallet_ids: &[&str],
        rotation_entered: &[&str],
        rotation_left: &[&str],
    ) -> WalletFreshnessCaptureSnapshot {
        let published_wallet_ids = published_wallet_ids
            .iter()
            .map(|value| (*value).to_string())
            .collect::<Vec<_>>();
        let active_follow_wallet_ids = active_follow_wallet_ids
            .iter()
            .map(|value| (*value).to_string())
            .collect::<Vec<_>>();
        let current_raw_top_wallet_ids = current_raw_top_wallet_ids
            .iter()
            .map(|value| (*value).to_string())
            .collect::<Vec<_>>();
        let selected_raw_wallet_ids = selected_raw_wallet_ids
            .iter()
            .map(|value| (*value).to_string())
            .collect::<Vec<_>>();
        let selected_shadow_wallet_ids = selected_shadow_wallet_ids
            .iter()
            .map(|value| (*value).to_string())
            .collect::<Vec<_>>();
        let rotation_entered = rotation_entered
            .iter()
            .map(|value| (*value).to_string())
            .collect::<Vec<_>>();
        let rotation_left = rotation_left
            .iter()
            .map(|value| (*value).to_string())
            .collect::<Vec<_>>();
        let raw_counts = selected_raw_wallet_ids
            .iter()
            .enumerate()
            .map(|(idx, wallet_id)| WalletRecentActivityCountRow {
                wallet_id: wallet_id.clone(),
                row_count: (selected_raw_wallet_ids.len().saturating_sub(idx)).max(1),
                latest_ts: captured_at - Duration::seconds(idx as i64),
            })
            .collect::<Vec<_>>();
        let shadow_counts = selected_shadow_wallet_ids
            .iter()
            .enumerate()
            .map(|(idx, wallet_id)| WalletRecentActivityCountRow {
                wallet_id: wallet_id.clone(),
                row_count: (selected_shadow_wallet_ids.len().saturating_sub(idx)).max(1),
                latest_ts: captured_at - Duration::seconds(idx as i64),
            })
            .collect::<Vec<_>>();
        let selected_wallet_count = active_follow_wallet_ids.len();

        WalletFreshnessCaptureSnapshot {
            capture_id: None,
            captured_at,
            capture_age_seconds: None,
            within_recent_horizon: None,
            recent_cycles: 3,
            audit: WalletFreshnessAuditReport {
                now: captured_at,
                window_start: captured_at - Duration::days(5),
                verdict: audit_verdict,
                reason: audit_reason(audit_verdict).to_string(),
                follow_top_n: current_raw_top_wallet_ids.len(),
                publication_truth_available: true,
                publication_runtime_mode: Some("healthy".to_string()),
                publication_recent_under_gate: true,
                latest_publication_ts: Some(captured_at - Duration::seconds(60)),
                publication_age_seconds: Some(60),
                latest_publication_window_start: Some(captured_at - Duration::days(5)),
                published_scoring_source: Some("raw_window_persisted_stream".to_string()),
                published_wallet_ids: published_wallet_ids.clone(),
                active_follow_wallet_ids: active_follow_wallet_ids.clone(),
                current_raw_top_wallet_ids: current_raw_top_wallet_ids.clone(),
                published_vs_current_raw: WalletUniverseComparison {
                    left_count: published_wallet_ids.len(),
                    right_count: current_raw_top_wallet_ids.len(),
                    overlap_count: published_wallet_ids
                        .iter()
                        .collect::<std::collections::BTreeSet<_>>()
                        .intersection(
                            &current_raw_top_wallet_ids
                                .iter()
                                .collect::<std::collections::BTreeSet<_>>(),
                        )
                        .count(),
                    exact_match: compare_wallet_universes(
                        &published_wallet_ids,
                        &current_raw_top_wallet_ids,
                    )
                    .exact_match,
                    only_left: compare_wallet_universes(
                        &published_wallet_ids,
                        &current_raw_top_wallet_ids,
                    )
                    .only_left,
                    only_right: compare_wallet_universes(
                        &published_wallet_ids,
                        &current_raw_top_wallet_ids,
                    )
                    .only_right,
                },
                active_follow_vs_current_raw: compare_wallet_universes(
                    &active_follow_wallet_ids,
                    &current_raw_top_wallet_ids,
                ),
                active_follow_vs_published: compare_wallet_universes(
                    &active_follow_wallet_ids,
                    &published_wallet_ids,
                ),
                raw_truth: WalletFreshnessRawTruthStatus {
                    sufficient: audit_verdict != WalletFreshnessVerdict::InsufficientRawTruth,
                    reason: if audit_verdict == WalletFreshnessVerdict::InsufficientRawTruth {
                        "observed_swaps_coverage_ends_before_freshness_gate".to_string()
                    } else {
                        "full_scoring_window_raw_truth_available".to_string()
                    },
                    observed_swaps_loaded: 10,
                    eligible_wallet_count: current_raw_top_wallet_ids.len(),
                    top_wallet_count: current_raw_top_wallet_ids.len(),
                    short_retention_configured: false,
                    covered_since: Some(captured_at - Duration::days(5)),
                    covered_through_cursor: Some(DiscoveryRuntimeCursor {
                        ts_utc: captured_at,
                        slot: 1,
                        signature: format!("sig-{}", captured_at.timestamp()),
                    }),
                    covered_through_lag_seconds: Some(30),
                    tail_fresh_within_runtime_lag: audit_verdict
                        != WalletFreshnessVerdict::InsufficientRawTruth,
                    runtime_freshness_lag_seconds: 600,
                    total_observed_swaps_rows: 10,
                },
                rotation: WalletFreshnessRotationSignal {
                    signal_available: true,
                    reason: None,
                    cycles_requested: 3,
                    cycles_completed: 3,
                    sample_interval_seconds: 600,
                    overlap_with_previous_cycle: Some(current_raw_top_wallet_ids.len()),
                    entered_since_previous_cycle: rotation_entered.clone(),
                    left_since_previous_cycle: rotation_left.clone(),
                    stable_wallets_across_cycles: current_raw_top_wallet_ids.clone(),
                    unique_wallet_count_across_cycles: current_raw_top_wallet_ids.len()
                        + rotation_entered.len(),
                    samples: vec![WalletFreshnessRawCycleSample {
                        sample_now: captured_at,
                        window_start: captured_at - Duration::days(5),
                        observed_swaps_loaded: 10,
                        eligible_wallet_count: current_raw_top_wallet_ids.len(),
                        top_wallet_ids: current_raw_top_wallet_ids.clone(),
                    }],
                },
            },
            shadow_signal: WalletFreshnessShadowSignalEvidence {
                recent_window_start: captured_at - Duration::minutes(30),
                recent_window_end: captured_at,
                evidence_lookback_seconds: Some(1_800),
                selected_wallet_ids: active_follow_wallet_ids,
                selected_wallet_count,
                selected_wallets_with_recent_raw_activity: selected_raw_wallet_ids.len(),
                selected_wallets_with_recent_shadow_signal: selected_shadow_wallet_ids.len(),
                recent_raw_swap_count: raw_counts.iter().map(|row| row.row_count).sum(),
                recent_shadow_signal_count: shadow_counts.iter().map(|row| row.row_count).sum(),
                recent_raw_activity_wallet_ids: selected_raw_wallet_ids,
                recent_shadow_signal_wallet_ids: selected_shadow_wallet_ids,
                recent_raw_activity_by_wallet: raw_counts,
                recent_shadow_signal_by_wallet: shadow_counts,
                raw_activity_top_wallet_share: Some(1.0),
                shadow_signal_top_wallet_share: Some(1.0),
                raw_activity_broadly_distributed: false,
                shadow_signal_broadly_distributed: matches!(
                    shadow_verdict,
                    WalletShadowSignalVerdict::ShadowSignalsPresentAndDistributed
                ),
                verdict: shadow_verdict,
                reason: shadow_reason(shadow_verdict).to_string(),
            },
        }
    }

    fn audit_reason(verdict: WalletFreshnessVerdict) -> &'static str {
        match verdict {
            WalletFreshnessVerdict::FreshCurrent => "published_and_active_match_current_raw_top_n",
            WalletFreshnessVerdict::DriftingButAcceptable => {
                "publication_recent_under_gate_but_current_raw_top_n_has_rotated"
            }
            WalletFreshnessVerdict::StalePublicationTruth => {
                "published_universe_drifted_from_current_raw_top_n"
            }
            WalletFreshnessVerdict::InsufficientRawTruth => {
                "observed_swaps_coverage_ends_before_freshness_gate"
            }
            WalletFreshnessVerdict::FailClosedNoPublicationTruth => "no_complete_publication_truth",
        }
    }

    fn shadow_reason(verdict: WalletShadowSignalVerdict) -> &'static str {
        match verdict {
            WalletShadowSignalVerdict::NoSelectedWallets => "no_active_follow_wallets_selected",
            WalletShadowSignalVerdict::NoRecentSelectedRawActivity => {
                "no_recent_observed_swaps_from_selected_wallets"
            }
            WalletShadowSignalVerdict::RecentSelectedRawActivityWithoutShadowSignals => {
                "selected_wallets_emit_recent_raw_activity_but_no_shadow_signals"
            }
            WalletShadowSignalVerdict::ShadowSignalsPresentButConcentrated => {
                "recent_shadow_signals_present_but_concentrated_in_few_selected_wallets"
            }
            WalletShadowSignalVerdict::ShadowSignalsPresentAndDistributed => {
                "recent_shadow_signals_present_across_multiple_selected_wallets"
            }
        }
    }

    fn buy_swap(
        signature: &str,
        wallet: &str,
        mint: &str,
        slot: u64,
        ts_utc: DateTime<Utc>,
    ) -> SwapEvent {
        SwapEvent {
            signature: signature.to_string(),
            wallet: wallet.to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: mint.to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            slot,
            ts_utc,
            exact_amounts: None,
        }
    }

    fn ts(raw: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(raw)
            .expect("valid rfc3339")
            .with_timezone(&Utc)
    }
}
