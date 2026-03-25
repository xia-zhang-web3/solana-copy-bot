use crate::{
    followlist::{desired_wallets, rank_follow_candidates},
    DiscoveryService, RuntimePublishedUniverseTruth,
};
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_storage::{
    DiscoveryPublicationStateRow, DiscoveryRuntimeCursor, ObservedSwapsCoverageSnapshot,
    SqliteStore,
};
use serde::Serialize;
use std::collections::BTreeSet;

pub const DEFAULT_RECENT_CYCLES: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
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

#[derive(Debug, Clone, Serialize)]
pub struct WalletUniverseComparison {
    pub left_count: usize,
    pub right_count: usize,
    pub overlap_count: usize,
    pub exact_match: bool,
    pub only_left: Vec<String>,
    pub only_right: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
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

#[derive(Debug, Clone, Serialize)]
pub struct WalletFreshnessRawCycleSample {
    pub sample_now: DateTime<Utc>,
    pub window_start: DateTime<Utc>,
    pub observed_swaps_loaded: usize,
    pub eligible_wallet_count: usize,
    pub top_wallet_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
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

#[derive(Debug, Clone, Serialize)]
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

#[derive(Debug, Clone)]
struct RawTruthSample {
    window_start: DateTime<Utc>,
    observed_swaps_loaded: usize,
    eligible_wallet_count: usize,
    top_wallet_ids: Vec<String>,
}

impl DiscoveryService {
    pub fn wallet_freshness_audit(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        recent_cycles: usize,
    ) -> Result<WalletFreshnessAuditReport> {
        let recent_cycles = recent_cycles.max(1);
        let window_start = now - Duration::days(self.config.scoring_window_days.max(1) as i64);
        let publication_state = store.discovery_publication_state_read_only()?;
        let publication_truth = publication_truth_for_audit(publication_state.as_ref());
        let publication_recent_under_gate = publication_state
            .as_ref()
            .is_some_and(|state| state.is_fresh_under_gate(self.publication_freshness_gate(), now));
        let active_follow_wallet_ids =
            sorted_wallets_from_iter(store.list_active_follow_wallets()?);
        let current_raw = self.current_raw_truth_sample(store, now)?;
        let raw_coverage = store.observed_swaps_coverage_snapshot()?;
        let raw_truth =
            self.classify_raw_truth_status(raw_coverage, window_start, now, &current_raw);

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
        let rotation = self.build_rotation_signal(store, now, recent_cycles)?;

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
            current_raw_top_wallet_ids: current_raw.top_wallet_ids,
            published_vs_current_raw,
            active_follow_vs_current_raw,
            active_follow_vs_published,
            raw_truth,
            rotation,
        })
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

    fn build_rotation_signal(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        recent_cycles: usize,
    ) -> Result<WalletFreshnessRotationSignal> {
        let cycles_requested = recent_cycles.max(1);
        let sample_interval_seconds = self.config.refresh_seconds.max(1);
        let mut samples = Vec::with_capacity(cycles_requested);
        for idx in 0..cycles_requested {
            let sample_now =
                now - Duration::seconds(sample_interval_seconds.saturating_mul(idx as u64) as i64);
            let sample = self.current_raw_truth_sample(store, sample_now)?;
            samples.push(WalletFreshnessRawCycleSample {
                sample_now,
                window_start: sample.window_start,
                observed_swaps_loaded: sample.observed_swaps_loaded,
                eligible_wallet_count: sample.eligible_wallet_count,
                top_wallet_ids: sample.top_wallet_ids,
            });
        }

        if samples.len() < 2 {
            return Ok(WalletFreshnessRotationSignal {
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
            });
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

        Ok(WalletFreshnessRotationSignal {
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
        })
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

#[cfg(test)]
mod tests {
    use super::{WalletFreshnessVerdict, DEFAULT_RECENT_CYCLES};
    use anyhow::{Context, Result};
    use chrono::{DateTime, Duration, Utc};
    use copybot_config::{DiscoveryConfig, ShadowConfig};
    use copybot_core_types::SwapEvent;
    use copybot_storage::{
        DiscoveryPublicationStateUpdate, DiscoveryRuntimeCursor, DiscoveryRuntimeMode, SqliteStore,
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
