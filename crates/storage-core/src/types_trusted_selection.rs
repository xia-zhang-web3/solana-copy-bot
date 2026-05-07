use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration, Utc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrustedSelectionState {
    TrustedCurrent,
    Invalid,
}

impl TrustedSelectionState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TrustedCurrent => "trusted_current",
            Self::Invalid => "invalid",
        }
    }

    pub fn parse(raw: &str) -> Result<Self> {
        match raw {
            "trusted_current" => Ok(Self::TrustedCurrent),
            "trusted_bridged" | "trusted_bridged_stale" | "invalid" => Ok(Self::Invalid),
            _ => Err(anyhow!("invalid trusted selection state: {raw}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrustedSnapshotSourceKind {
    DiscoveryRefresh,
    Legacy,
}

impl TrustedSnapshotSourceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DiscoveryRefresh => "discovery_refresh",
            Self::Legacy => "legacy",
        }
    }

    pub fn parse(raw: &str) -> Result<Self> {
        match raw {
            "discovery_refresh" => Ok(Self::DiscoveryRefresh),
            "clone_latest_bridge" | "admin_materialization" | "legacy" => Ok(Self::Legacy),
            _ => Err(anyhow!("invalid trusted snapshot source kind: {raw}")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DiscoveryTrustedSelectionStateUpdate {
    pub bootstrap_required: bool,
    pub reason: String,
    pub selection_state: TrustedSelectionState,
    pub active_snapshot_id: Option<String>,
    pub active_snapshot_window_start: Option<DateTime<Utc>>,
    pub last_bootstrap_source_kind: Option<TrustedSnapshotSourceKind>,
    pub last_bootstrap_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct DiscoveryTrustedSelectionStateRow {
    pub bootstrap_required: bool,
    pub reason: String,
    pub selection_state: TrustedSelectionState,
    pub active_snapshot_id: Option<String>,
    pub active_snapshot_window_start: Option<DateTime<Utc>>,
    pub last_bootstrap_source_kind: Option<TrustedSnapshotSourceKind>,
    pub last_bootstrap_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct StartupTrustedSelectionGateStatus {
    pub bootstrap_required: bool,
    pub selection_state: Option<TrustedSelectionState>,
    pub startup_fail_closed: bool,
    pub reason: Option<String>,
    pub active_snapshot_id: Option<String>,
    pub active_snapshot_window_start: Option<DateTime<Utc>>,
    pub last_bootstrap_source_kind: Option<TrustedSnapshotSourceKind>,
    pub source_snapshot_window_start: Option<DateTime<Utc>>,
    pub legacy_bool_fallback_used: bool,
}

impl StartupTrustedSelectionGateStatus {
    fn expected_metrics_window_start(
        now: DateTime<Utc>,
        scoring_window_days: i64,
        metric_snapshot_interval_seconds: u64,
    ) -> DateTime<Utc> {
        let interval_seconds = metric_snapshot_interval_seconds.max(1) as i64;
        let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
        let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
        bucketed_now - Duration::days(scoring_window_days.max(1))
    }

    pub fn effective_selection_state(
        &self,
        now: DateTime<Utc>,
        scoring_window_days: i64,
        metric_snapshot_interval_seconds: u64,
        _max_bootstrap_snapshot_age_seconds: u64,
    ) -> Option<TrustedSelectionState> {
        let selection_state = self.selection_state?;
        let expected_metrics_window_start = Self::expected_metrics_window_start(
            now,
            scoring_window_days,
            metric_snapshot_interval_seconds,
        );
        if selection_state == TrustedSelectionState::TrustedCurrent {
            let Some(active_snapshot_window_start) = self.active_snapshot_window_start else {
                return Some(TrustedSelectionState::Invalid);
            };
            let max_lag = Duration::seconds(metric_snapshot_interval_seconds.max(1) as i64);
            if active_snapshot_window_start + max_lag < expected_metrics_window_start {
                return Some(TrustedSelectionState::Invalid);
            }
        }
        Some(selection_state)
    }

    pub fn effective_startup_fail_closed(
        &self,
        now: DateTime<Utc>,
        scoring_window_days: i64,
        metric_snapshot_interval_seconds: u64,
        max_bootstrap_snapshot_age_seconds: u64,
    ) -> bool {
        self.bootstrap_required
            || matches!(
                self.effective_selection_state(
                    now,
                    scoring_window_days,
                    metric_snapshot_interval_seconds,
                    max_bootstrap_snapshot_age_seconds
                ),
                Some(TrustedSelectionState::Invalid)
            )
            || (self.selection_state.is_none() && self.startup_fail_closed)
    }
}
