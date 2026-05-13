use super::types_runtime::{DiscoveryRuntimeCursor, DiscoveryRuntimeMode};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryPublicationStateRow {
    pub runtime_mode: DiscoveryRuntimeMode,
    pub reason: String,
    pub last_published_at: Option<DateTime<Utc>>,
    pub last_published_window_start: Option<DateTime<Utc>>,
    pub published_scoring_source: Option<String>,
    pub published_wallet_ids: Option<Vec<String>>,
    pub publication_policy_fingerprint: Option<String>,
    pub publication_runtime_cursor: Option<DiscoveryRuntimeCursor>,
    pub updated_at: DateTime<Utc>,
}

impl DiscoveryPublicationStateRow {
    pub fn has_complete_publication_truth(&self) -> bool {
        self.last_published_at.is_some()
            && self.last_published_window_start.is_some()
            && self
                .published_wallet_ids
                .as_ref()
                .is_some_and(|wallet_ids| !wallet_ids.is_empty())
    }

    pub fn has_valid_recent_published_universe(
        &self,
        now: DateTime<Utc>,
        scoring_window_days: i64,
        metric_snapshot_interval_seconds: u64,
    ) -> bool {
        let Some(last_published_window_start) = self.last_published_window_start else {
            return false;
        };
        let expected_metrics_window_start = DiscoveryPublicationFreshnessGate {
            scoring_window_days,
            metric_snapshot_interval_seconds,
            refresh_seconds: metric_snapshot_interval_seconds,
            expected_scoring_source: None,
            expected_policy_fingerprint: None,
        }
        .expected_metrics_window_start(now);
        let max_lag = Duration::seconds(metric_snapshot_interval_seconds.max(1) as i64);
        let earliest_window_start = expected_metrics_window_start - max_lag;
        let latest_window_start = expected_metrics_window_start + max_lag;
        last_published_window_start >= earliest_window_start
            && last_published_window_start <= latest_window_start
            && last_published_window_start <= now
    }

    pub fn has_valid_published_window_under_gate(
        &self,
        gate: &DiscoveryPublicationFreshnessGate,
        now: DateTime<Utc>,
    ) -> bool {
        let Some(last_published_window_start) = self.last_published_window_start else {
            return false;
        };
        let expected_metrics_window_start = gate.expected_metrics_window_start(now);
        let max_lag = Duration::seconds(gate.metric_snapshot_interval_seconds.max(1) as i64);
        let earliest_window_start = expected_metrics_window_start - max_lag;
        let latest_window_start = expected_metrics_window_start + max_lag;
        last_published_window_start >= earliest_window_start
            && last_published_window_start <= latest_window_start
            && last_published_window_start <= now
    }

    pub fn is_fresh_under_gate(
        &self,
        gate: &DiscoveryPublicationFreshnessGate,
        now: DateTime<Utc>,
    ) -> bool {
        let Some(last_published_at) = self.last_published_at else {
            return false;
        };
        let published_age = now.signed_duration_since(last_published_at);
        self.has_complete_publication_truth()
            && published_age >= Duration::zero()
            && published_age <= gate.published_universe_max_age()
            && self.has_valid_published_window_under_gate(gate, now)
    }

    pub fn has_fresh_publication_runtime_cursor_under_gate(
        &self,
        gate: &DiscoveryPublicationFreshnessGate,
        now: DateTime<Utc>,
    ) -> bool {
        self.publication_runtime_cursor
            .as_ref()
            .is_some_and(|cursor| gate.is_fresh_runtime_cursor(cursor, now))
    }

    pub fn matches_expected_publication_identity(
        &self,
        gate: &DiscoveryPublicationFreshnessGate,
    ) -> bool {
        if let Some(expected_source) = gate.expected_scoring_source.as_deref() {
            if self.published_scoring_source.as_deref() != Some(expected_source) {
                return false;
            }
        }
        if let Some(expected_fingerprint) = gate.expected_policy_fingerprint.as_deref() {
            if self.publication_policy_fingerprint.as_deref() != Some(expected_fingerprint) {
                return false;
            }
        }
        true
    }

    pub fn matches_publication_runtime_cursor(&self, cursor: &DiscoveryRuntimeCursor) -> bool {
        self.publication_runtime_cursor.as_ref() == Some(cursor)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistedWalletMetricSnapshotRow {
    pub wallet_id: String,
    pub window_start: DateTime<Utc>,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub pnl: f64,
    pub win_rate: f64,
    pub trades: u32,
    pub closed_trades: u32,
    pub hold_median_seconds: i64,
    pub score: f64,
    pub buy_total: u32,
    pub tradable_ratio: f64,
    pub rug_ratio: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveryPublicationFreshnessGate {
    pub scoring_window_days: i64,
    pub metric_snapshot_interval_seconds: u64,
    pub refresh_seconds: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_scoring_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_policy_fingerprint: Option<String>,
}

impl DiscoveryPublicationFreshnessGate {
    pub fn published_universe_max_age(&self) -> Duration {
        Duration::seconds(
            self.metric_snapshot_interval_seconds
                .max(self.refresh_seconds.max(1))
                .saturating_mul(3) as i64,
        )
    }

    pub fn runtime_cursor_max_age(&self) -> Duration {
        Duration::seconds(self.refresh_seconds.max(1).min(i64::MAX as u64) as i64)
    }

    pub fn is_fresh_runtime_cursor(
        &self,
        cursor: &DiscoveryRuntimeCursor,
        now: DateTime<Utc>,
    ) -> bool {
        let cursor_age = now.signed_duration_since(cursor.ts_utc);
        cursor_age >= Duration::zero() && cursor_age <= self.runtime_cursor_max_age()
    }

    fn expected_metrics_window_start(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        let interval_seconds = self.metric_snapshot_interval_seconds.max(1) as i64;
        let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
        let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
        bucketed_now - Duration::days(self.scoring_window_days.max(1))
    }
}

pub const DISCOVERY_RUNTIME_ARTIFACT_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryRuntimeArtifact {
    pub format_version: u32,
    pub exported_at: DateTime<Utc>,
    pub export_gate: DiscoveryPublicationFreshnessGate,
    pub publication_state: DiscoveryPublicationStateRow,
    pub runtime_cursor: DiscoveryRuntimeCursor,
    pub published_wallet_metrics_snapshot: Vec<PersistedWalletMetricSnapshotRow>,
}
