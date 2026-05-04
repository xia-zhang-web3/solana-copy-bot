#[derive(Debug, Clone)]
pub struct TrustedWalletMetricsSnapshotWrite {
    pub snapshot_id: String,
    pub source_snapshot_id: Option<String>,
    pub source_window_start: Option<DateTime<Utc>>,
    pub effective_window_start: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub source_kind: TrustedSnapshotSourceKind,
    pub row_count: usize,
    pub trust_state: TrustedSelectionState,
}

#[derive(Debug, Clone)]
pub struct TrustedWalletMetricsSnapshotRow {
    pub snapshot_id: String,
    pub source_snapshot_id: Option<String>,
    pub source_window_start: Option<DateTime<Utc>>,
    pub effective_window_start: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub source_kind: TrustedSnapshotSourceKind,
    pub row_count: usize,
    pub trust_state: TrustedSelectionState,
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
pub struct DiscoveryPublicationStateUpdate {
    pub runtime_mode: DiscoveryRuntimeMode,
    pub reason: String,
    pub last_published_at: Option<DateTime<Utc>>,
    pub last_published_window_start: Option<DateTime<Utc>>,
    pub published_scoring_source: Option<String>,
    pub published_wallet_ids: Option<Vec<String>>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryPublicationStateRow {
    pub runtime_mode: DiscoveryRuntimeMode,
    pub reason: String,
    pub last_published_at: Option<DateTime<Utc>>,
    pub last_published_window_start: Option<DateTime<Utc>>,
    pub published_scoring_source: Option<String>,
    pub published_wallet_ids: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication_policy_fingerprint: Option<String>,
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
        }
        .expected_metrics_window_start(now);
        let max_lag = Duration::seconds(metric_snapshot_interval_seconds.max(1) as i64);
        last_published_window_start + max_lag >= expected_metrics_window_start
    }

    pub fn has_valid_published_window_under_gate(
        &self,
        gate: DiscoveryPublicationFreshnessGate,
        now: DateTime<Utc>,
    ) -> bool {
        let Some(last_published_window_start) = self.last_published_window_start else {
            return false;
        };
        let expected_metrics_window_start = gate.expected_metrics_window_start(now);
        let max_lag = Duration::seconds(gate.metric_snapshot_interval_seconds.max(1) as i64);
        last_published_window_start + max_lag >= expected_metrics_window_start
    }

    pub fn is_fresh_under_gate(
        &self,
        gate: DiscoveryPublicationFreshnessGate,
        now: DateTime<Utc>,
    ) -> bool {
        let Some(last_published_at) = self.last_published_at else {
            return false;
        };
        self.has_complete_publication_truth()
            && now.signed_duration_since(last_published_at) <= gate.published_universe_max_age()
            && self.has_valid_published_window_under_gate(gate, now)
    }
}
