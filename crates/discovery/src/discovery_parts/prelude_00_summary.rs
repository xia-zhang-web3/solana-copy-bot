#[derive(Debug, Clone)]
pub struct DiscoveryService {
    config: DiscoveryConfig,
    shadow_quality: ShadowConfig,
    helius_http_url: Option<String>,
    window_state: Arc<Mutex<DiscoveryWindowState>>,
}

#[derive(Debug, Clone, Default)]
pub struct DiscoverySummary {
    pub window_start: DateTime<Utc>,
    pub wallets_seen: usize,
    pub eligible_wallets: usize,
    pub metrics_written: usize,
    pub follow_promoted: usize,
    pub follow_demoted: usize,
    pub active_follow_wallets: usize,
    pub top_wallets: Vec<String>,
    pub published: bool,
    pub runtime_mode: DiscoveryRuntimeMode,
    pub scoring_source: &'static str,
    pub trusted_selection_fail_closed: bool,
    pub raw_window_cap_truncated: bool,
    pub cap_truncation_deactivation_guard_active: bool,
    pub cap_truncation_deactivation_guard_reason: Option<&'static str>,
    pub cap_truncation_deactivation_guard_started_at: Option<DateTime<Utc>>,
    pub cap_truncation_floor_ts_utc: Option<DateTime<Utc>>,
    pub cap_truncation_floor_signature: Option<String>,
    pub persisted_stream_catch_up_requested: bool,
    pub persisted_stream_catch_up_pressure_override_requested: bool,
    pub wallet_freshness_capture_state: Option<&'static str>,
    pub wallet_freshness_capture_reason: Option<String>,
    pub wallet_freshness_capture_id: Option<i64>,
    pub wallet_freshness_capture_captured_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
struct PersistedStreamSnapshotState {
    snapshots: Vec<WalletSnapshot>,
    observed_swaps_loaded: usize,
}

#[derive(Debug, Clone)]
struct WalletSnapshotOutcome {
    snapshot: WalletSnapshot,
}

fn trusted_snapshot_id(
    source_kind: TrustedSnapshotSourceKind,
    effective_window_start: DateTime<Utc>,
) -> String {
    format!(
        "wallet_metrics:{}:{}",
        source_kind.as_str(),
        effective_window_start.to_rfc3339()
    )
}

fn trusted_snapshot_write(
    source_kind: TrustedSnapshotSourceKind,
    trust_state: TrustedSelectionState,
    effective_window_start: DateTime<Utc>,
    created_at: DateTime<Utc>,
    row_count: usize,
    source_snapshot_id: Option<String>,
    source_window_start: Option<DateTime<Utc>>,
) -> TrustedWalletMetricsSnapshotWrite {
    TrustedWalletMetricsSnapshotWrite {
        snapshot_id: trusted_snapshot_id(source_kind, effective_window_start),
        source_snapshot_id,
        source_window_start,
        effective_window_start,
        created_at,
        source_kind,
        row_count,
        trust_state,
    }
}

impl DiscoverySummary {
    fn with_runtime_mode(mut self, runtime_mode: DiscoveryRuntimeMode) -> Self {
        self.runtime_mode = runtime_mode;
        self.trusted_selection_fail_closed = matches!(
            runtime_mode,
            DiscoveryRuntimeMode::FailClosed | DiscoveryRuntimeMode::BootstrapDegraded
        );
        self
    }

    fn with_scoring_source(mut self, scoring_source: &'static str) -> Self {
        self.scoring_source = scoring_source;
        self
    }

    fn with_cap_truncation_telemetry(mut self, telemetry: &CapTruncationTelemetrySnapshot) -> Self {
        self.raw_window_cap_truncated = telemetry.raw_window_cap_truncated;
        self.cap_truncation_deactivation_guard_active =
            telemetry.cap_truncation_deactivation_guard_active;
        self.cap_truncation_deactivation_guard_reason =
            telemetry.cap_truncation_deactivation_guard_reason;
        self.cap_truncation_deactivation_guard_started_at =
            telemetry.cap_truncation_deactivation_guard_started_at;
        self.cap_truncation_floor_ts_utc = telemetry.cap_truncation_floor_ts_utc;
        self.cap_truncation_floor_signature = telemetry.cap_truncation_floor_signature.clone();
        self
    }

    fn with_persisted_stream_catch_up_requested(mut self, requested: bool) -> Self {
        self.persisted_stream_catch_up_requested = requested;
        self
    }

    fn with_persisted_stream_catch_up_pressure_override_requested(
        mut self,
        requested: bool,
    ) -> Self {
        self.persisted_stream_catch_up_pressure_override_requested = requested;
        self
    }

    fn with_wallet_freshness_capture(
        mut self,
        telemetry: &InBandWalletFreshnessCaptureTelemetry,
    ) -> Self {
        self.wallet_freshness_capture_state = Some(telemetry.state);
        self.wallet_freshness_capture_reason = telemetry.reason.clone();
        self.wallet_freshness_capture_id = telemetry.capture_id;
        self.wallet_freshness_capture_captured_at = telemetry.captured_at;
        self
    }
}

#[derive(Debug, Clone)]
pub struct RuntimePublishedUniverseTruth {
    pub runtime_mode: DiscoveryRuntimeMode,
    pub reason: String,
    pub last_published_at: DateTime<Utc>,
    pub last_published_window_start: DateTime<Utc>,
    pub published_scoring_source: Option<String>,
    pub published_wallet_ids: Vec<String>,
}

impl RuntimePublishedUniverseTruth {
    pub fn active_wallets(&self) -> HashSet<String> {
        self.published_wallet_ids.iter().cloned().collect()
    }
}

#[derive(Debug, Clone)]
pub enum RuntimePublicationTruthResolution {
    Recent(RuntimePublishedUniverseTruth),
    BootstrapDegraded(RuntimePublishedUniverseTruth),
}

#[derive(Debug, Clone)]
pub struct DiscoveryPublicationTruthRepairTelemetry {
    pub state: &'static str,
    pub reason: Option<String>,
    pub required_window_start: DateTime<Utc>,
    pub journal_covered_since: Option<DateTime<Utc>>,
    pub journal_covers_runtime_cursor: bool,
    pub publication_state_exists_before: bool,
    pub publication_truth_complete_before: bool,
    pub publication_truth_fresh_before: bool,
    pub runtime_cursor_exists_before: bool,
    pub journal_store_exists: bool,
    pub runtime_window_complete_before: bool,
    pub runtime_window_complete_after: bool,
    pub runtime_window_first_cursor: Option<DiscoveryRuntimeCursor>,
    pub replay_until_cursor: Option<DiscoveryRuntimeCursor>,
    pub replay_batches_completed: usize,
    pub replay_rows_loaded: usize,
    pub replay_rows_inserted: usize,
    pub replay_time_budget_exhausted: bool,
    pub publication_truth_refresh_attempted: bool,
    pub publication_truth_refresh_completed: bool,
    pub publication_truth_refresh_phase: Option<&'static str>,
    pub publication_truth_refresh_replay_subphase: Option<&'static str>,
    pub publication_truth_refresh_replay_wallet_stats_complete: bool,
    pub publication_truth_refresh_replay_wallet_stats_wallet_cursor: Option<String>,
    pub publication_truth_refresh_delegated_to_runtime_cycle: bool,
    pub publication_truth_refresh_priority_recovery_contract_reason: Option<&'static str>,
    pub publication_truth_refresh_publishable_checkpoint_blocker: Option<&'static str>,
    pub publication_truth_refresh_effective_time_budget_ms: Option<u64>,
    pub publication_truth_refresh_collect_buy_mints_phase_page_limit: Option<usize>,
    pub publication_truth_refresh_replay_wallet_stats_phase_page_limit: Option<usize>,
    pub publication_truth_refresh_replay_sol_leg_phase_page_limit: Option<usize>,
    pub publication_truth_refresh_observed_swaps_loaded: usize,
    pub publication_truth_refresh_replay_rows_processed: usize,
    pub publication_truth_refresh_replay_pages_processed: usize,
    pub publication_truth_refresh_wallets_buffered: usize,
    pub publication_truth_refresh_cycle_rows_processed: usize,
    pub publication_truth_refresh_cycle_pages_processed: usize,
    pub publication_truth_refresh_budget_exhausted_reason: Option<&'static str>,
    pub publication_truth_refresh_helper_write_attempted: bool,
    pub publication_truth_refresh_helper_write_succeeded: bool,
    pub publication_truth_refresh_helper_write_resulting_reason: Option<String>,
    pub publication_truth_refresh_helper_write_resulting_updated_at: Option<DateTime<Utc>>,
    pub publication_truth_refresh_resume_exact_target_surface_repair_attempted: bool,
    pub publication_truth_refresh_resume_exact_target_surface_repair_completed: bool,
    pub publication_truth_refresh_resume_exact_target_surface_repair_time_budget_exhausted: bool,
    pub publication_truth_refresh_resume_exact_target_surface_repair_wallet_pages: usize,
    pub publication_truth_refresh_resume_exact_target_surface_repair_wallet_rows: usize,
    pub publication_truth_refresh_resume_exact_target_surface_repair_target_buy_mints_restored:
        usize,
}
