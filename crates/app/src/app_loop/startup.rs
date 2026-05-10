use super::*;

pub(super) struct AppLoopStartup {
    pub(super) interval: time::Interval,
    pub(super) risk_refresh_interval: time::Interval,
    pub(super) shadow_interval: time::Interval,
    pub(super) follow_snapshot: Arc<FollowSnapshot>,
    pub(super) open_shadow_lots: HashSet<(String, String)>,
    pub(super) shadow_strategy_fail_closed: bool,
    pub(super) stale_lot_max_hold_hours: u32,
    pub(super) stale_lot_terminal_zero_price_hours: u32,
    pub(super) stale_lot_recovery_zero_price_enabled: bool,
    pub(super) shadow_risk_guard: ShadowRiskGuard,
    pub(super) shadow_drop_reason_counts: BTreeMap<&'static str, u64>,
    pub(super) shadow_drop_stage_counts: BTreeMap<&'static str, u64>,
    pub(super) shadow_queue_full_outcome_counts: BTreeMap<&'static str, u64>,
    pub(super) app_consumer_loop_telemetry: AppConsumerLoopTelemetry,
    pub(super) recent_swap_signatures: HashSet<String>,
    pub(super) recent_swap_signature_order: VecDeque<String>,
    pub(super) pending_irrelevant_swaps: VecDeque<PendingIrrelevantObservedSwap>,
    pub(super) discovery_critical_target_buy_mints: HashSet<String>,
    pub(super) discovery_critical_target_buy_mints_backpressure_refresh_state:
        DiscoveryCriticalTargetBuyMintsBackpressureRefreshState,
    pub(super) zero_universe_empty_target_noncritical_best_effort:
        ZeroUniverseEmptyTargetNoncriticalBestEffortState,
    pub(super) shadow_scheduler: ShadowScheduler,
    pub(super) observed_swap_writer: ObservedSwapWriter,
    pub(super) latest_ingestion_runtime_snapshot: Arc<Mutex<Option<IngestionRuntimeSnapshot>>>,
    pub(super) observed_swap_retention_runtime_health: ObservedSwapRetentionRuntimeHealthHandle,
    pub(super) observed_swap_retention_config: ObservedSwapRetentionConfig,
    pub(super) observed_swap_retention_sweep_interval: Duration,
    pub(super) app_started_at: StdInstant,
    pub(super) last_observed_swap_retention_sweep: StdInstant,
    pub(super) history_retention: HistoryRetentionRunner,
    pub(super) history_retention_sweep_interval: Duration,
    pub(super) last_history_retention_sweep: StdInstant,
    pub(super) last_sqlite_contention_snapshot: SqliteContentionSnapshot,
    pub(super) last_history_retention_skip_reason_key: Option<&'static str>,
    pub(super) last_observed_swap_retention_skip_reason_key: Option<&'static str>,
    pub(super) operator_emergency_stop: OperatorEmergencyStop,
    pub(super) observed_swap_retention_handle:
        Option<JoinHandle<Result<ObservedSwapRetentionMaintenanceSummary>>>,
    pub(super) ingestion_error_streak: u32,
    pub(super) ingestion_backoff_until: Option<time::Instant>,
}

pub(super) fn initialize_app_loop_startup(
    store: &SqliteStore,
    ingestion: &IngestionService,
    discovery: &DiscoveryService,
    risk_config: RiskConfig,
    sqlite_path: &str,
    heartbeat_seconds: u64,
    history_retention_config: copybot_config::HistoryRetentionConfig,
    recent_raw_journal_config: &copybot_config::RecentRawJournalConfig,
    discovery_fetch_refresh_seconds: u64,
    discovery_refresh_seconds: u64,
    observed_swaps_retention_days: u32,
    ingestion_source: String,
    shadow_refresh_seconds: u64,
    pause_new_trades_on_outage: bool,
) -> Result<AppLoopStartup> {
    info!(
        heartbeat_seconds,
        discovery_fetch_refresh_seconds,
        discovery_refresh_seconds,
        shadow_refresh_seconds,
        sqlite_path = %sqlite_path,
        "app runtime loop started"
    );

    let mut interval = time::interval(Duration::from_secs(heartbeat_seconds.max(1)));
    let mut risk_refresh_interval = time::interval(Duration::from_secs(
        RISK_DB_REFRESH_MIN_SECONDS.max(1) as u64,
    ));
    let mut shadow_interval = time::interval(Duration::from_secs(shadow_refresh_seconds.max(10)));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    risk_refresh_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    shadow_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let initial_active_wallets = store
        .list_active_follow_wallets()
        .context("failed to load active follow wallets")?;
    let startup_gate_now = Utc::now();
    let runtime_publication_truth =
        startup_runtime_publication_truth(discovery, sqlite_path, startup_gate_now)
            .context("failed to load startup published follow universe")?;
    let (initial_follow_snapshot, recovered_active_wallets, shadow_strategy_fail_closed) =
        startup_follow_snapshot_from_publication_truth(
            initial_active_wallets,
            runtime_publication_truth.as_ref(),
        );
    let follow_snapshot = Arc::new(initial_follow_snapshot);
    let open_shadow_lots = if shadow_strategy_fail_closed {
        HashSet::new()
    } else {
        store
            .list_shadow_open_pairs()
            .context("failed to load open shadow lot index")?
    };

    let stale_lot_max_hold_hours = risk_config.max_hold_hours;
    let stale_lot_terminal_zero_price_hours =
        risk_config.shadow_stale_close_terminal_zero_price_hours;
    let stale_lot_recovery_zero_price_enabled =
        risk_config.shadow_stale_close_recovery_zero_price_enabled;
    let mut shadow_risk_guard =
        ShadowRiskGuard::new_with_ingestion_source(risk_config, ingestion_source);
    shadow_risk_guard.restore_pause_from_store(store, Utc::now())?;

    let shadow_drop_reason_counts = BTreeMap::new();
    let shadow_drop_stage_counts = BTreeMap::new();
    let shadow_queue_full_outcome_counts = BTreeMap::new();
    let app_consumer_loop_telemetry = AppConsumerLoopTelemetry::default();
    let recent_swap_signatures = HashSet::new();
    let recent_swap_signature_order = VecDeque::new();
    let pending_irrelevant_swaps = VecDeque::new();
    let mut discovery_critical_target_buy_mints = HashSet::new();
    let mut discovery_critical_target_buy_mints_backpressure_refresh_state =
        DiscoveryCriticalTargetBuyMintsBackpressureRefreshState::default();
    refresh_discovery_critical_target_buy_mints_or_warn(
        store,
        &mut discovery_critical_target_buy_mints,
    )?;
    discovery_critical_target_buy_mints_backpressure_refresh_state
        .note_refresh_attempt(StdInstant::now());
    let shadow_scheduler = ShadowScheduler::new();

    let recent_raw_journal_writer_config = ObservedSwapRecentRawJournalConfig {
        sqlite_path: recent_raw_journal_config.path.clone(),
        retention_days: observed_swaps_retention_days.max(1).saturating_add(
            recent_raw_journal_config
                .retention_safety_buffer_days
                .max(1),
        ),
        writer_queue_capacity_batches: recent_raw_journal_config
            .writer_queue_capacity_batches
            .max(1),
        write_coalesce_max_batches: OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_COALESCE_MAX_BATCHES,
        overflow_capacity_batches: recent_raw_journal_config
            .writer_queue_capacity_batches
            .max(1)
            .saturating_mul(OBSERVED_SWAP_RECENT_RAW_JOURNAL_OVERFLOW_CAPACITY_MULTIPLIER),
        skip_prune_while_backlogged: true,
        skip_startup_prune: false,
    };
    let observed_swap_writer = ObservedSwapWriter::start_with_recent_raw_journal(
        sqlite_path.to_owned(),
        Some(recent_raw_journal_writer_config),
    )
    .context("failed to start observed swap writer")?;

    let latest_ingestion_runtime_snapshot = Arc::new(Mutex::new(ingestion.runtime_snapshot()));
    let observed_swap_retention_runtime_health = ObservedSwapRetentionRuntimeHealthHandle::new(
        observed_swap_writer.health_handle(),
        Arc::clone(&latest_ingestion_runtime_snapshot),
    );
    let observed_swap_retention_config =
        ObservedSwapRetentionConfig::production(observed_swaps_retention_days);
    let observed_swap_retention_sweep_interval =
        Duration::from_secs(OBSERVED_SWAP_RETENTION_SWEEP_INTERVAL.as_secs().max(1));
    let app_started_at = StdInstant::now();
    let last_observed_swap_retention_sweep = app_started_at;
    let history_retention = HistoryRetentionRunner::new(history_retention_config);
    let history_retention_sweep_interval = Duration::from_secs(history_retention.sweep_seconds());
    let last_history_retention_sweep = app_started_at;
    let last_sqlite_contention_snapshot = sqlite_contention_snapshot();
    let last_history_retention_skip_reason_key = None;
    let last_observed_swap_retention_skip_reason_key = None;
    let mut operator_emergency_stop = OperatorEmergencyStop::from_env();
    let observed_swap_retention_handle = None;
    let ingestion_error_streak = 0;
    let ingestion_backoff_until = None;
    operator_emergency_stop.refresh(store, Utc::now())?;
    info!(
        path = %operator_emergency_stop.path().display(),
        pause_new_trades_on_outage,
        "buy gate controls initialized"
    );
    info!("execution runtime quarantined");

    log_startup_follow_universe(
        runtime_publication_truth.as_ref(),
        recovered_active_wallets,
        follow_snapshot.as_ref(),
    );

    Ok(AppLoopStartup {
        interval,
        risk_refresh_interval,
        shadow_interval,
        follow_snapshot,
        open_shadow_lots,
        shadow_strategy_fail_closed,
        stale_lot_max_hold_hours,
        stale_lot_terminal_zero_price_hours,
        stale_lot_recovery_zero_price_enabled,
        shadow_risk_guard,
        shadow_drop_reason_counts,
        shadow_drop_stage_counts,
        shadow_queue_full_outcome_counts,
        app_consumer_loop_telemetry,
        recent_swap_signatures,
        recent_swap_signature_order,
        pending_irrelevant_swaps,
        discovery_critical_target_buy_mints,
        discovery_critical_target_buy_mints_backpressure_refresh_state,
        zero_universe_empty_target_noncritical_best_effort:
            ZeroUniverseEmptyTargetNoncriticalBestEffortState::default(),
        shadow_scheduler,
        observed_swap_writer,
        latest_ingestion_runtime_snapshot,
        observed_swap_retention_runtime_health,
        observed_swap_retention_config,
        observed_swap_retention_sweep_interval,
        app_started_at,
        last_observed_swap_retention_sweep,
        history_retention,
        history_retention_sweep_interval,
        last_history_retention_sweep,
        last_sqlite_contention_snapshot,
        last_history_retention_skip_reason_key,
        last_observed_swap_retention_skip_reason_key,
        operator_emergency_stop,
        observed_swap_retention_handle,
        ingestion_error_streak,
        ingestion_backoff_until,
    })
}

fn log_startup_follow_universe(
    runtime_publication_truth: Option<&RuntimePublicationTruthResolution>,
    recovered_active_wallets: usize,
    follow_snapshot: &FollowSnapshot,
) {
    if let Some(RuntimePublicationTruthResolution::Recent(truth)) = runtime_publication_truth {
        info!(
            active_follow_wallets = truth.published_wallet_ids.len(),
            "recent published follow universe loaded for startup runtime"
        );
    } else if let Some(RuntimePublicationTruthResolution::BootstrapDegraded(truth)) =
        runtime_publication_truth
    {
        info!(
            bootstrap_degraded_active_follow_wallets = truth.published_wallet_ids.len(),
            "startup loaded explicit bootstrap-degraded publication truth; shadow and live execution remain fail-closed until fresh raw truth publishes"
        );
    } else if recovered_active_wallets > 0 {
        info!(
            recovered_active_follow_wallets = recovered_active_wallets,
            "startup has no recent published follow universe; recovered historical wallets stay out of runtime until discovery publishes fresh or degraded runtime truth"
        );
    } else if !follow_snapshot.active.is_empty() {
        info!(
            active_follow_wallets = follow_snapshot.active.len(),
            "active follow wallets loaded"
        );
    }
}
