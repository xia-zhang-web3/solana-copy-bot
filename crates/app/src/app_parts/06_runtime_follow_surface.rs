fn sanitize_json_value(value: &str) -> String {
    let json_string = serde_json::Value::String(value.to_string()).to_string();
    json_string[1..json_string.len().saturating_sub(1)].to_string()
}

fn follow_event_retention_duration(
    shadow_max_signal_lag_seconds: u64,
    discovery_refresh_seconds: u64,
) -> Duration {
    Duration::from_secs(
        shadow_max_signal_lag_seconds
            .max(discovery_refresh_seconds.max(1).saturating_mul(2))
            .max(1),
    )
}

fn startup_follow_snapshot_from_publication_truth(
    initial_active_wallets: HashSet<String>,
    runtime_publication_truth: Option<&RuntimePublicationTruthResolution>,
) -> (FollowSnapshot, usize, bool) {
    if let Some(RuntimePublicationTruthResolution::Recent(truth)) = runtime_publication_truth {
        return (
            FollowSnapshot::from_active_wallets(truth.active_wallets()),
            0,
            false,
        );
    }
    if matches!(
        runtime_publication_truth,
        Some(RuntimePublicationTruthResolution::BootstrapDegraded(_))
    ) {
        return (
            FollowSnapshot::default(),
            initial_active_wallets.len(),
            true,
        );
    }
    let recovered_active_wallets = initial_active_wallets.len();
    let shadow_strategy_fail_closed = recovered_active_wallets > 0;
    if !shadow_strategy_fail_closed {
        (FollowSnapshot::default(), 0, false)
    } else {
        (FollowSnapshot::default(), recovered_active_wallets, true)
    }
}

fn startup_runtime_publication_truth(
    discovery: &DiscoveryService,
    store: &SqliteStore,
    now: DateTime<Utc>,
) -> Result<Option<RuntimePublicationTruthResolution>> {
    discovery.runtime_publication_truth_resolution(store, now)
}

fn apply_fail_closed_runtime_follow_surface_if_needed(
    follow_snapshot: &mut Arc<FollowSnapshot>,
    open_shadow_lots: &mut HashSet<(String, String)>,
    shadow_strategy_fail_closed: &mut bool,
    discovery_output: &DiscoveryTaskOutput,
) -> bool {
    if !matches!(
        discovery_output.runtime_mode,
        DiscoveryRuntimeMode::FailClosed | DiscoveryRuntimeMode::BootstrapDegraded
    ) {
        return false;
    }
    *shadow_strategy_fail_closed = true;
    *follow_snapshot = Arc::new(FollowSnapshot::default());
    open_shadow_lots.clear();
    true
}
