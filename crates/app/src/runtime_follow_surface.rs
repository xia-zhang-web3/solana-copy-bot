use crate::*;

#[cfg(test)]
pub(crate) fn follow_event_retention_duration(
    shadow_max_signal_lag_seconds: u64,
    discovery_refresh_seconds: u64,
) -> Duration {
    Duration::from_secs(
        shadow_max_signal_lag_seconds
            .max(discovery_refresh_seconds.max(1).saturating_mul(2))
            .max(1),
    )
}

pub(crate) fn startup_follow_snapshot_from_publication_truth(
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

pub(crate) fn startup_runtime_publication_truth(
    discovery: &DiscoveryService,
    sqlite_path: &str,
    now: DateTime<Utc>,
    execution_enabled: bool,
) -> Result<Option<RuntimePublicationTruthResolution>> {
    let store = copybot_storage_core::SqliteDiscoveryStore::open_read_only(sqlite_path)?;
    let Some(resolution) = discovery.runtime_publication_truth_resolution(&store, now)? else {
        return Ok(None);
    };
    let Some(publication_state) = store.discovery_publication_state_read_only()? else {
        return Ok(None);
    };
    let Some(current_cursor) = store.load_discovery_runtime_cursor()? else {
        return Ok(None);
    };
    let expected_policy_fingerprint =
        discovery.discovery_v2_publication_policy_fingerprint(execution_enabled);
    if runtime_publication_truth_has_current_v2_identity(
        &resolution,
        &publication_state,
        &current_cursor,
        expected_policy_fingerprint.as_str(),
    ) {
        Ok(Some(resolution))
    } else {
        Ok(None)
    }
}

fn runtime_publication_truth_has_current_v2_identity(
    resolution: &RuntimePublicationTruthResolution,
    publication_state: &copybot_storage_core::DiscoveryPublicationStateRow,
    current_cursor: &copybot_storage_core::DiscoveryRuntimeCursor,
    expected_policy_fingerprint: &str,
) -> bool {
    const V2_SOURCE: &str = "discovery_v2_operational_window";
    let RuntimePublicationTruthResolution::Recent(truth) = resolution else {
        return false;
    };
    truth.runtime_mode == copybot_storage_core::DiscoveryRuntimeMode::Healthy
        && publication_state.runtime_mode == copybot_storage_core::DiscoveryRuntimeMode::Healthy
        && truth
            .published_scoring_source
            .as_deref()
            .is_some_and(|source| source == V2_SOURCE)
        && publication_state
            .published_scoring_source
            .as_deref()
            .is_some_and(|source| source == V2_SOURCE)
        && publication_state
            .publication_policy_fingerprint
            .as_deref()
            .is_some_and(|fingerprint| fingerprint == expected_policy_fingerprint)
        && publication_state.publication_runtime_cursor.as_ref() == Some(current_cursor)
}
