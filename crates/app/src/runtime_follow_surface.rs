use crate::discovery_runtime::RuntimePublishedUniverseTruth;
use crate::*;
use copybot_config::DISCOVERY_V2_SCORING_SOURCE;
use std::collections::HashSet;

pub(crate) struct RuntimeFollowReload {
    pub(crate) follow_snapshot: FollowSnapshot,
    pub(crate) shadow_strategy_fail_closed: bool,
    pub(crate) active_follow_wallets: usize,
    pub(crate) added_wallets: usize,
    pub(crate) removed_wallets: usize,
    pub(crate) source: &'static str,
}

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
    (FollowSnapshot::default(), recovered_active_wallets, true)
}

pub(crate) fn startup_runtime_publication_truth(
    discovery: &DiscoveryService,
    sqlite_path: &str,
    now: DateTime<Utc>,
) -> Result<Option<RuntimePublicationTruthResolution>> {
    let store = copybot_storage_core::SqliteDiscoveryStore::open_read_only(sqlite_path)?;
    if let Err(error) = copybot_storage_core::validate_discovery_v2_schema_read_only(&store) {
        warn!(
            error = %error,
            "startup V2 publication truth rejected because storage schema is not ready; runtime follow surface remains fail-closed"
        );
        return Ok(None);
    }
    let Some(publication_state) = store.discovery_publication_state_read_only()? else {
        return Ok(None);
    };
    let Some(current_cursor) = store.load_discovery_runtime_cursor()? else {
        return Ok(None);
    };
    let freshness_gate = discovery.publication_freshness_gate();
    let expected_policy_fingerprint = discovery.discovery_v2_publication_policy_fingerprint(false);
    if !publication_state.is_fresh_under_gate(&freshness_gate, now)
        || !publication_state.has_fresh_publication_runtime_cursor_under_gate(&freshness_gate, now)
        || !publication_truth_has_current_v2_identity(
            &publication_state,
            &current_cursor,
            expected_policy_fingerprint.as_str(),
        )
    {
        return Ok(None);
    }
    let Some(truth) = RuntimePublishedUniverseTruth::from_state(publication_state) else {
        return Ok(None);
    };
    Ok(Some(RuntimePublicationTruthResolution::Recent(truth)))
}

pub(crate) fn runtime_follow_reload_from_publication_truth(
    current: &FollowSnapshot,
    current_shadow_strategy_fail_closed: bool,
    runtime_publication_truth: Option<&RuntimePublicationTruthResolution>,
    now: DateTime<Utc>,
) -> Option<RuntimeFollowReload> {
    let next_active = match runtime_publication_truth {
        Some(RuntimePublicationTruthResolution::Recent(truth)) => truth.active_wallets(),
        Some(RuntimePublicationTruthResolution::BootstrapDegraded(_)) | None => HashSet::new(),
    };
    let next_shadow_strategy_fail_closed = runtime_publication_truth.is_none()
        || matches!(
            runtime_publication_truth,
            Some(RuntimePublicationTruthResolution::BootstrapDegraded(_))
        );
    if current.active == next_active
        && current_shadow_strategy_fail_closed == next_shadow_strategy_fail_closed
    {
        return None;
    }

    let added_wallets = next_active.difference(&current.active).count();
    let removed_wallets = current.active.difference(&next_active).count();
    let mut next = current.clone();
    for wallet_id in next_active.difference(&current.active) {
        next.promoted_at.insert(wallet_id.clone(), now);
    }
    for wallet_id in current.active.difference(&next_active) {
        next.demoted_at.insert(wallet_id.clone(), now);
    }
    next.active = next_active;
    Some(RuntimeFollowReload {
        active_follow_wallets: next.active.len(),
        follow_snapshot: next,
        shadow_strategy_fail_closed: next_shadow_strategy_fail_closed,
        added_wallets,
        removed_wallets,
        source: if runtime_publication_truth.is_some() {
            "discovery_v2_publication"
        } else {
            "fail_closed_missing_current_publication"
        },
    })
}

fn publication_truth_has_current_v2_identity(
    publication_state: &copybot_storage_core::DiscoveryPublicationStateRow,
    current_cursor: &copybot_storage_core::DiscoveryRuntimeCursor,
    expected_policy_fingerprint: &str,
) -> bool {
    publication_state.runtime_mode == copybot_storage_core::DiscoveryRuntimeMode::Healthy
        && publication_state
            .published_scoring_source
            .as_deref()
            .is_some_and(|source| source == DISCOVERY_V2_SCORING_SOURCE)
        && publication_state
            .publication_policy_fingerprint
            .as_deref()
            .is_some_and(|fingerprint| fingerprint == expected_policy_fingerprint)
        && publication_state.publication_runtime_cursor.as_ref() == Some(current_cursor)
}
