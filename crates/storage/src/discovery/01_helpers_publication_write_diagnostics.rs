use super::canonicalize_wallet_ids;
use crate::{DiscoveryPublicationStateRow, DiscoveryPublicationStateUpdate, DiscoveryRuntimeMode};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiscoveryPublicationStateWriteDiagnostics {
    pub write_kind: &'static str,
    pub previous_last_published_at: Option<DateTime<Utc>>,
    pub new_last_published_at: Option<DateTime<Utc>>,
    pub previous_published_wallet_count: usize,
    pub new_published_wallet_count: usize,
    pub published_universe_persisted: bool,
    pub runtime_mode: DiscoveryRuntimeMode,
    pub reason: String,
    pub stale_fields_carried_forward: bool,
    pub stale_last_published_at_carried_forward: bool,
    pub stale_published_wallet_ids_carried_forward: bool,
    pub updated_at: DateTime<Utc>,
}

pub(crate) fn snapshot_discovery_publication_state_write_diagnostics(
    previous: Option<&DiscoveryPublicationStateRow>,
    new_state: &DiscoveryPublicationStateRow,
    update: &DiscoveryPublicationStateUpdate,
    clear_published_truth: bool,
) -> DiscoveryPublicationStateWriteDiagnostics {
    let previous_last_published_at = previous.and_then(|state| state.last_published_at);
    let previous_published_wallet_ids =
        previous.and_then(|state| state.published_wallet_ids.clone());
    let previous_published_wallet_count = previous_published_wallet_ids
        .as_ref()
        .map(Vec::len)
        .unwrap_or(0);
    let new_published_wallet_count = new_state
        .published_wallet_ids
        .as_ref()
        .map(Vec::len)
        .unwrap_or(0);
    let stale_last_published_at_carried_forward = !clear_published_truth
        && update.last_published_at.is_none()
        && previous_last_published_at.is_some()
        && new_state.last_published_at == previous_last_published_at;
    let stale_published_wallet_ids_carried_forward = !clear_published_truth
        && update.published_wallet_ids.is_none()
        && previous_published_wallet_ids.is_some()
        && new_state.published_wallet_ids == previous_published_wallet_ids;
    let stale_fields_carried_forward =
        stale_last_published_at_carried_forward || stale_published_wallet_ids_carried_forward;
    let published_universe_persisted = !clear_published_truth
        && new_state.runtime_mode == DiscoveryRuntimeMode::Healthy
        && update.runtime_mode == DiscoveryRuntimeMode::Healthy
        && update.last_published_at.is_some()
        && update.last_published_window_start.is_some()
        && update
            .published_wallet_ids
            .as_ref()
            .is_some_and(|wallet_ids| !canonicalize_wallet_ids(wallet_ids).is_empty());
    let write_kind = if clear_published_truth {
        "clear_published_truth"
    } else if published_universe_persisted {
        "fresh_publish"
    } else if stale_fields_carried_forward {
        "carried_forward_stale_truth"
    } else {
        "runtime_only_refresh"
    };
    DiscoveryPublicationStateWriteDiagnostics {
        write_kind,
        previous_last_published_at,
        new_last_published_at: new_state.last_published_at,
        previous_published_wallet_count,
        new_published_wallet_count,
        published_universe_persisted,
        runtime_mode: new_state.runtime_mode,
        reason: new_state.reason.clone(),
        stale_fields_carried_forward,
        stale_last_published_at_carried_forward,
        stale_published_wallet_ids_carried_forward,
        updated_at: new_state.updated_at,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeArtifactRestoreDirtyTable {
    pub table: String,
    pub category: &'static str,
}
