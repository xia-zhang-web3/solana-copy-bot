use crate::{DiscoveryPublicationFreshnessGate, DiscoveryPublicationStateRow};
use chrono::{DateTime, Utc};

fn format_optional_export_ts(value: Option<DateTime<Utc>>) -> String {
    value
        .map(|ts| ts.to_rfc3339())
        .unwrap_or_else(|| "null".to_string())
}

fn incomplete_publication_truth_fields(
    publication_state: &DiscoveryPublicationStateRow,
) -> Vec<&'static str> {
    let mut missing_fields = Vec::new();
    if publication_state.last_published_at.is_none() {
        missing_fields.push("last_published_at");
    }
    if publication_state.last_published_window_start.is_none() {
        missing_fields.push("last_published_window_start");
    }
    if !publication_state
        .published_wallet_ids
        .as_ref()
        .is_some_and(|wallet_ids| !wallet_ids.is_empty())
    {
        missing_fields.push("published_wallet_ids");
    }
    missing_fields
}

pub(crate) fn runtime_artifact_export_truth_detail(
    publication_state: &DiscoveryPublicationStateRow,
    gate: &DiscoveryPublicationFreshnessGate,
    now: DateTime<Utc>,
) -> String {
    let missing_fields = incomplete_publication_truth_fields(publication_state);
    format!(
        "runtime_mode={} reason={} complete={} fresh_under_export_gate={} last_published_at={} last_published_window_start={} published_wallet_count={} updated_at={} gate_scoring_window_days={} gate_metric_snapshot_interval_seconds={} gate_refresh_seconds={} missing_fields={}",
        publication_state.runtime_mode.as_str(),
        publication_state.reason,
        publication_state.has_complete_publication_truth(),
        publication_state.is_fresh_under_gate(gate, now),
        format_optional_export_ts(publication_state.last_published_at),
        format_optional_export_ts(publication_state.last_published_window_start),
        publication_state
            .published_wallet_ids
            .as_ref()
            .map(Vec::len)
            .unwrap_or(0),
        publication_state.updated_at.to_rfc3339(),
        gate.scoring_window_days,
        gate.metric_snapshot_interval_seconds,
        gate.refresh_seconds,
        if missing_fields.is_empty() {
            "none".to_string()
        } else {
            missing_fields.join(",")
        }
    )
}
