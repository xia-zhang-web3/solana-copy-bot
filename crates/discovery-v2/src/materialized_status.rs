use crate::policy::{discovery_v2_policy_fingerprint, DiscoveryV2BuildOptions};
use crate::status::{build_discovery_v2_status, DiscoveryV2Status, DISCOVERY_V2_SCORING_SOURCE};
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_storage_core::{DiscoveryPublicationFreshnessGate, SqliteDiscoveryStore};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2MaterializedStatusReport {
    pub committed: bool,
    pub reused_existing_snapshot: bool,
    pub source: String,
    pub status_now: DateTime<Utc>,
    pub status_age_seconds: i64,
    pub refresh_after_age_seconds: u64,
    pub reuse_before_age_seconds: u64,
    pub rebuild_after_age_seconds: u64,
    pub max_status_age_seconds: u64,
    pub policy_fingerprint: String,
    pub production_green: bool,
    pub candidate_wallet_count: usize,
    pub wallet_metrics_returned: usize,
    pub blockers: Vec<String>,
}

pub fn materialize_discovery_v2_status(
    store: &SqliteDiscoveryStore,
    discovery: &DiscoveryConfig,
    shadow: &ShadowConfig,
    options: DiscoveryV2BuildOptions,
) -> Result<(DiscoveryV2Status, DiscoveryV2MaterializedStatusReport)> {
    let status = build_discovery_v2_status(store, discovery, shadow, options.clone())?;
    let runtime_cursor = status.tail.as_ref().map(|tail| &tail.cursor);
    let status_json = serde_json::to_string(&status)
        .context("failed serializing discovery v2 status snapshot")?;
    store.persist_discovery_v2_status_snapshot(
        status.policy_fingerprint.as_str(),
        status.now,
        status.window_start,
        runtime_cursor,
        status_json.as_str(),
    )?;
    let report = materialized_status_report(&status, discovery, options.now, true);
    Ok((status, report))
}

pub fn reusable_materialized_discovery_v2_status_for_prepare(
    store: &SqliteDiscoveryStore,
    discovery: &DiscoveryConfig,
    shadow: &ShadowConfig,
    options: &DiscoveryV2BuildOptions,
) -> Result<Option<DiscoveryV2MaterializedStatusReport>> {
    let Ok((status, mut report)) =
        load_materialized_discovery_v2_status_for_publish(store, discovery, shadow, options)
    else {
        return Ok(None);
    };
    if !status.production_green {
        return Ok(None);
    }
    let reuse_before_age_seconds = materialized_status_reuse_before_age_seconds(discovery);
    if report.status_age_seconds < 0
        || report.status_age_seconds >= reuse_before_age_seconds.min(i64::MAX as u64) as i64
    {
        return Ok(None);
    }
    report.committed = false;
    report.reused_existing_snapshot = true;
    Ok(Some(report))
}

pub fn load_materialized_discovery_v2_status_for_publish(
    store: &SqliteDiscoveryStore,
    discovery: &DiscoveryConfig,
    shadow: &ShadowConfig,
    options: &DiscoveryV2BuildOptions,
) -> Result<(DiscoveryV2Status, DiscoveryV2MaterializedStatusReport)> {
    let expected_policy_fingerprint = discovery_v2_policy_fingerprint(discovery, shadow, options);
    let row = store
        .discovery_v2_status_snapshot_read_only()?
        .ok_or_else(|| anyhow::anyhow!("discovery v2 materialized status snapshot is missing"))?;
    if row.policy_fingerprint != expected_policy_fingerprint {
        bail!(
            "discovery v2 materialized status policy mismatch: snapshot={} expected={}",
            row.policy_fingerprint,
            expected_policy_fingerprint
        );
    }
    let status: DiscoveryV2Status = serde_json::from_str(&row.status_json)
        .context("failed parsing discovery v2 materialized status json")?;
    if status.source != DISCOVERY_V2_SCORING_SOURCE {
        bail!(
            "discovery v2 materialized status has unexpected source: {}",
            status.source
        );
    }
    if status.policy_fingerprint != expected_policy_fingerprint {
        bail!(
            "discovery v2 materialized status embedded policy mismatch: status={} expected={}",
            status.policy_fingerprint,
            expected_policy_fingerprint
        );
    }
    if row.status_now != status.now || row.status_window_start != status.window_start {
        bail!("discovery v2 materialized status metadata does not match embedded status");
    }
    match (&row.runtime_cursor, status.tail.as_ref()) {
        (Some(row_cursor), Some(tail)) if row_cursor == &tail.cursor => {}
        (None, None) => {}
        _ => bail!("discovery v2 materialized status runtime cursor mismatch"),
    }
    validate_status_identity(&status, options)?;
    validate_status_age(&status, discovery, options.now)?;
    let report = materialized_status_report(&status, discovery, options.now, false);
    Ok((status, report))
}

fn validate_status_identity(
    status: &DiscoveryV2Status,
    options: &DiscoveryV2BuildOptions,
) -> Result<()> {
    if status.window_minutes != options.window_minutes {
        bail!(
            "discovery v2 materialized status window mismatch: status={} expected={}",
            status.window_minutes,
            options.window_minutes
        );
    }
    if status.max_tail_lag_seconds != options.max_tail_lag_seconds {
        bail!(
            "discovery v2 materialized status tail-lag policy mismatch: status={} expected={}",
            status.max_tail_lag_seconds,
            options.max_tail_lag_seconds
        );
    }
    if status.execution_enabled != options.execution_enabled {
        bail!("discovery v2 materialized status execution identity mismatch");
    }
    if status.execution_disabled == status.execution_enabled {
        bail!("discovery v2 materialized status has inconsistent execution flags");
    }
    Ok(())
}

fn validate_status_age(
    status: &DiscoveryV2Status,
    discovery: &DiscoveryConfig,
    now: DateTime<Utc>,
) -> Result<()> {
    let age = now.signed_duration_since(status.now);
    if age < Duration::zero() {
        bail!("discovery v2 materialized status is future-dated");
    }
    let max_age = materialized_status_max_age_seconds(discovery);
    if age.num_seconds() > max_age.min(i64::MAX as u64) as i64 {
        bail!(
            "discovery v2 materialized status is stale: age_seconds={} max_status_age_seconds={}",
            age.num_seconds(),
            max_age
        );
    }
    Ok(())
}

fn materialized_status_report(
    status: &DiscoveryV2Status,
    discovery: &DiscoveryConfig,
    now: DateTime<Utc>,
    committed: bool,
) -> DiscoveryV2MaterializedStatusReport {
    DiscoveryV2MaterializedStatusReport {
        committed,
        reused_existing_snapshot: false,
        source: status.source.clone(),
        status_now: status.now,
        status_age_seconds: now.signed_duration_since(status.now).num_seconds(),
        refresh_after_age_seconds: materialized_status_refresh_after_age_seconds(discovery),
        reuse_before_age_seconds: materialized_status_reuse_before_age_seconds(discovery),
        rebuild_after_age_seconds: materialized_status_rebuild_after_age_seconds(discovery),
        max_status_age_seconds: materialized_status_max_age_seconds(discovery),
        policy_fingerprint: status.policy_fingerprint.clone(),
        production_green: status.production_green,
        candidate_wallet_count: status.candidate_wallets.len(),
        wallet_metrics_returned: status.wallet_metrics_returned,
        blockers: status.blockers.clone(),
    }
}

fn materialized_status_max_age_seconds(discovery: &DiscoveryConfig) -> u64 {
    let gate = DiscoveryPublicationFreshnessGate {
        scoring_window_days: discovery.scoring_window_days as i64,
        metric_snapshot_interval_seconds: discovery.metric_snapshot_interval_seconds,
        refresh_seconds: discovery.refresh_seconds,
        expected_scoring_source: None,
        expected_policy_fingerprint: None,
    };
    let seconds = gate.published_universe_max_age().num_seconds();
    if seconds <= 0 {
        1
    } else {
        seconds as u64
    }
}

fn materialized_status_rebuild_after_age_seconds(discovery: &DiscoveryConfig) -> u64 {
    materialized_status_max_age_seconds(discovery)
}

fn materialized_status_reuse_before_age_seconds(discovery: &DiscoveryConfig) -> u64 {
    let max_age = materialized_status_rebuild_after_age_seconds(discovery).max(1);
    let refresh_after = materialized_status_refresh_after_age_seconds(discovery).min(max_age);
    let publish_cycle_margin = discovery
        .metric_snapshot_interval_seconds
        .max(discovery.refresh_seconds.max(1))
        .saturating_mul(2);
    let rebuild_margin = publish_cycle_margin.max(1).min(max_age.saturating_sub(1));
    max_age
        .saturating_sub(rebuild_margin)
        .max(refresh_after)
        .min(max_age)
        .max(1)
}

fn materialized_status_refresh_after_age_seconds(discovery: &DiscoveryConfig) -> u64 {
    discovery
        .metric_snapshot_interval_seconds
        .max(discovery.refresh_seconds.max(1))
}
