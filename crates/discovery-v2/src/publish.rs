use crate::status::{DiscoveryV2Status, DISCOVERY_V2_SCORING_SOURCE};
use anyhow::{bail, Result};
use chrono::Duration;
use copybot_core_types::{WalletMetricRow, WalletUpsertRow};
use copybot_storage_core::{
    DiscoveryPublicationStateUpdate, DiscoveryRuntimeMode, RugWalletQuarantineUpsert,
    SqliteDiscoveryStore,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2PublishReport {
    pub dry_run: bool,
    pub committed: bool,
    pub live_daemon_follow_surface_action: String,
    pub runtime_mode: String,
    pub reason: String,
    pub scoring_source: String,
    pub policy_fingerprint: String,
    pub published_wallet_count: usize,
    pub rug_quarantine_wallet_count: usize,
    pub publication_rotation: DiscoveryV2PublicationRotationReport,
    pub status: DiscoveryV2Status,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryV2PublicationRotationReport {
    pub previous_active_wallet_count: usize,
    pub desired_wallet_count: usize,
    pub retained_wallet_count: usize,
    pub added_wallet_count: usize,
    pub removed_wallet_count: usize,
}

pub fn publish_discovery_v2_status(
    store: &SqliteDiscoveryStore,
    status: DiscoveryV2Status,
    commit: bool,
    rug_quarantine_hours: u64,
) -> Result<DiscoveryV2PublishReport> {
    let runtime_mode = if status.production_green {
        DiscoveryRuntimeMode::Healthy
    } else {
        DiscoveryRuntimeMode::FailClosed
    };
    let reason = if status.production_green {
        "discovery_v2_operational_window_ready"
    } else {
        "discovery_v2_operational_window_blocked"
    };
    let publishable_wallets = if status.production_green {
        status.candidate_wallets.as_slice()
    } else {
        &[]
    };
    let mut publication_rotation = publication_rotation_report(store, publishable_wallets)?;
    let rug_quarantines = rug_quarantine_rows(&status, rug_quarantine_hours);
    if commit {
        if !status.production_green {
            bail!(
                "discovery v2 publication is blocked; refusing to mutate publication state: {}",
                status.blockers.join(",")
            );
        }
        let runtime_cursor = status
            .tail
            .as_ref()
            .map(|tail| tail.cursor.clone())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "discovery v2 publication requires a fresh persisted runtime cursor"
                )
            })?;
        let update = DiscoveryPublicationStateUpdate {
            runtime_mode,
            reason: reason.to_string(),
            last_published_at: Some(status.now),
            last_published_window_start: Some(status.window_start),
            published_scoring_source: Some(DISCOVERY_V2_SCORING_SOURCE.to_string()),
            published_wallet_ids: Some(status.candidate_wallets.clone()),
        };
        let candidate_sources = candidate_source_rows(&status);
        let followlist_update = store.persist_discovery_v2_publication(
            &wallet_rows(&status),
            &metric_rows(&status),
            &status.candidate_wallets,
            Some(&candidate_sources),
            status.now,
            reason,
            &update,
            status.policy_fingerprint.as_str(),
            &runtime_cursor,
            Some(crate::rug_feedback::RUG_FEEDBACK_REJECT_REASON),
            &rug_quarantines,
        )?;
        publication_rotation.added_wallet_count = followlist_update.activated;
        publication_rotation.removed_wallet_count = followlist_update.deactivated;
    }
    let policy_fingerprint = status.policy_fingerprint.clone();
    let published_wallet_count = if status.production_green {
        status.candidate_wallets.len()
    } else {
        0
    };
    Ok(DiscoveryV2PublishReport {
        dry_run: !commit,
        committed: commit,
        live_daemon_follow_surface_action: if commit {
            "copybot_app_live_reload_will_pick_up_publication_without_restart".to_string()
        } else {
            "none_for_dry_run".to_string()
        },
        runtime_mode: runtime_mode.as_str().to_string(),
        reason: reason.to_string(),
        scoring_source: DISCOVERY_V2_SCORING_SOURCE.to_string(),
        policy_fingerprint,
        published_wallet_count,
        rug_quarantine_wallet_count: rug_quarantines.len(),
        publication_rotation,
        status: status.bounded_operator_wallet_metrics(),
    })
}

fn candidate_source_rows(status: &DiscoveryV2Status) -> Vec<(String, String)> {
    status
        .candidate_wallet_sources
        .iter()
        .map(|source| (source.wallet_id.clone(), source.source_cohort.clone()))
        .collect()
}

fn publication_rotation_report(
    store: &SqliteDiscoveryStore,
    desired_wallets: &[String],
) -> Result<DiscoveryV2PublicationRotationReport> {
    let current = store.list_active_follow_wallets()?;
    Ok(publication_rotation_from_current(&current, desired_wallets))
}

fn publication_rotation_from_current(
    current: &HashSet<String>,
    desired_wallets: &[String],
) -> DiscoveryV2PublicationRotationReport {
    let desired = desired_wallets.iter().cloned().collect::<HashSet<_>>();
    let retained_wallet_count = desired.intersection(current).count();
    DiscoveryV2PublicationRotationReport {
        previous_active_wallet_count: current.len(),
        desired_wallet_count: desired.len(),
        retained_wallet_count,
        added_wallet_count: desired.difference(current).count(),
        removed_wallet_count: current.difference(&desired).count(),
    }
}

fn wallet_rows(status: &DiscoveryV2Status) -> Vec<WalletUpsertRow> {
    status
        .wallet_metrics
        .iter()
        .map(|metric| WalletUpsertRow {
            wallet_id: metric.wallet_id.clone(),
            first_seen: metric.first_seen,
            last_seen: metric.last_seen,
            status: "active".to_string(),
        })
        .collect()
}

fn metric_rows(status: &DiscoveryV2Status) -> Vec<WalletMetricRow> {
    status
        .wallet_metrics
        .iter()
        .map(|metric| WalletMetricRow {
            wallet_id: metric.wallet_id.clone(),
            window_start: status.window_start,
            pnl: metric.pnl_sol,
            win_rate: metric.win_rate,
            trades: metric.trades,
            closed_trades: metric.closed_trades,
            hold_median_seconds: metric.hold_median_seconds,
            score: metric.score,
            buy_total: metric.buy_total,
            tradable_ratio: metric.tradable_ratio,
            rug_ratio: metric.rug_ratio,
        })
        .collect()
}

fn rug_quarantine_rows(
    status: &DiscoveryV2Status,
    quarantine_hours: u64,
) -> Vec<RugWalletQuarantineUpsert> {
    let quarantine_until = status.now + Duration::hours(quarantine_hours.max(1) as i64);
    let mut rows = BTreeMap::<String, RugWalletQuarantineUpsert>::new();
    for candidate in &status.rug_quarantine_candidates {
        let evidence_json = serde_json::json!({
            "source": "discovery_v2_rug_wallet_filter",
            "closed_trades": candidate.closed_trades,
            "stale_terminal_closes": candidate.stale_terminal_closes,
            "stale_terminal_rate": candidate.stale_terminal_rate,
            "stale_terminal_pnl_sol": candidate.stale_terminal_pnl_sol,
        })
        .to_string();
        rows.insert(
            candidate.wallet_id.clone(),
            RugWalletQuarantineUpsert {
                wallet_id: candidate.wallet_id.clone(),
                reason: crate::rug_feedback::RUG_FEEDBACK_REJECT_REASON.to_string(),
                rejected_at: status.now,
                quarantine_until,
                evidence_json,
            },
        );
    }
    rows.into_values().collect()
}
