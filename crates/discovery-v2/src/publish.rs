use crate::status::{DiscoveryV2Status, DISCOVERY_V2_SCORING_SOURCE};
use anyhow::{bail, Result};
use copybot_core_types::{WalletMetricRow, WalletUpsertRow};
use copybot_storage_core::{
    DiscoveryPublicationStateUpdate, DiscoveryRuntimeMode, SqliteDiscoveryStore,
};
use serde::{Deserialize, Serialize};

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
    pub status: DiscoveryV2Status,
}

pub fn publish_discovery_v2_status(
    store: &SqliteDiscoveryStore,
    status: DiscoveryV2Status,
    commit: bool,
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
        store.persist_discovery_v2_publication(
            &wallet_rows(&status),
            &metric_rows(&status),
            &status.candidate_wallets,
            status.now,
            reason,
            &update,
            status.policy_fingerprint.as_str(),
            &runtime_cursor,
        )?;
    }
    Ok(DiscoveryV2PublishReport {
        dry_run: !commit,
        committed: commit,
        live_daemon_follow_surface_action: if commit {
            "restart_or_reload_copybot_app_before_live_follow_surface_uses_publication".to_string()
        } else {
            "none_for_dry_run".to_string()
        },
        runtime_mode: runtime_mode.as_str().to_string(),
        reason: reason.to_string(),
        scoring_source: DISCOVERY_V2_SCORING_SOURCE.to_string(),
        policy_fingerprint: status.policy_fingerprint.clone(),
        published_wallet_count: if status.production_green {
            status.candidate_wallets.len()
        } else {
            0
        },
        status,
    })
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
