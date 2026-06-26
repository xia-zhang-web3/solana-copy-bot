use crate::{
    DiscoveryPublicationFreshnessGate, DiscoveryPublicationStateRow,
    DiscoveryPublicationStateUpdate, DiscoveryRuntimeArtifact, DiscoveryRuntimeCursor,
    DiscoveryRuntimeMode, FollowlistUpdateResult, PersistedWalletMetricSnapshotRow,
    RugWalletQuarantineUpsert, SqliteDiscoveryStore, DISCOVERY_RUNTIME_ARTIFACT_FORMAT_VERSION,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::{WalletMetricRow, WalletUpsertRow};
use rusqlite::{params, OptionalExtension};
use std::collections::HashSet;

use crate::rug_wallet_quarantine::{
    prune_expired_rug_wallet_quarantines_on_conn, upsert_rug_wallet_quarantines_on_conn,
};
use publication_artifact::{
    load_discovery_runtime_cursor_on_conn, runtime_artifact_export_truth_detail,
    validate_runtime_artifact_snapshot_shape,
};
use publication_followlist::{
    insert_metrics, replace_candidate_sources, update_followlist, upsert_wallets,
};
use publication_metrics::load_wallet_metric_snapshots_for_window_on_conn;
use publication_state::{
    publication_state_query, write_discovery_runtime_cursor_on_conn,
    write_publication_state_on_conn,
};

#[path = "publication_artifact.rs"]
mod publication_artifact;
#[path = "publication_followlist.rs"]
mod publication_followlist;
#[path = "publication_metrics.rs"]
mod publication_metrics;
#[path = "publication_state.rs"]
mod publication_state;

pub fn validate_discovery_runtime_artifact_snapshot_shape(
    artifact: &DiscoveryRuntimeArtifact,
) -> Result<()> {
    validate_runtime_artifact_snapshot_shape(artifact)
}

pub fn validate_discovery_runtime_artifact_export_readiness(
    artifact: &DiscoveryRuntimeArtifact,
    export_gate: &DiscoveryPublicationFreshnessGate,
    now: DateTime<Utc>,
) -> Result<()> {
    let truth_detail =
        runtime_artifact_export_truth_detail(&artifact.publication_state, export_gate, now);
    if artifact.publication_state.runtime_mode != DiscoveryRuntimeMode::Healthy {
        return Err(anyhow::anyhow!(
            "discovery runtime artifact requires healthy publication state ({truth_detail})"
        ));
    }
    if !artifact.publication_state.has_complete_publication_truth() {
        return Err(anyhow::anyhow!(
            "discovery runtime artifact requires complete publication truth ({truth_detail})"
        ));
    }
    if export_gate.expected_scoring_source.is_none()
        || export_gate.expected_policy_fingerprint.is_none()
        || artifact
            .publication_state
            .published_scoring_source
            .is_none()
        || artifact
            .publication_state
            .publication_policy_fingerprint
            .is_none()
        || artifact
            .publication_state
            .publication_runtime_cursor
            .is_none()
    {
        return Err(anyhow::anyhow!(
            "discovery runtime artifact requires complete publication identity ({truth_detail})"
        ));
    }
    if !artifact
        .publication_state
        .is_fresh_under_gate(export_gate, now)
    {
        return Err(anyhow::anyhow!(
            "discovery runtime artifact requires fresh publication truth under export gate ({truth_detail})"
        ));
    }
    if !artifact
        .publication_state
        .matches_expected_publication_identity(export_gate)
    {
        return Err(anyhow::anyhow!(
            "discovery runtime artifact requires expected publication identity ({truth_detail})"
        ));
    }
    if !artifact
        .publication_state
        .matches_publication_runtime_cursor(&artifact.runtime_cursor)
    {
        return Err(anyhow::anyhow!(
            "discovery runtime artifact requires publication-bound runtime cursor ({truth_detail})"
        ));
    }
    if !artifact
        .publication_state
        .has_fresh_publication_runtime_cursor_under_gate(export_gate, now)
    {
        return Err(anyhow::anyhow!(
            "discovery runtime artifact requires fresh publication-bound runtime cursor ({truth_detail})"
        ));
    }
    validate_runtime_artifact_snapshot_shape(artifact)
}

impl SqliteDiscoveryStore {
    pub fn persist_discovery_cycle(
        &self,
        wallets: &[WalletUpsertRow],
        metrics: &[WalletMetricRow],
        desired_wallets: &[String],
        allow_followlist_activate: bool,
        allow_followlist_deactivate: bool,
        now: DateTime<Utc>,
        reason: &str,
    ) -> Result<FollowlistUpdateResult> {
        crate::schema::ensure_discovery_v2_schema(self)?;
        let tx = self.conn.unchecked_transaction()?;
        upsert_wallets(&tx, wallets)?;
        insert_metrics(&tx, metrics)?;
        let result = update_followlist(
            &tx,
            desired_wallets,
            allow_followlist_activate,
            allow_followlist_deactivate,
            now,
            reason,
        )?;
        tx.commit()
            .context("failed committing discovery v2 publication transaction")?;
        Ok(result)
    }

    pub fn set_discovery_publication_state_with_options(
        &self,
        update: &DiscoveryPublicationStateUpdate,
        clear_published_truth: bool,
        policy_fingerprint: Option<&str>,
    ) -> Result<()> {
        crate::schema::ensure_discovery_strategy_state_table(self)?;
        write_publication_state_on_conn(
            &self.conn,
            update,
            clear_published_truth,
            policy_fingerprint,
            None,
        )
    }

    pub fn set_discovery_publication_state_with_identity(
        &self,
        update: &DiscoveryPublicationStateUpdate,
        clear_published_truth: bool,
        policy_fingerprint: Option<&str>,
        publication_runtime_cursor: Option<&DiscoveryRuntimeCursor>,
    ) -> Result<()> {
        crate::schema::ensure_discovery_strategy_state_table(self)?;
        write_publication_state_on_conn(
            &self.conn,
            update,
            clear_published_truth,
            policy_fingerprint,
            publication_runtime_cursor,
        )
    }

    pub fn persist_discovery_v2_publication(
        &self,
        wallets: &[WalletUpsertRow],
        metrics: &[WalletMetricRow],
        desired_wallets: &[String],
        candidate_sources: Option<&[(String, String)]>,
        now: DateTime<Utc>,
        reason: &str,
        update: &DiscoveryPublicationStateUpdate,
        policy_fingerprint: &str,
        runtime_cursor: &DiscoveryRuntimeCursor,
        rug_quarantine_prune_reason: Option<&str>,
        rug_quarantines: &[RugWalletQuarantineUpsert],
    ) -> Result<FollowlistUpdateResult> {
        crate::schema::ensure_discovery_v2_schema(self)?;
        let tx = self.conn.unchecked_transaction()?;
        upsert_wallets(&tx, wallets)?;
        insert_metrics(&tx, metrics)?;
        if let Some(reason) = rug_quarantine_prune_reason {
            prune_expired_rug_wallet_quarantines_on_conn(&tx, reason, now)?;
        }
        upsert_rug_wallet_quarantines_on_conn(&tx, rug_quarantines)?;
        let result = update_followlist(&tx, desired_wallets, true, true, now, reason)?;
        if let Some(candidate_sources) = candidate_sources {
            replace_candidate_sources(
                &tx,
                candidate_sources,
                update.last_published_window_start.unwrap_or(now),
                now,
            )?;
        }
        write_publication_state_on_conn(
            &tx,
            update,
            false,
            Some(policy_fingerprint),
            Some(runtime_cursor),
        )?;
        write_discovery_runtime_cursor_on_conn(&tx, runtime_cursor)?;
        tx.commit()
            .context("failed committing discovery v2 publication truth transaction")?;
        Ok(result)
    }

    pub fn discovery_publication_state_read_only(
        &self,
    ) -> Result<Option<DiscoveryPublicationStateRow>> {
        if !self.sqlite_table_exists("discovery_strategy_state")? {
            return Ok(None);
        }
        publication_state_query(&self.conn)
    }

    pub fn load_wallet_metric_snapshots_for_window(
        &self,
        window_start: DateTime<Utc>,
    ) -> Result<Vec<PersistedWalletMetricSnapshotRow>> {
        load_wallet_metric_snapshots_for_window_on_conn(&self.conn, window_start)
    }

    pub fn export_discovery_runtime_artifact(
        &self,
        exported_at: DateTime<Utc>,
        export_gate: DiscoveryPublicationFreshnessGate,
    ) -> Result<DiscoveryRuntimeArtifact> {
        let tx = self
            .conn
            .unchecked_transaction()
            .context("failed opening discovery runtime artifact export transaction")?;
        let publication_state = publication_state_query(&tx)?.ok_or_else(|| {
            anyhow::anyhow!(
                "discovery runtime artifact export requires persisted publication truth"
            )
        })?;
        let truth_detail =
            runtime_artifact_export_truth_detail(&publication_state, &export_gate, exported_at);
        if publication_state.runtime_mode != DiscoveryRuntimeMode::Healthy {
            return Err(anyhow::anyhow!(
                "discovery runtime artifact export requires healthy publication state ({truth_detail})"
            ));
        }
        if !publication_state.has_complete_publication_truth() {
            return Err(anyhow::anyhow!(
                "discovery runtime artifact export requires complete publication truth ({truth_detail})"
            ));
        }
        if export_gate.expected_scoring_source.is_none()
            || export_gate.expected_policy_fingerprint.is_none()
            || publication_state.published_scoring_source.is_none()
            || publication_state.publication_policy_fingerprint.is_none()
            || publication_state.publication_runtime_cursor.is_none()
        {
            return Err(anyhow::anyhow!(
                "discovery runtime artifact export requires complete publication identity ({truth_detail})"
            ));
        }
        if !publication_state.is_fresh_under_gate(&export_gate, exported_at) {
            return Err(anyhow::anyhow!(
                "discovery runtime artifact export requires fresh publication truth under export gate ({truth_detail})"
            ));
        }
        if !publication_state.matches_expected_publication_identity(&export_gate) {
            return Err(anyhow::anyhow!(
                "discovery runtime artifact export requires expected publication identity ({truth_detail})"
            ));
        }
        let runtime_cursor = load_discovery_runtime_cursor_on_conn(&tx)?.ok_or_else(|| {
                anyhow::anyhow!(
                    "discovery runtime artifact export requires a persisted discovery runtime cursor ({truth_detail})"
                )
            })?;
        if !publication_state.matches_publication_runtime_cursor(&runtime_cursor) {
            return Err(anyhow::anyhow!(
                "discovery runtime artifact export requires publication-bound runtime cursor ({truth_detail})"
            ));
        }
        if !publication_state
            .has_fresh_publication_runtime_cursor_under_gate(&export_gate, exported_at)
        {
            return Err(anyhow::anyhow!(
                "discovery runtime artifact export requires fresh publication-bound runtime cursor ({truth_detail})"
            ));
        }
        let published_window_start = publication_state
            .last_published_window_start
            .expect("validated complete publication truth above");
        let published_wallet_ids = publication_state
            .published_wallet_ids
            .as_ref()
            .expect("validated complete publication truth above");
        let published_wallet_ids = published_wallet_ids.iter().cloned().collect::<HashSet<_>>();
        let published_wallet_metrics_snapshot =
            load_wallet_metric_snapshots_for_window_on_conn(&tx, published_window_start)?
                .into_iter()
                .filter(|row| published_wallet_ids.contains(&row.wallet_id))
                .collect();
        let artifact = DiscoveryRuntimeArtifact {
            format_version: DISCOVERY_RUNTIME_ARTIFACT_FORMAT_VERSION,
            exported_at,
            export_gate,
            publication_state,
            runtime_cursor,
            published_wallet_metrics_snapshot,
        };
        validate_runtime_artifact_snapshot_shape(&artifact)?;
        tx.commit()
            .context("failed committing discovery runtime artifact export transaction")?;
        Ok(artifact)
    }

    pub fn load_discovery_runtime_cursor(&self) -> Result<Option<DiscoveryRuntimeCursor>> {
        if !self.sqlite_table_exists("discovery_runtime_state")? {
            return Ok(None);
        }
        load_discovery_runtime_cursor_on_conn(&self.conn)
    }

    pub fn set_discovery_runtime_cursor(&self, cursor: &DiscoveryRuntimeCursor) -> Result<()> {
        crate::schema::ensure_discovery_runtime_state_table(self)?;
        write_discovery_runtime_cursor_on_conn(&self.conn, cursor)
    }

    pub fn list_active_follow_wallets(&self) -> Result<HashSet<String>> {
        if !self.sqlite_table_exists("followlist")? {
            return Ok(HashSet::new());
        }
        let mut stmt = self
            .conn
            .prepare("SELECT wallet_id FROM followlist WHERE active = 1")?;
        let mut rows = stmt.query([])?;
        let mut wallets = HashSet::new();
        while let Some(row) = rows.next()? {
            wallets.insert(row.get::<_, String>(0)?);
        }
        Ok(wallets)
    }

    pub fn was_wallet_followed_at(&self, wallet_id: &str, ts: DateTime<Utc>) -> Result<bool> {
        if !self.sqlite_table_exists("followlist")? {
            return Ok(false);
        }
        let ts_raw = ts.to_rfc3339();
        let exists: Option<i64> = self
            .conn
            .query_row(
                "SELECT 1
                 FROM followlist
                 WHERE wallet_id = ?1
                   AND added_at <= ?2
                   AND (removed_at IS NULL OR ?2 < removed_at)
                 LIMIT 1",
                params![wallet_id, ts_raw],
                |row| row.get(0),
            )
            .optional()
            .context("failed checking temporal followlist membership")?;
        Ok(exists.is_some())
    }

    pub fn active_follow_wallet_row_count(&self) -> Result<usize> {
        if !self.sqlite_table_exists("followlist")? {
            return Ok(0);
        }
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM followlist WHERE active = 1",
            [],
            |row| row.get(0),
        )?;
        Ok(count.max(0) as usize)
    }
}
