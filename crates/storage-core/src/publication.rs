use crate::observed::parse_rfc3339_utc;
use crate::{
    DiscoveryPublicationFreshnessGate, DiscoveryPublicationStateRow,
    DiscoveryPublicationStateUpdate, DiscoveryRuntimeArtifact, DiscoveryRuntimeCursor,
    DiscoveryRuntimeMode, FollowlistUpdateResult, PersistedWalletMetricSnapshotRow,
    SqliteDiscoveryStore, DISCOVERY_RUNTIME_ARTIFACT_FORMAT_VERSION,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::{WalletMetricRow, WalletUpsertRow};
use rusqlite::{params, OptionalExtension};
use std::collections::HashSet;

include!("publication_artifact.rs");
include!("publication_followlist.rs");
include!("publication_metrics.rs");
include!("publication_state.rs");
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
        let wallet_ids_json = update
            .published_wallet_ids
            .as_deref()
            .map(canonical_wallet_ids_json)
            .transpose()?;
        self.conn.execute(
            "INSERT INTO discovery_strategy_state(
                id, publication_runtime_mode, publication_reason,
                publication_last_published_at, publication_last_published_window_start,
                publication_scoring_source, publication_wallet_ids_json,
                publication_policy_fingerprint, updated_at
             ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(id) DO UPDATE SET
                publication_runtime_mode = excluded.publication_runtime_mode,
                publication_reason = excluded.publication_reason,
                publication_last_published_at =
                    CASE WHEN ?9 THEN NULL ELSE excluded.publication_last_published_at END,
                publication_last_published_window_start =
                    CASE WHEN ?9 THEN NULL ELSE excluded.publication_last_published_window_start END,
                publication_scoring_source = excluded.publication_scoring_source,
                publication_wallet_ids_json =
                    CASE WHEN ?9 THEN NULL ELSE excluded.publication_wallet_ids_json END,
                publication_policy_fingerprint =
                    CASE WHEN ?9 THEN NULL ELSE excluded.publication_policy_fingerprint END,
                updated_at = excluded.updated_at",
            params![
                update.runtime_mode.as_str(),
                &update.reason,
                update.last_published_at.map(|ts| ts.to_rfc3339()),
                update.last_published_window_start.map(|ts| ts.to_rfc3339()),
                update.published_scoring_source.as_deref(),
                wallet_ids_json.as_deref(),
                policy_fingerprint,
                Utc::now().to_rfc3339(),
                clear_published_truth,
            ],
        )?;
        Ok(())
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
            runtime_artifact_export_truth_detail(&publication_state, export_gate, exported_at);
        if !publication_state.has_complete_publication_truth() {
            return Err(anyhow::anyhow!(
                "discovery runtime artifact export requires complete publication truth ({truth_detail})"
            ));
        }
        if !publication_state.is_fresh_under_gate(export_gate, exported_at) {
            return Err(anyhow::anyhow!(
                "discovery runtime artifact export requires fresh publication truth under export gate ({truth_detail})"
            ));
        }
        let runtime_cursor = load_discovery_runtime_cursor_on_conn(&tx)?.ok_or_else(|| {
                anyhow::anyhow!(
                    "discovery runtime artifact export requires a persisted discovery runtime cursor ({truth_detail})"
                )
            })?;
        let published_window_start = publication_state
            .last_published_window_start
            .expect("validated complete publication truth above");
        let artifact = DiscoveryRuntimeArtifact {
            format_version: DISCOVERY_RUNTIME_ARTIFACT_FORMAT_VERSION,
            exported_at,
            export_gate,
            publication_state,
            runtime_cursor,
            published_wallet_metrics_snapshot: load_wallet_metric_snapshots_for_window_on_conn(
                &tx,
                published_window_start,
            )?,
        };
        validate_runtime_artifact_snapshot_shape(&artifact)?;
        tx.commit()
            .context("failed committing discovery runtime artifact export transaction")?;
        Ok(artifact)
    }

    pub fn load_discovery_runtime_cursor(&self) -> Result<Option<DiscoveryRuntimeCursor>> {
        load_discovery_runtime_cursor_on_conn(&self.conn)
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
}
