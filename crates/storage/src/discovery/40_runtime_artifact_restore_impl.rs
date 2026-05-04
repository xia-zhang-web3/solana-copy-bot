impl SqliteStore {
    pub fn restore_discovery_runtime_artifact(
        &self,
        artifact: &DiscoveryRuntimeArtifact,
        restored_at: DateTime<Utc>,
        bootstrap_degraded: bool,
    ) -> Result<()> {
        validate_runtime_artifact_snapshot_shape(artifact)?;
        self.ensure_discovery_strategy_state_table()?;
        self.ensure_trusted_wallet_metrics_snapshots_table()?;
        self.ensure_discovery_recent_raw_restore_state_table()?;
        let dirty_tables = self.runtime_artifact_restore_dirty_tables()?;
        if !dirty_tables.is_empty() {
            let detail = format_runtime_artifact_restore_dirty_tables(&dirty_tables);
            return Err(anyhow::anyhow!(
                "discovery runtime artifact restore requires an empty runtime db; found durable rows in {detail}"
            ));
        }
        self.with_immediate_transaction_retry("discovery runtime artifact restore", |conn| {
            ensure_discovery_runtime_restore_tables_on_conn(conn)?;
            fail_if_runtime_artifact_restore_dirty_on_conn(conn)?;
            restore_runtime_artifact_wallets_on_conn(conn, artifact)?;
            restore_runtime_artifact_wallet_metrics_on_conn(conn, artifact)?;
            restore_runtime_artifact_followlist_on_conn(conn, artifact, restored_at)?;
            restore_runtime_artifact_cursor_on_conn(conn, artifact, restored_at)?;
            restore_runtime_artifact_publication_state_on_conn(
                conn,
                artifact,
                restored_at,
                bootstrap_degraded,
            )?;
            initialize_recent_raw_restore_state_from_artifact_on_conn(conn, artifact, restored_at)?;
            Ok(())
        })
    }
}
