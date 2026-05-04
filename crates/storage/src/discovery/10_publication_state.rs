impl SqliteStore {
    pub fn discovery_publication_state(&self) -> Result<Option<DiscoveryPublicationStateRow>> {
        self.ensure_discovery_strategy_state_table()?;
        self.discovery_publication_state_query()
    }

    pub fn discovery_publication_state_read_only(
        &self,
    ) -> Result<Option<DiscoveryPublicationStateRow>> {
        if !self.sqlite_table_exists("discovery_strategy_state")? {
            return Ok(None);
        }
        self.discovery_publication_state_query()
    }

    fn discovery_publication_state_query_with_policy(
        &self,
    ) -> Result<Option<(DiscoveryPublicationStateRow, Option<String>)>> {
        let raw = self
            .conn
            .query_row(
                "SELECT
                    publication_runtime_mode,
                    publication_reason,
                    publication_last_published_at,
                    publication_last_published_window_start,
                    publication_scoring_source,
                    publication_wallet_ids_json,
                    publication_policy_fingerprint,
                    updated_at
                 FROM discovery_strategy_state
                 WHERE id = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, String>(7)?,
                    ))
                },
            )
            .optional()
            .context("failed reading discovery publication state")?;
        raw.map(
            |(
                runtime_mode_raw,
                reason,
                last_published_at_raw,
                last_published_window_start_raw,
                published_scoring_source,
                published_wallet_ids_raw,
                publication_policy_fingerprint,
                updated_at_raw,
            )| {
                Ok((
                    DiscoveryPublicationStateRow {
                        runtime_mode: DiscoveryRuntimeMode::parse(&runtime_mode_raw)?,
                        reason,
                        last_published_at: parse_optional_rfc3339_utc(
                            last_published_at_raw,
                            "discovery_strategy_state.publication_last_published_at",
                        )?,
                        last_published_window_start: parse_optional_rfc3339_utc(
                            last_published_window_start_raw,
                            "discovery_strategy_state.publication_last_published_window_start",
                        )?,
                        published_scoring_source,
                        published_wallet_ids: parse_optional_wallet_ids_json(
                            published_wallet_ids_raw,
                            "discovery_strategy_state.publication_wallet_ids_json",
                        )?,
                        publication_policy_fingerprint: publication_policy_fingerprint.clone(),
                        updated_at: parse_rfc3339_utc(
                            &updated_at_raw,
                            "discovery_strategy_state.updated_at",
                        )?,
                    },
                    publication_policy_fingerprint,
                ))
            },
        )
        .transpose()
    }

    fn discovery_publication_state_query(&self) -> Result<Option<DiscoveryPublicationStateRow>> {
        Ok(self
            .discovery_publication_state_query_with_policy()?
            .map(|(state, _)| state))
    }

    pub fn set_discovery_publication_state(
        &self,
        update: &DiscoveryPublicationStateUpdate,
    ) -> Result<()> {
        self.set_discovery_publication_state_with_options(update, false, None)
    }

    pub fn set_discovery_publication_state_with_options(
        &self,
        update: &DiscoveryPublicationStateUpdate,
        clear_published_truth: bool,
        policy_fingerprint: Option<&str>,
    ) -> Result<()> {
        self.ensure_discovery_strategy_state_table()?;
        let previous_state = self.discovery_publication_state_query()?;
        let published_wallet_ids_json = update
            .published_wallet_ids
            .as_deref()
            .map(canonicalize_wallet_ids)
            .map(|wallet_ids| {
                serde_json::to_string(&wallet_ids)
                    .context("failed serializing discovery published wallet ids")
            })
            .transpose()?;
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO discovery_strategy_state(
                    id,
                    publication_runtime_mode,
                    publication_reason,
                    publication_last_published_at,
                    publication_last_published_window_start,
                    publication_scoring_source,
                    publication_wallet_ids_json,
                    publication_policy_fingerprint,
                    updated_at
                 ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(id) DO UPDATE SET
                    publication_runtime_mode = excluded.publication_runtime_mode,
                    publication_reason = excluded.publication_reason,
                    publication_last_published_at = CASE
                        WHEN ?9 THEN NULL
                        ELSE COALESCE(
                            excluded.publication_last_published_at,
                            discovery_strategy_state.publication_last_published_at
                        )
                    END,
                    publication_last_published_window_start = CASE
                        WHEN ?9 THEN NULL
                        ELSE COALESCE(
                            excluded.publication_last_published_window_start,
                            discovery_strategy_state.publication_last_published_window_start
                        )
                    END,
                    publication_scoring_source = excluded.publication_scoring_source,
                    publication_wallet_ids_json = CASE
                        WHEN ?9 THEN NULL
                        ELSE COALESCE(
                            excluded.publication_wallet_ids_json,
                            discovery_strategy_state.publication_wallet_ids_json
                        )
                    END,
                    publication_policy_fingerprint = CASE
                        WHEN ?9 THEN NULL
                        ELSE COALESCE(
                            excluded.publication_policy_fingerprint,
                            discovery_strategy_state.publication_policy_fingerprint
                        )
                    END,
                    updated_at = excluded.updated_at",
                params![
                    update.runtime_mode.as_str(),
                    &update.reason,
                    update.last_published_at.map(|ts| ts.to_rfc3339()),
                    update
                        .last_published_window_start
                        .map(canonical_wallet_metrics_window_start),
                    update.published_scoring_source.as_deref(),
                    published_wallet_ids_json.as_deref(),
                    policy_fingerprint,
                    Utc::now().to_rfc3339(),
                    clear_published_truth,
                ],
            )
        })
        .context("failed updating discovery publication state")?;
        let new_state = self
            .discovery_publication_state_query()?
            .context("expected discovery publication state row after write")?;
        let diagnostics = snapshot_discovery_publication_state_write_diagnostics(
            previous_state.as_ref(),
            &new_state,
            update,
            clear_published_truth,
        );
        info!(
            publication_state_write_kind = diagnostics.write_kind,
            publication_previous_last_published_at = ?diagnostics.previous_last_published_at,
            publication_new_last_published_at = ?diagnostics.new_last_published_at,
            publication_previous_wallet_id_count = diagnostics.previous_published_wallet_count,
            publication_new_wallet_id_count = diagnostics.new_published_wallet_count,
            publication_published_universe_persisted = diagnostics.published_universe_persisted,
            publication_runtime_mode = diagnostics.runtime_mode.as_str(),
            publication_reason = diagnostics.reason.as_str(),
            publication_stale_fields_carried_forward =
                diagnostics.stale_fields_carried_forward,
            publication_stale_last_published_at_carried_forward =
                diagnostics.stale_last_published_at_carried_forward,
            publication_stale_published_wallet_ids_carried_forward =
                diagnostics.stale_published_wallet_ids_carried_forward,
            publication_updated_at = %diagnostics.updated_at,
            "discovery publication state write completed"
        );
        Ok(())
    }

    pub fn discovery_publication_state_with_policy_read_only(
        &self,
    ) -> Result<Option<(DiscoveryPublicationStateRow, Option<String>)>> {
        self.ensure_discovery_strategy_state_table()?;
        if !self.sqlite_table_exists("discovery_strategy_state")? {
            return Ok(None);
        }
        self.discovery_publication_state_query_with_policy()
    }
}
