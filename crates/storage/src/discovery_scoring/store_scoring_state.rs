impl SqliteStore {
    fn upsert_discovery_scoring_state_ts(
        &self,
        state_key: &str,
        value: DateTime<Utc>,
    ) -> Result<()> {
        self.with_immediate_transaction_retry("discovery scoring state update", |conn| {
            conn.execute(
                "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT(state_key) DO UPDATE SET
                    state_value = excluded.state_value,
                    updated_at = excluded.updated_at",
                params![state_key, value.to_rfc3339(), Utc::now().to_rfc3339()],
            )
            .with_context(|| format!("failed upserting discovery_scoring_state.{state_key}"))?;
            Ok(0usize)
        })?;
        Ok(())
    }

    fn load_discovery_scoring_state_ts(&self, state_key: &str) -> Result<Option<DateTime<Utc>>> {
        let raw: Option<String> = self
            .conn
            .query_row(
                "SELECT state_value
                 FROM discovery_scoring_state
                 WHERE state_key = ?1",
                params![state_key],
                |row| row.get(0),
            )
            .optional()
            .with_context(|| format!("failed querying discovery_scoring_state.{state_key}"))?;
        raw.map(|raw| parse_ts(&raw, &format!("discovery_scoring_state.{state_key}")))
            .transpose()
    }

    fn load_discovery_scoring_state_value(&self, state_key: &str) -> Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT state_value
                 FROM discovery_scoring_state
                 WHERE state_key = ?1",
                params![state_key],
                |row| row.get(0),
            )
            .optional()
            .with_context(|| format!("failed querying discovery_scoring_state.{state_key}"))
    }

    fn load_discovery_scoring_cursor_state_exact(
        &self,
        ts_key: &str,
        slot_key: &str,
        signature_key: &str,
        label: &str,
    ) -> Result<Option<DiscoveryRuntimeCursor>> {
        let ts_utc = self.load_discovery_scoring_state_ts(ts_key)?;
        let slot_raw = self.load_discovery_scoring_state_value(slot_key)?;
        let signature = self.load_discovery_scoring_state_value(signature_key)?;
        match (ts_utc, slot_raw, signature) {
            (None, None, None) => Ok(None),
            (Some(ts_utc), Some(slot_raw), Some(signature)) => {
                let slot = slot_raw.parse::<u64>().with_context(|| {
                    format!("invalid discovery_scoring_state.{slot_key} value: {slot_raw}")
                })?;
                Ok(Some(DiscoveryRuntimeCursor {
                    ts_utc,
                    slot,
                    signature,
                }))
            }
            _ => Err(anyhow!(
                "discovery_scoring_state.{label} cursor is partially populated"
            )),
        }
    }

    pub fn apply_discovery_scoring_batch(
        &self,
        swaps: &[SwapEvent],
        config: &DiscoveryAggregateWriteConfig,
    ) -> Result<()> {
        let prepared = prepare_discovery_scoring_swaps(&self.conn, swaps, config)?;
        self.with_immediate_transaction_retry("discovery scoring batch", |conn| {
            apply_discovery_scoring_swaps_on_conn(conn, &prepared)
        })
    }
}
