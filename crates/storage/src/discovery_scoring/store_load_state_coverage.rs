impl SqliteStore {
    pub fn load_discovery_scoring_covered_through(&self) -> Result<Option<DateTime<Utc>>> {
        self.load_discovery_scoring_state_ts("covered_through_ts")
    }

    pub fn load_discovery_scoring_covered_through_cursor(
        &self,
    ) -> Result<Option<DiscoveryRuntimeCursor>> {
        let Some(ts_utc) = self.load_discovery_scoring_covered_through()? else {
            return Ok(None);
        };
        let slot_raw = self
            .conn
            .query_row(
                "SELECT state_value
                 FROM discovery_scoring_state
                 WHERE state_key = 'covered_through_slot'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("failed querying discovery_scoring_state.covered_through_slot")?;
        let signature = self
            .conn
            .query_row(
                "SELECT state_value
                 FROM discovery_scoring_state
                 WHERE state_key = 'covered_through_signature'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("failed querying discovery_scoring_state.covered_through_signature")?;
        let Some(slot_raw) = slot_raw else {
            return Ok(None);
        };
        let Some(signature) = signature else {
            return Ok(None);
        };
        let slot = slot_raw.parse::<u64>().with_context(|| {
            format!("invalid discovery_scoring_state.covered_through_slot value: {slot_raw}")
        })?;
        Ok(Some(DiscoveryRuntimeCursor {
            ts_utc,
            slot,
            signature,
        }))
    }

    pub fn load_discovery_scoring_backfill_protected_since(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>> {
        let Some(expires_at) =
            self.load_discovery_scoring_state_ts("backfill_protect_expires_at")?
        else {
            return Ok(None);
        };
        if expires_at < now {
            return Ok(None);
        }
        self.load_discovery_scoring_state_ts("backfill_protect_since_ts")
    }
}
