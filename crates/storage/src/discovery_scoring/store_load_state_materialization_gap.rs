use super::*;

impl SqliteStore {
    pub fn load_discovery_scoring_materialization_gap_cursor(
        &self,
    ) -> Result<Option<DiscoveryRuntimeCursor>> {
        let Some(ts_utc) = self.load_discovery_scoring_state_ts("materialization_gap_since_ts")?
        else {
            return Ok(None);
        };
        let slot_raw = self
            .conn
            .query_row(
                "SELECT state_value
                 FROM discovery_scoring_state
                 WHERE state_key = 'materialization_gap_since_slot'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("failed querying discovery_scoring_state.materialization_gap_since_slot")?;
        let signature = self
            .conn
            .query_row(
                "SELECT state_value
                 FROM discovery_scoring_state
                 WHERE state_key = 'materialization_gap_since_signature'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context(
                "failed querying discovery_scoring_state.materialization_gap_since_signature",
            )?;
        match (slot_raw, signature) {
            (Some(slot_raw), Some(signature)) => {
                let slot = slot_raw.parse::<u64>().with_context(|| {
                    format!(
                        "invalid discovery_scoring_state.materialization_gap_since_slot value: {slot_raw}"
                    )
                })?;
                Ok(Some(DiscoveryRuntimeCursor {
                    ts_utc,
                    slot,
                    signature,
                }))
            }
            _ => Ok(Some(DiscoveryRuntimeCursor {
                ts_utc,
                slot: 0,
                signature: String::new(),
            })),
        }
    }

    pub fn load_discovery_scoring_materialization_gap_repair_target(
        &self,
    ) -> Result<Option<(DiscoveryRuntimeCursor, DiscoveryRuntimeCursor)>> {
        let gap_cursor = self.load_discovery_scoring_cursor_state_exact(
            "materialization_gap_repair_gap_ts",
            "materialization_gap_repair_gap_slot",
            "materialization_gap_repair_gap_signature",
            "materialization_gap_repair_gap",
        )?;
        let target_cursor = self.load_discovery_scoring_cursor_state_exact(
            "materialization_gap_repair_target_ts",
            "materialization_gap_repair_target_slot",
            "materialization_gap_repair_target_signature",
            "materialization_gap_repair_target",
        )?;
        match (gap_cursor, target_cursor) {
            (None, None) => Ok(None),
            (Some(gap_cursor), Some(target_cursor)) => Ok(Some((gap_cursor, target_cursor))),
            _ => Err(anyhow!(
                "discovery_scoring_state.materialization_gap_repair target is partially populated"
            )),
        }
    }
}
