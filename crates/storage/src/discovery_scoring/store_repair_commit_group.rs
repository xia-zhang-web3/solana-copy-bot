impl SqliteStore {
    pub fn apply_discovery_scoring_repair_commit_group(
        &self,
        swaps: &[SwapEvent],
        config: &DiscoveryAggregateWriteConfig,
        expected_start_cursor: &DiscoveryRuntimeCursor,
        covered_through_cursor: &DiscoveryRuntimeCursor,
    ) -> Result<&'static str> {
        let prepared = prepare_discovery_scoring_swaps(&self.conn, swaps, config)?;
        self.with_immediate_transaction_retry(
            "discovery scoring aggregate repair commit group",
            |conn| {
                let current = load_discovery_scoring_cursor_state_exact_on_conn(
                    conn,
                    "covered_through_ts",
                    "covered_through_slot",
                    "covered_through_signature",
                    "covered_through",
                )?;
                let Some(current) = current else {
                    anyhow::bail!(
                        "discovery scoring covered_through cursor missing before repair commit group"
                    );
                };
                match cmp_cursor_order(&current, expected_start_cursor) {
                    Ordering::Equal => {}
                    Ordering::Greater => {
                        if cmp_cursor_order(&current, covered_through_cursor) != Ordering::Less {
                            return Ok("already_covered");
                        }
                        return Ok("concurrent_progress");
                    }
                    Ordering::Less => {
                        anyhow::bail!(
                            "discovery scoring covered_through cursor is before expected repair start cursor"
                        );
                    }
                }

                apply_discovery_scoring_swaps_on_conn(conn, &prepared)?;
                finalize_mature_rug_facts_on_conn(conn, covered_through_cursor.ts_utc)?;
                let now = Utc::now().to_rfc3339();
                upsert_discovery_scoring_cursor_state_on_conn(
                    conn,
                    "covered_through_ts",
                    "covered_through_slot",
                    "covered_through_signature",
                    covered_through_cursor,
                    &now,
                )?;
                Ok("committed")
            },
        )
    }
}
