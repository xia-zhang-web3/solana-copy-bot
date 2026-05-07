use super::*;

impl SqliteStore {
    pub fn append_discovery_wallet_freshness_capture(
        &self,
        capture: &DiscoveryWalletFreshnessCaptureWrite,
    ) -> Result<DiscoveryWalletFreshnessCaptureRow> {
        self.ensure_discovery_wallet_freshness_history_table()?;
        let published_wallet_ids_json = serde_json::to_string(&capture.published_wallet_ids)
            .context("failed serializing discovery wallet freshness published wallet ids")?;
        let active_follow_wallet_ids_json = serde_json::to_string(
            &capture.active_follow_wallet_ids,
        )
        .context("failed serializing discovery wallet freshness active follow wallet ids")?;
        let current_raw_top_wallet_ids_json = serde_json::to_string(
            &capture.current_raw_top_wallet_ids,
        )
        .context("failed serializing discovery wallet freshness current raw top wallet ids")?;
        let capture_id = self
            .execute_with_retry_result(|conn| {
                conn.execute(
                    "INSERT INTO discovery_wallet_freshness_history(
                        captured_at,
                        recent_cycles,
                        verdict,
                        reason,
                        publication_age_seconds,
                        raw_truth_sufficient,
                        raw_truth_reason,
                        shadow_signal_verdict,
                        shadow_signal_reason,
                        published_wallet_ids_json,
                        active_follow_wallet_ids_json,
                        current_raw_top_wallet_ids_json,
                        audit_json,
                        shadow_signal_json
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                    params![
                        capture.captured_at.to_rfc3339(),
                        capture.recent_cycles.max(1) as i64,
                        capture.verdict,
                        capture.reason,
                        capture
                            .publication_age_seconds
                            .map(|value| value.min(i64::MAX as u64) as i64),
                        if capture.raw_truth_sufficient { 1 } else { 0 },
                        capture.raw_truth_reason,
                        capture.shadow_signal_verdict,
                        capture.shadow_signal_reason,
                        published_wallet_ids_json,
                        active_follow_wallet_ids_json,
                        current_raw_top_wallet_ids_json,
                        capture.audit_json,
                        capture.shadow_signal_json,
                    ],
                )?;
                Ok(conn.last_insert_rowid())
            })
            .context("failed appending discovery wallet freshness capture")?;
        self.load_discovery_wallet_freshness_capture(capture_id)?
            .ok_or_else(|| anyhow::anyhow!("wallet freshness capture disappeared after insert"))
    }

    pub fn list_discovery_wallet_freshness_captures(
        &self,
        limit: usize,
    ) -> Result<Vec<DiscoveryWalletFreshnessCaptureRow>> {
        self.ensure_discovery_wallet_freshness_history_table()?;
        let query_limit = limit.max(1).min(i64::MAX as usize) as i64;
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    capture_id,
                    captured_at,
                    recent_cycles,
                    verdict,
                    reason,
                    publication_age_seconds,
                    raw_truth_sufficient,
                    raw_truth_reason,
                    shadow_signal_verdict,
                    shadow_signal_reason,
                    published_wallet_ids_json,
                    active_follow_wallet_ids_json,
                    current_raw_top_wallet_ids_json,
                    audit_json,
                    shadow_signal_json
                 FROM discovery_wallet_freshness_history
                 ORDER BY captured_at DESC, capture_id DESC
                 LIMIT ?1",
            )
            .context("failed to prepare discovery wallet freshness history query")?;
        let mut rows = stmt
            .query(params![query_limit])
            .context("failed querying discovery wallet freshness history")?;
        let mut captures = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating discovery wallet freshness history rows")?
        {
            captures.push(read_discovery_wallet_freshness_capture_row(row)?);
        }
        Ok(captures)
    }

    fn load_discovery_wallet_freshness_capture(
        &self,
        capture_id: i64,
    ) -> Result<Option<DiscoveryWalletFreshnessCaptureRow>> {
        self.ensure_discovery_wallet_freshness_history_table()?;
        self.conn
            .query_row(
                "SELECT
                    capture_id,
                    captured_at,
                    recent_cycles,
                    verdict,
                    reason,
                    publication_age_seconds,
                    raw_truth_sufficient,
                    raw_truth_reason,
                    shadow_signal_verdict,
                    shadow_signal_reason,
                    published_wallet_ids_json,
                    active_follow_wallet_ids_json,
                    current_raw_top_wallet_ids_json,
                    audit_json,
                    shadow_signal_json
                 FROM discovery_wallet_freshness_history
                 WHERE capture_id = ?1",
                params![capture_id],
                read_discovery_wallet_freshness_capture_row,
            )
            .optional()
            .context("failed loading discovery wallet freshness capture")
    }
}
