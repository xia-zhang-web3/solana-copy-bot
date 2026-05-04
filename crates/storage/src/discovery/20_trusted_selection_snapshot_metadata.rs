impl SqliteStore {
    pub fn latest_trusted_wallet_metrics_snapshot_metadata(
        &self,
    ) -> Result<Option<TrustedWalletMetricsSnapshotRow>> {
        self.ensure_trusted_wallet_metrics_snapshots_table()?;
        let raw = self
            .conn
            .query_row(
                "SELECT
                    snapshot_id,
                    source_snapshot_id,
                    source_window_start,
                    effective_window_start,
                    created_at,
                    source_kind,
                    row_count,
                    trust_state
                 FROM trusted_wallet_metrics_snapshots
                 ORDER BY effective_window_start DESC, created_at DESC
                 LIMIT 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, String>(7)?,
                    ))
                },
            )
            .optional()
            .context("failed reading latest trusted wallet_metrics snapshot metadata")?;
        raw.map(
            |(
                snapshot_id,
                source_snapshot_id,
                source_window_start_raw,
                effective_window_start_raw,
                created_at_raw,
                source_kind_raw,
                row_count,
                trust_state_raw,
            )| {
                Ok(TrustedWalletMetricsSnapshotRow {
                    snapshot_id,
                    source_snapshot_id,
                    source_window_start: parse_optional_rfc3339_utc(
                        source_window_start_raw,
                        "trusted_wallet_metrics_snapshots.source_window_start",
                    )?,
                    effective_window_start: parse_rfc3339_utc(
                        &effective_window_start_raw,
                        "trusted_wallet_metrics_snapshots.effective_window_start",
                    )?,
                    created_at: parse_rfc3339_utc(
                        &created_at_raw,
                        "trusted_wallet_metrics_snapshots.created_at",
                    )?,
                    source_kind: TrustedSnapshotSourceKind::parse(&source_kind_raw)?,
                    row_count: row_count.max(0) as usize,
                    trust_state: TrustedSelectionState::parse(&trust_state_raw)?,
                })
            },
        )
        .transpose()
    }

    pub fn trusted_wallet_metrics_snapshot_metadata_for_window(
        &self,
        effective_window_start: DateTime<Utc>,
    ) -> Result<Option<TrustedWalletMetricsSnapshotRow>> {
        self.ensure_trusted_wallet_metrics_snapshots_table()?;
        let raw = self
            .conn
            .query_row(
                "SELECT
                    snapshot_id,
                    source_snapshot_id,
                    source_window_start,
                    effective_window_start,
                    created_at,
                    source_kind,
                    row_count,
                    trust_state
                 FROM trusted_wallet_metrics_snapshots
                 WHERE effective_window_start = ?1",
                params![canonical_wallet_metrics_window_start(
                    effective_window_start
                )],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, String>(7)?,
                    ))
                },
            )
            .optional()
            .context("failed reading trusted wallet_metrics snapshot metadata by window")?;
        raw.map(
            |(
                snapshot_id,
                source_snapshot_id,
                source_window_start_raw,
                effective_window_start_raw,
                created_at_raw,
                source_kind_raw,
                row_count,
                trust_state_raw,
            )| {
                Ok(TrustedWalletMetricsSnapshotRow {
                    snapshot_id,
                    source_snapshot_id,
                    source_window_start: parse_optional_rfc3339_utc(
                        source_window_start_raw,
                        "trusted_wallet_metrics_snapshots.source_window_start",
                    )?,
                    effective_window_start: parse_rfc3339_utc(
                        &effective_window_start_raw,
                        "trusted_wallet_metrics_snapshots.effective_window_start",
                    )?,
                    created_at: parse_rfc3339_utc(
                        &created_at_raw,
                        "trusted_wallet_metrics_snapshots.created_at",
                    )?,
                    source_kind: TrustedSnapshotSourceKind::parse(&source_kind_raw)?,
                    row_count: row_count.max(0) as usize,
                    trust_state: TrustedSelectionState::parse(&trust_state_raw)?,
                })
            },
        )
        .transpose()
    }
}
