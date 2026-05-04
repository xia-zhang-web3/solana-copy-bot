fn insert_trusted_wallet_metrics_snapshot_on_conn(
    conn: &Connection,
    snapshot_write: &TrustedWalletMetricsSnapshotWrite,
) -> Result<()> {
    conn.execute(
        "INSERT INTO trusted_wallet_metrics_snapshots(
            snapshot_id,
            source_snapshot_id,
            source_window_start,
            effective_window_start,
            created_at,
            source_kind,
            row_count,
            trust_state
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            &snapshot_write.snapshot_id,
            &snapshot_write.source_snapshot_id,
            snapshot_write
                .source_window_start
                .map(canonical_wallet_metrics_window_start),
            canonical_wallet_metrics_window_start(snapshot_write.effective_window_start),
            snapshot_write.created_at.to_rfc3339(),
            snapshot_write.source_kind.as_str(),
            snapshot_write.row_count as i64,
            snapshot_write.trust_state.as_str(),
        ],
    )
    .context("failed inserting trusted wallet_metrics snapshot metadata")?;
    Ok(())
}

pub(crate) fn upsert_wallet_activity_days_on_conn(
    conn: &Connection,
    rows: &[WalletActivityDayRow],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let mut stmt = conn
        .prepare_cached(
            "INSERT INTO wallet_activity_days(wallet_id, activity_day, last_seen)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(wallet_id, activity_day) DO UPDATE SET
                last_seen = CASE
                    WHEN excluded.last_seen > wallet_activity_days.last_seen
                        THEN excluded.last_seen
                    ELSE wallet_activity_days.last_seen
                END",
        )
        .context("failed to prepare wallet_activity_days upsert statement")?;
    for row in rows {
        stmt.execute(params![
            &row.wallet_id,
            row.activity_day.format("%Y-%m-%d").to_string(),
            row.last_seen.to_rfc3339(),
        ])
        .context("failed to upsert wallet_activity_days row")?;
    }
    Ok(())
}
