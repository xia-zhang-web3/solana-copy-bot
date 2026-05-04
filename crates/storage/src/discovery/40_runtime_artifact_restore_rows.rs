fn restore_runtime_artifact_wallets_on_conn(
    conn: &Connection,
    artifact: &DiscoveryRuntimeArtifact,
) -> Result<()> {
    let mut wallet_stmt = conn
        .prepare_cached(
            "INSERT INTO wallets(wallet_id, first_seen, last_seen, status)
             VALUES (?1, ?2, ?3, ?4)",
        )
        .context("failed to prepare runtime artifact wallet restore statement")?;
    for row in &artifact.published_wallet_metrics_snapshot {
        let status = if artifact
            .publication_state
            .published_wallet_ids
            .as_ref()
            .is_some_and(|wallet_ids| wallet_ids.contains(&row.wallet_id))
        {
            "candidate"
        } else {
            "observed"
        };
        wallet_stmt
            .execute(params![
                &row.wallet_id,
                row.first_seen.to_rfc3339(),
                row.last_seen.to_rfc3339(),
                status,
            ])
            .context("failed inserting runtime artifact wallet row")?;
    }
    Ok(())
}

fn restore_runtime_artifact_wallet_metrics_on_conn(
    conn: &Connection,
    artifact: &DiscoveryRuntimeArtifact,
) -> Result<()> {
    let mut metric_stmt = conn
        .prepare_cached(
            "INSERT INTO wallet_metrics(
                wallet_id,
                window_start,
                pnl,
                win_rate,
                trades,
                closed_trades,
                hold_median_seconds,
                score,
                buy_total,
                tradable_ratio,
                rug_ratio
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        )
        .context("failed to prepare runtime artifact wallet_metrics restore statement")?;
    for row in &artifact.published_wallet_metrics_snapshot {
        metric_stmt
            .execute(params![
                &row.wallet_id,
                canonical_wallet_metrics_window_start(row.window_start),
                row.pnl,
                row.win_rate,
                row.trades as i64,
                row.closed_trades as i64,
                row.hold_median_seconds,
                row.score,
                row.buy_total as i64,
                row.tradable_ratio,
                row.rug_ratio,
            ])
            .context("failed inserting runtime artifact wallet_metrics row")?;
    }
    Ok(())
}

fn restore_runtime_artifact_followlist_on_conn(
    conn: &Connection,
    artifact: &DiscoveryRuntimeArtifact,
    restored_at: DateTime<Utc>,
) -> Result<()> {
    let mut follow_stmt = conn
        .prepare_cached(
            "INSERT INTO followlist(wallet_id, added_at, reason, active)
             VALUES (?1, ?2, ?3, 1)",
        )
        .context("failed to prepare runtime artifact followlist restore statement")?;
    for wallet_id in artifact
        .publication_state
        .published_wallet_ids
        .as_ref()
        .expect("validated complete publication truth above")
    {
        follow_stmt
            .execute(params![
                wallet_id,
                restored_at.to_rfc3339(),
                "runtime_artifact_restore",
            ])
            .context("failed inserting runtime artifact followlist row")?;
    }
    Ok(())
}

fn restore_runtime_artifact_cursor_on_conn(
    conn: &Connection,
    artifact: &DiscoveryRuntimeArtifact,
    restored_at: DateTime<Utc>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO discovery_runtime_state(
            id,
            cursor_ts,
            cursor_slot,
            cursor_signature,
            updated_at
         ) VALUES (1, ?1, ?2, ?3, ?4)",
        params![
            artifact.runtime_cursor.ts_utc.to_rfc3339(),
            artifact.runtime_cursor.slot as i64,
            artifact.runtime_cursor.signature.as_str(),
            restored_at.to_rfc3339(),
        ],
    )
    .context("failed restoring discovery runtime cursor from artifact")?;
    Ok(())
}
