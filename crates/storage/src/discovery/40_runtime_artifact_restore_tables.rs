#[allow(dead_code)]
fn ensure_discovery_runtime_restore_tables_on_conn(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS discovery_runtime_state (
            id INTEGER PRIMARY KEY CHECK(id = 1),
            cursor_ts TEXT NOT NULL,
            cursor_slot INTEGER NOT NULL,
            cursor_signature TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS discovery_persisted_rebuild_state (
            id INTEGER PRIMARY KEY CHECK(id = 1),
            phase TEXT NOT NULL,
            window_start TEXT NOT NULL,
            horizon_end TEXT NOT NULL,
            metrics_window_start TEXT NOT NULL,
            phase_cursor_ts TEXT,
            phase_cursor_slot INTEGER,
            phase_cursor_signature TEXT,
            prepass_rows_processed INTEGER NOT NULL DEFAULT 0,
            prepass_pages_processed INTEGER NOT NULL DEFAULT 0,
            replay_rows_processed INTEGER NOT NULL DEFAULT 0,
            replay_pages_processed INTEGER NOT NULL DEFAULT 0,
            chunks_completed INTEGER NOT NULL DEFAULT 0,
            state_json TEXT NOT NULL,
            started_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS discovery_recent_raw_restore_state (
            id INTEGER PRIMARY KEY CHECK(id = 1),
            journal_available INTEGER NOT NULL DEFAULT 0,
            journal_replayed INTEGER NOT NULL DEFAULT 0,
            required_window_start TEXT,
            journal_covered_since TEXT,
            journal_covered_through_cursor_ts TEXT,
            journal_covered_through_cursor_slot INTEGER,
            journal_covered_through_cursor_signature TEXT,
            gap_fill_replayed INTEGER NOT NULL DEFAULT 0,
            gap_fill_covered_since TEXT,
            gap_fill_covered_through_cursor_ts TEXT,
            gap_fill_covered_through_cursor_slot INTEGER,
            gap_fill_covered_through_cursor_signature TEXT,
            effective_covered_since TEXT,
            effective_covered_through_cursor_ts TEXT,
            effective_covered_through_cursor_slot INTEGER,
            effective_covered_through_cursor_signature TEXT,
            artifact_runtime_cursor_ts TEXT,
            artifact_runtime_cursor_slot INTEGER,
            artifact_runtime_cursor_signature TEXT,
            journal_covers_artifact_cursor INTEGER NOT NULL DEFAULT 0,
            raw_coverage_satisfied INTEGER NOT NULL DEFAULT 0,
            gap_fill_replayed_rows INTEGER NOT NULL DEFAULT 0,
            replayed_rows INTEGER NOT NULL DEFAULT 0,
            reason TEXT,
            replay_started_at TEXT,
            replay_completed_at TEXT,
            updated_at TEXT NOT NULL
        );",
    )
    .context("failed ensuring discovery runtime restore tables exist")?;
    Ok(())
}

#[allow(dead_code)]
fn fail_if_runtime_artifact_restore_dirty_on_conn(conn: &Connection) -> Result<()> {
    let dirty_tables = runtime_artifact_restore_dirty_tables_on_conn(conn)?;
    if dirty_tables.is_empty() {
        return Ok(());
    }
    let detail = format_runtime_artifact_restore_dirty_tables(&dirty_tables);
    Err(anyhow::anyhow!(
        "discovery runtime artifact restore requires an empty runtime db; found durable rows in {detail}"
    ))
}
