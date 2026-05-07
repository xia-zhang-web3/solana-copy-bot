use crate::SqliteStore;
use anyhow::{Context, Result};
use std::collections::HashSet;

impl SqliteStore {
    pub(crate) fn ensure_discovery_recent_raw_restore_state_table(&self) -> Result<()> {
        self.conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS discovery_recent_raw_restore_state (
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
                )",
            )
            .context("failed to ensure discovery_recent_raw_restore_state table exists")?;
        let columns: HashSet<String> = {
            let mut stmt = self
                .conn
                .prepare("PRAGMA table_info(discovery_recent_raw_restore_state)")
                .context(
                    "failed to prepare discovery_recent_raw_restore_state column introspection",
                )?;
            let columns = stmt
                .query_map([], |row| row.get::<_, String>(1))
                .context("failed querying discovery_recent_raw_restore_state columns")?
                .collect::<rusqlite::Result<HashSet<String>>>()
                .context("failed collecting discovery_recent_raw_restore_state columns")?;
            columns
        };
        if !columns.contains("gap_fill_replayed") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_recent_raw_restore_state
                     ADD COLUMN gap_fill_replayed INTEGER NOT NULL DEFAULT 0",
                    [],
                )
                .context("failed adding discovery_recent_raw_restore_state.gap_fill_replayed")?;
        }
        if !columns.contains("gap_fill_covered_since") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_recent_raw_restore_state
                     ADD COLUMN gap_fill_covered_since TEXT",
                    [],
                )
                .context(
                    "failed adding discovery_recent_raw_restore_state.gap_fill_covered_since",
                )?;
        }
        if !columns.contains("gap_fill_covered_through_cursor_ts") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_recent_raw_restore_state
                     ADD COLUMN gap_fill_covered_through_cursor_ts TEXT",
                    [],
                )
                .context(
                    "failed adding discovery_recent_raw_restore_state.gap_fill_covered_through_cursor_ts",
                )?;
        }
        if !columns.contains("gap_fill_covered_through_cursor_slot") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_recent_raw_restore_state
                     ADD COLUMN gap_fill_covered_through_cursor_slot INTEGER",
                    [],
                )
                .context(
                    "failed adding discovery_recent_raw_restore_state.gap_fill_covered_through_cursor_slot",
                )?;
        }
        if !columns.contains("gap_fill_covered_through_cursor_signature") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_recent_raw_restore_state
                     ADD COLUMN gap_fill_covered_through_cursor_signature TEXT",
                    [],
                )
                .context(
                    "failed adding discovery_recent_raw_restore_state.gap_fill_covered_through_cursor_signature",
                )?;
        }
        if !columns.contains("effective_covered_since") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_recent_raw_restore_state
                     ADD COLUMN effective_covered_since TEXT",
                    [],
                )
                .context(
                    "failed adding discovery_recent_raw_restore_state.effective_covered_since",
                )?;
        }
        if !columns.contains("effective_covered_through_cursor_ts") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_recent_raw_restore_state
                     ADD COLUMN effective_covered_through_cursor_ts TEXT",
                    [],
                )
                .context(
                    "failed adding discovery_recent_raw_restore_state.effective_covered_through_cursor_ts",
                )?;
        }
        if !columns.contains("effective_covered_through_cursor_slot") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_recent_raw_restore_state
                     ADD COLUMN effective_covered_through_cursor_slot INTEGER",
                    [],
                )
                .context(
                    "failed adding discovery_recent_raw_restore_state.effective_covered_through_cursor_slot",
                )?;
        }
        if !columns.contains("effective_covered_through_cursor_signature") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_recent_raw_restore_state
                     ADD COLUMN effective_covered_through_cursor_signature TEXT",
                    [],
                )
                .context(
                    "failed adding discovery_recent_raw_restore_state.effective_covered_through_cursor_signature",
                )?;
        }
        if !columns.contains("gap_fill_replayed_rows") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_recent_raw_restore_state
                     ADD COLUMN gap_fill_replayed_rows INTEGER NOT NULL DEFAULT 0",
                    [],
                )
                .context(
                    "failed adding discovery_recent_raw_restore_state.gap_fill_replayed_rows",
                )?;
        }
        Ok(())
    }
}
