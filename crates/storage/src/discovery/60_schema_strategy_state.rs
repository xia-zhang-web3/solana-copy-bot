use crate::SqliteStore;
use anyhow::{Context, Result};
use std::collections::HashSet;

impl SqliteStore {
    pub(crate) fn ensure_discovery_strategy_state_table(&self) -> Result<()> {
        self.conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS discovery_strategy_state (
                    id INTEGER PRIMARY KEY CHECK(id = 1),
                    trusted_selection_bootstrap_required INTEGER NOT NULL DEFAULT 0,
                    trusted_selection_reason TEXT NOT NULL DEFAULT '',
                    trusted_selection_state TEXT NOT NULL DEFAULT 'invalid',
                    active_trusted_snapshot_id TEXT,
                    active_trusted_snapshot_window_start TEXT,
                    last_trusted_bootstrap_source_kind TEXT,
                    last_trusted_bootstrap_at TEXT,
                    bootstrap_degraded_active INTEGER NOT NULL DEFAULT 0,
                    bootstrap_degraded_reason TEXT,
                    bootstrap_degraded_armed_at TEXT,
                    publication_runtime_mode TEXT NOT NULL DEFAULT 'fail_closed',
                    publication_reason TEXT NOT NULL DEFAULT '',
                    publication_last_published_at TEXT,
                    publication_last_published_window_start TEXT,
                    publication_scoring_source TEXT,
                    publication_wallet_ids_json TEXT,
                    publication_policy_fingerprint TEXT,
                    publication_runtime_cursor_ts TEXT,
                    publication_runtime_cursor_slot INTEGER,
                    publication_runtime_cursor_signature TEXT,
                    updated_at TEXT NOT NULL
                )",
            )
            .context("failed to ensure discovery_strategy_state table exists")?;
        let columns: HashSet<String> = {
            let mut stmt = self
                .conn
                .prepare("PRAGMA table_info(discovery_strategy_state)")
                .context("failed to prepare discovery_strategy_state column introspection")?;
            let columns = stmt
                .query_map([], |row| row.get::<_, String>(1))
                .context("failed querying discovery_strategy_state columns")?
                .collect::<rusqlite::Result<HashSet<String>>>()
                .context("failed collecting discovery_strategy_state columns")?;
            columns
        };
        if !columns.contains("trusted_selection_state") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN trusted_selection_state TEXT NOT NULL DEFAULT 'invalid'",
                    [],
                )
                .context("failed adding discovery_strategy_state.trusted_selection_state")?;
        }
        if !columns.contains("active_trusted_snapshot_id") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN active_trusted_snapshot_id TEXT",
                    [],
                )
                .context("failed adding discovery_strategy_state.active_trusted_snapshot_id")?;
        }
        if !columns.contains("active_trusted_snapshot_window_start") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN active_trusted_snapshot_window_start TEXT",
                    [],
                )
                .context(
                    "failed adding discovery_strategy_state.active_trusted_snapshot_window_start",
                )?;
        }
        if !columns.contains("last_trusted_bootstrap_source_kind") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN last_trusted_bootstrap_source_kind TEXT",
                    [],
                )
                .context(
                    "failed adding discovery_strategy_state.last_trusted_bootstrap_source_kind",
                )?;
        }
        if !columns.contains("last_trusted_bootstrap_at") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN last_trusted_bootstrap_at TEXT",
                    [],
                )
                .context("failed adding discovery_strategy_state.last_trusted_bootstrap_at")?;
        }
        if !columns.contains("bootstrap_degraded_active") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN bootstrap_degraded_active INTEGER NOT NULL DEFAULT 0",
                    [],
                )
                .context("failed adding discovery_strategy_state.bootstrap_degraded_active")?;
        }
        if !columns.contains("bootstrap_degraded_reason") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN bootstrap_degraded_reason TEXT",
                    [],
                )
                .context("failed adding discovery_strategy_state.bootstrap_degraded_reason")?;
        }
        if !columns.contains("bootstrap_degraded_armed_at") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN bootstrap_degraded_armed_at TEXT",
                    [],
                )
                .context("failed adding discovery_strategy_state.bootstrap_degraded_armed_at")?;
        }
        if !columns.contains("publication_runtime_mode") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN publication_runtime_mode TEXT NOT NULL DEFAULT 'fail_closed'",
                    [],
                )
                .context("failed adding discovery_strategy_state.publication_runtime_mode")?;
        }
        if !columns.contains("publication_reason") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN publication_reason TEXT NOT NULL DEFAULT ''",
                    [],
                )
                .context("failed adding discovery_strategy_state.publication_reason")?;
        }
        if !columns.contains("publication_last_published_at") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN publication_last_published_at TEXT",
                    [],
                )
                .context("failed adding discovery_strategy_state.publication_last_published_at")?;
        }
        if !columns.contains("publication_last_published_window_start") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN publication_last_published_window_start TEXT",
                    [],
                )
                .context(
                    "failed adding discovery_strategy_state.publication_last_published_window_start",
                )?;
        }
        if !columns.contains("publication_scoring_source") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN publication_scoring_source TEXT",
                    [],
                )
                .context("failed adding discovery_strategy_state.publication_scoring_source")?;
        }
        if !columns.contains("publication_wallet_ids_json") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN publication_wallet_ids_json TEXT",
                    [],
                )
                .context("failed adding discovery_strategy_state.publication_wallet_ids_json")?;
        }
        if !columns.contains("publication_policy_fingerprint") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN publication_policy_fingerprint TEXT",
                    [],
                )
                .context("failed adding discovery_strategy_state.publication_policy_fingerprint")?;
        }
        if !columns.contains("publication_runtime_cursor_ts") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN publication_runtime_cursor_ts TEXT",
                    [],
                )
                .context("failed adding discovery_strategy_state.publication_runtime_cursor_ts")?;
        }
        if !columns.contains("publication_runtime_cursor_slot") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN publication_runtime_cursor_slot INTEGER",
                    [],
                )
                .context(
                    "failed adding discovery_strategy_state.publication_runtime_cursor_slot",
                )?;
        }
        if !columns.contains("publication_runtime_cursor_signature") {
            self.conn
                .execute(
                    "ALTER TABLE discovery_strategy_state
                     ADD COLUMN publication_runtime_cursor_signature TEXT",
                    [],
                )
                .context(
                    "failed adding discovery_strategy_state.publication_runtime_cursor_signature",
                )?;
        }
        Ok(())
    }
}
