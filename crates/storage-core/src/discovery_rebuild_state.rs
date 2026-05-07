use crate::{DiscoveryPersistedRebuildPhase, DiscoveryPersistedRebuildStateRow};
use crate::{DiscoveryRuntimeCursor, SqliteDiscoveryStore};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::OptionalExtension;

impl SqliteDiscoveryStore {
    pub fn upsert_discovery_persisted_rebuild_state(
        &self,
        state: &DiscoveryPersistedRebuildStateRow,
    ) -> Result<()> {
        self.ensure_discovery_persisted_rebuild_state_table()?;
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO discovery_persisted_rebuild_state(
                    id, phase, window_start, horizon_end, metrics_window_start,
                    phase_cursor_ts, phase_cursor_slot, phase_cursor_signature,
                    prepass_rows_processed, prepass_pages_processed,
                    replay_rows_processed, replay_pages_processed, chunks_completed,
                    state_json, started_at, updated_at
                 ) VALUES (
                    1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15
                 )
                 ON CONFLICT(id) DO UPDATE SET
                    phase = excluded.phase,
                    window_start = excluded.window_start,
                    horizon_end = excluded.horizon_end,
                    metrics_window_start = excluded.metrics_window_start,
                    phase_cursor_ts = excluded.phase_cursor_ts,
                    phase_cursor_slot = excluded.phase_cursor_slot,
                    phase_cursor_signature = excluded.phase_cursor_signature,
                    prepass_rows_processed = excluded.prepass_rows_processed,
                    prepass_pages_processed = excluded.prepass_pages_processed,
                    replay_rows_processed = excluded.replay_rows_processed,
                    replay_pages_processed = excluded.replay_pages_processed,
                    chunks_completed = excluded.chunks_completed,
                    state_json = excluded.state_json,
                    started_at = excluded.started_at,
                    updated_at = excluded.updated_at",
                rusqlite::params![
                    state.phase.as_str(),
                    state.window_start.to_rfc3339(),
                    state.horizon_end.to_rfc3339(),
                    state.metrics_window_start.to_rfc3339(),
                    state
                        .phase_cursor
                        .as_ref()
                        .map(|cursor| cursor.ts_utc.to_rfc3339()),
                    state.phase_cursor.as_ref().map(|cursor| cursor.slot as i64),
                    state
                        .phase_cursor
                        .as_ref()
                        .map(|cursor| cursor.signature.as_str()),
                    state.prepass_rows_processed as i64,
                    state.prepass_pages_processed as i64,
                    state.replay_rows_processed as i64,
                    state.replay_pages_processed as i64,
                    state.chunks_completed as i64,
                    &state.state_json,
                    state.started_at.to_rfc3339(),
                    state.updated_at.to_rfc3339(),
                ],
            )
        })
        .context("failed updating discovery persisted rebuild state")?;
        Ok(())
    }

    pub fn load_discovery_persisted_rebuild_state_read_only(
        &self,
    ) -> Result<Option<DiscoveryPersistedRebuildStateRow>> {
        if !self.sqlite_table_exists("discovery_persisted_rebuild_state")? {
            return Ok(None);
        }
        self.load_discovery_persisted_rebuild_state_query()
    }

    fn load_discovery_persisted_rebuild_state_query(
        &self,
    ) -> Result<Option<DiscoveryPersistedRebuildStateRow>> {
        let raw = self
            .conn
            .query_row(
                "SELECT
                    phase,
                    window_start,
                    horizon_end,
                    metrics_window_start,
                    phase_cursor_ts,
                    phase_cursor_slot,
                    phase_cursor_signature,
                    prepass_rows_processed,
                    prepass_pages_processed,
                    replay_rows_processed,
                    replay_pages_processed,
                    chunks_completed,
                    state_json,
                    started_at,
                    updated_at
                 FROM discovery_persisted_rebuild_state
                 WHERE id = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, i64>(7)?,
                        row.get::<_, i64>(8)?,
                        row.get::<_, i64>(9)?,
                        row.get::<_, i64>(10)?,
                        row.get::<_, i64>(11)?,
                        row.get::<_, String>(12)?,
                        row.get::<_, String>(13)?,
                        row.get::<_, String>(14)?,
                    ))
                },
            )
            .optional()
            .context("failed reading discovery persisted rebuild state")?;
        raw.map(
            |(
                phase_raw,
                window_start_raw,
                horizon_end_raw,
                metrics_window_start_raw,
                cursor_ts_raw,
                cursor_slot_raw,
                cursor_signature,
                prepass_rows_processed,
                prepass_pages_processed,
                replay_rows_processed,
                replay_pages_processed,
                chunks_completed,
                state_json,
                started_at_raw,
                updated_at_raw,
            )| {
                let phase_cursor = match (cursor_ts_raw, cursor_slot_raw, cursor_signature) {
                    (None, None, None) => None,
                    (Some(ts_raw), Some(slot_raw), Some(signature)) => {
                        Some(DiscoveryRuntimeCursor {
                            ts_utc: parse_rfc3339_utc(
                                &ts_raw,
                                "discovery_persisted_rebuild_state.phase_cursor_ts",
                            )?,
                            slot: slot_raw.max(0) as u64,
                            signature,
                        })
                    }
                    _ => {
                        return Err(anyhow!(
                            "discovery_persisted_rebuild_state contains partial phase cursor state"
                        ));
                    }
                };
                Ok(DiscoveryPersistedRebuildStateRow {
                    phase: DiscoveryPersistedRebuildPhase::parse(&phase_raw)?,
                    window_start: parse_rfc3339_utc(
                        &window_start_raw,
                        "discovery_persisted_rebuild_state.window_start",
                    )?,
                    horizon_end: parse_rfc3339_utc(
                        &horizon_end_raw,
                        "discovery_persisted_rebuild_state.horizon_end",
                    )?,
                    metrics_window_start: parse_rfc3339_utc(
                        &metrics_window_start_raw,
                        "discovery_persisted_rebuild_state.metrics_window_start",
                    )?,
                    phase_cursor,
                    prepass_rows_processed: prepass_rows_processed.max(0) as usize,
                    prepass_pages_processed: prepass_pages_processed.max(0) as usize,
                    replay_rows_processed: replay_rows_processed.max(0) as usize,
                    replay_pages_processed: replay_pages_processed.max(0) as usize,
                    chunks_completed: chunks_completed.max(0) as usize,
                    state_json,
                    started_at: parse_rfc3339_utc(
                        &started_at_raw,
                        "discovery_persisted_rebuild_state.started_at",
                    )?,
                    updated_at: parse_rfc3339_utc(
                        &updated_at_raw,
                        "discovery_persisted_rebuild_state.updated_at",
                    )?,
                })
            },
        )
        .transpose()
    }

    fn ensure_discovery_persisted_rebuild_state_table(&self) -> Result<()> {
        self.execute_with_retry(|conn| {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS discovery_persisted_rebuild_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
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
                )",
                [],
            )
        })
        .context("failed ensuring discovery persisted rebuild state table")?;
        Ok(())
    }
}

fn parse_rfc3339_utc(raw: &str, field: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .with_context(|| format!("invalid {field} rfc3339 value: {raw}"))
}
