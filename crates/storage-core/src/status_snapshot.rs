use crate::observed_timestamp::parse_rfc3339_utc;
use crate::{DiscoveryRuntimeCursor, SqliteDiscoveryStore};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, OptionalExtension};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveryV2StatusSnapshotRow {
    pub policy_fingerprint: String,
    pub status_now: DateTime<Utc>,
    pub status_window_start: DateTime<Utc>,
    pub runtime_cursor: Option<DiscoveryRuntimeCursor>,
    pub status_json: String,
    pub updated_at: DateTime<Utc>,
}

impl SqliteDiscoveryStore {
    pub fn persist_discovery_v2_status_snapshot(
        &self,
        policy_fingerprint: &str,
        status_now: DateTime<Utc>,
        status_window_start: DateTime<Utc>,
        runtime_cursor: Option<&DiscoveryRuntimeCursor>,
        status_json: &str,
    ) -> Result<()> {
        ensure_discovery_v2_status_snapshot_table(self)?;
        let (cursor_ts, cursor_slot, cursor_signature) = snapshot_cursor_values(runtime_cursor);
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO discovery_v2_status_snapshot(
                    id, policy_fingerprint, status_now, status_window_start,
                    runtime_cursor_ts, runtime_cursor_slot, runtime_cursor_signature,
                    status_json, updated_at
                 ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(id) DO UPDATE SET
                    policy_fingerprint = excluded.policy_fingerprint,
                    status_now = excluded.status_now,
                    status_window_start = excluded.status_window_start,
                    runtime_cursor_ts = excluded.runtime_cursor_ts,
                    runtime_cursor_slot = excluded.runtime_cursor_slot,
                    runtime_cursor_signature = excluded.runtime_cursor_signature,
                    status_json = excluded.status_json,
                    updated_at = excluded.updated_at",
                params![
                    policy_fingerprint,
                    status_now.to_rfc3339(),
                    status_window_start.to_rfc3339(),
                    cursor_ts.as_deref(),
                    cursor_slot,
                    cursor_signature,
                    status_json,
                    Utc::now().to_rfc3339(),
                ],
            )
        })
        .context("failed persisting discovery v2 status snapshot")?;
        Ok(())
    }

    pub fn discovery_v2_status_snapshot_read_only(
        &self,
    ) -> Result<Option<DiscoveryV2StatusSnapshotRow>> {
        if !self.sqlite_table_exists("discovery_v2_status_snapshot")? {
            return Ok(None);
        }
        for column in [
            "id",
            "policy_fingerprint",
            "status_now",
            "status_window_start",
            "runtime_cursor_ts",
            "runtime_cursor_slot",
            "runtime_cursor_signature",
            "status_json",
            "updated_at",
        ] {
            if !crate::schema::column_exists(self, "discovery_v2_status_snapshot", column)? {
                anyhow::bail!(
                    "discovery v2 status snapshot schema missing required column: {column}"
                );
            }
        }
        let raw = self
            .conn
            .query_row(
                "SELECT policy_fingerprint, status_now, status_window_start,
                    runtime_cursor_ts, runtime_cursor_slot, runtime_cursor_signature,
                    status_json, updated_at
                 FROM discovery_v2_status_snapshot
                 WHERE id = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<i64>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                    ))
                },
            )
            .optional()
            .context("failed loading discovery v2 status snapshot")?;
        raw.map(row_to_status_snapshot).transpose()
    }
}

pub(crate) fn ensure_discovery_v2_status_snapshot_table(
    store: &SqliteDiscoveryStore,
) -> Result<()> {
    store
        .execute_with_retry_result(|conn| {
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS discovery_v2_status_snapshot (
                id INTEGER PRIMARY KEY CHECK(id = 1),
                policy_fingerprint TEXT NOT NULL,
                status_now TEXT NOT NULL,
                status_window_start TEXT NOT NULL,
                runtime_cursor_ts TEXT,
                runtime_cursor_slot INTEGER,
                runtime_cursor_signature TEXT,
                status_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )",
            )
        })
        .context("failed ensuring discovery_v2_status_snapshot table")?;
    for (column, definition) in [
        ("policy_fingerprint", "TEXT NOT NULL DEFAULT ''"),
        (
            "status_now",
            "TEXT NOT NULL DEFAULT '1970-01-01T00:00:00+00:00'",
        ),
        (
            "status_window_start",
            "TEXT NOT NULL DEFAULT '1970-01-01T00:00:00+00:00'",
        ),
        ("runtime_cursor_ts", "TEXT"),
        ("runtime_cursor_slot", "INTEGER"),
        ("runtime_cursor_signature", "TEXT"),
        ("status_json", "TEXT NOT NULL DEFAULT ''"),
        (
            "updated_at",
            "TEXT NOT NULL DEFAULT '1970-01-01T00:00:00+00:00'",
        ),
    ] {
        crate::schema::ensure_column(store, "discovery_v2_status_snapshot", column, definition)?;
    }
    Ok(())
}

fn row_to_status_snapshot(
    row: (
        String,
        String,
        String,
        Option<String>,
        Option<i64>,
        Option<String>,
        String,
        String,
    ),
) -> Result<DiscoveryV2StatusSnapshotRow> {
    if row.0.trim().is_empty() {
        anyhow::bail!("discovery v2 status snapshot policy fingerprint is empty");
    }
    if row.6.trim().is_empty() {
        anyhow::bail!("discovery v2 status snapshot json is empty");
    }
    Ok(DiscoveryV2StatusSnapshotRow {
        policy_fingerprint: row.0,
        status_now: parse_rfc3339_utc(&row.1, "discovery_v2_status_snapshot.status_now")?,
        status_window_start: parse_rfc3339_utc(
            &row.2,
            "discovery_v2_status_snapshot.status_window_start",
        )?,
        runtime_cursor: parse_optional_snapshot_cursor(row.3, row.4, row.5)?,
        status_json: row.6,
        updated_at: parse_rfc3339_utc(&row.7, "discovery_v2_status_snapshot.updated_at")?,
    })
}

fn parse_optional_snapshot_cursor(
    ts: Option<String>,
    slot: Option<i64>,
    signature: Option<String>,
) -> Result<Option<DiscoveryRuntimeCursor>> {
    match (ts, slot, signature) {
        (None, None, None) => Ok(None),
        (Some(ts), Some(slot), Some(signature)) => {
            if slot < 0 {
                return Err(anyhow!(
                    "invalid negative discovery_v2_status_snapshot.runtime_cursor_slot: {slot}"
                ));
            }
            Ok(Some(DiscoveryRuntimeCursor {
                ts_utc: parse_rfc3339_utc(&ts, "discovery_v2_status_snapshot.runtime_cursor_ts")?,
                slot: slot as u64,
                signature,
            }))
        }
        _ => Err(anyhow!(
            "discovery v2 status snapshot cursor columns must be all set or all empty"
        )),
    }
}

fn snapshot_cursor_values(
    cursor: Option<&DiscoveryRuntimeCursor>,
) -> (Option<String>, Option<i64>, Option<&str>) {
    match cursor {
        Some(cursor) => (
            Some(cursor.ts_utc.to_rfc3339()),
            Some(cursor.slot as i64),
            Some(cursor.signature.as_str()),
        ),
        None => (None, None, None),
    }
}
