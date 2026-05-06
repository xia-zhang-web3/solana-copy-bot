use crate::{RiskEventRow, SqliteDiscoveryStore};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, OptionalExtension};

impl SqliteDiscoveryStore {
    pub fn ensure_system_event_tables(&self) -> Result<()> {
        self.execute_with_retry_result(|conn| {
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS risk_events (
                    event_id TEXT PRIMARY KEY,
                    type TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    details_json TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_risk_events_type
                    ON risk_events(type);
                CREATE TABLE IF NOT EXISTS system_heartbeat (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    component TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    status TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS alert_delivery_state (
                    channel TEXT PRIMARY KEY,
                    last_rowid INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                );",
            )
        })
        .context("failed ensuring system event tables exist")?;
        Ok(())
    }

    pub fn record_heartbeat(&self, component: &str, status: &str) -> Result<()> {
        self.ensure_system_event_tables()?;
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO system_heartbeat(component, ts, status)
                 VALUES (?1, datetime('now'), ?2)",
                params![component, status],
            )
        })
        .context("failed to record heartbeat")?;
        Ok(())
    }

    pub fn insert_risk_event(
        &self,
        event_type: &str,
        severity: &str,
        ts: DateTime<Utc>,
        details_json: Option<&str>,
    ) -> Result<()> {
        self.ensure_system_event_tables()?;
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO risk_events(event_id, type, severity, ts, details_json)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    uuid::Uuid::new_v4().to_string(),
                    event_type,
                    severity,
                    ts.to_rfc3339(),
                    details_json,
                ],
            )
        })
        .context("failed to insert risk event")?;
        Ok(())
    }

    pub fn risk_event_count_by_type(&self, event_type: &str) -> Result<u64> {
        let count: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM risk_events WHERE type = ?1",
                params![event_type],
                |row| row.get(0),
            )
            .context("failed to count risk events by type")?;
        Ok(count.max(0) as u64)
    }

    pub fn latest_risk_event_by_type(&self, event_type: &str) -> Result<Option<RiskEventRow>> {
        self.conn
            .query_row(
                "SELECT rowid, event_id, type, severity, ts, details_json
                 FROM risk_events
                 WHERE type = ?1
                 ORDER BY rowid DESC
                 LIMIT 1",
                params![event_type],
                risk_event_from_row,
            )
            .optional()
            .context("failed to load latest risk event by type")
    }

    pub fn list_risk_events_by_type_desc(&self, event_type: &str) -> Result<Vec<RiskEventRow>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT rowid, event_id, type, severity, ts, details_json
                 FROM risk_events
                 WHERE type = ?1
                 ORDER BY rowid DESC",
            )
            .context("failed to prepare risk events by type query")?;
        let rows = stmt
            .query_map(params![event_type], risk_event_from_row)
            .context("failed to query risk events by type")?;
        collect_risk_event_rows(rows, "failed iterating risk events by type")
    }

    pub fn list_risk_events_after_cursor(
        &self,
        cursor: Option<i64>,
        limit: u32,
    ) -> Result<Vec<RiskEventRow>> {
        let query_with_cursor = "SELECT rowid, event_id, type, severity, ts, details_json
             FROM risk_events
             WHERE severity IN ('warn', 'error')
               AND rowid > ?1
             ORDER BY rowid ASC
             LIMIT ?2";
        let query_initial = "SELECT rowid, event_id, type, severity, ts, details_json
             FROM risk_events
             WHERE severity IN ('warn', 'error')
             ORDER BY rowid ASC
             LIMIT ?1";
        match cursor {
            Some(last_rowid) => {
                let mut stmt = self
                    .conn
                    .prepare(query_with_cursor)
                    .context("failed to prepare risk events cursor query")?;
                let rows = stmt
                    .query_map(params![last_rowid, limit], risk_event_from_row)
                    .context("failed to query risk events after cursor")?;
                collect_risk_event_rows(rows, "failed to iterate risk events after cursor")
            }
            None => {
                let mut stmt = self
                    .conn
                    .prepare(query_initial)
                    .context("failed to prepare initial risk events query")?;
                let rows = stmt
                    .query_map(params![limit], risk_event_from_row)
                    .context("failed to query initial risk events")?;
                collect_risk_event_rows(rows, "failed to iterate initial risk events")
            }
        }
    }

    pub fn load_alert_delivery_cursor(&self, channel: &str) -> Result<Option<i64>> {
        self.conn
            .query_row(
                "SELECT last_rowid
                 FROM alert_delivery_state
                 WHERE channel = ?1",
                params![channel],
                |row| row.get(0),
            )
            .optional()
            .context("failed to load alert delivery cursor")
    }

    pub fn upsert_alert_delivery_cursor(&self, channel: &str, last_rowid: i64) -> Result<()> {
        self.ensure_system_event_tables()?;
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO alert_delivery_state(channel, last_rowid, updated_at)
                 VALUES (?1, ?2, datetime('now'))
                 ON CONFLICT(channel) DO UPDATE SET
                     last_rowid = excluded.last_rowid,
                     updated_at = datetime('now')",
                params![channel, last_rowid],
            )
        })
        .context("failed to upsert alert delivery cursor")?;
        Ok(())
    }

    pub fn ensure_alert_delivery_cursor(&self, channel: &str) -> Result<()> {
        self.ensure_system_event_tables()?;
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO alert_delivery_state(channel, last_rowid, updated_at)
                 VALUES (?1, 0, datetime('now'))
                 ON CONFLICT(channel) DO NOTHING",
                params![channel],
            )
        })
        .context("failed to ensure alert delivery cursor")?;
        Ok(())
    }
}

fn risk_event_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RiskEventRow> {
    Ok(RiskEventRow {
        rowid: row.get(0)?,
        event_id: row.get(1)?,
        event_type: row.get(2)?,
        severity: row.get(3)?,
        ts: row.get(4)?,
        details_json: row.get(5)?,
    })
}

fn collect_risk_event_rows<F>(
    rows: rusqlite::MappedRows<'_, F>,
    context: &'static str,
) -> Result<Vec<RiskEventRow>>
where
    F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<RiskEventRow>,
{
    let mut events = Vec::new();
    for row in rows {
        events.push(row.context(context)?);
    }
    Ok(events)
}
