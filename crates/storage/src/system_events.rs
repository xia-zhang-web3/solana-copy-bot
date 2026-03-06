use super::SqliteStore;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, OptionalExtension};

#[derive(Debug, Clone)]
pub struct RiskEventRow {
    pub event_id: String,
    pub event_type: String,
    pub severity: String,
    pub ts: String,
    pub details_json: Option<String>,
}

impl SqliteStore {
    pub fn record_heartbeat(&self, component: &str, status: &str) -> Result<()> {
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO system_heartbeat(component, ts, status) VALUES (?1, datetime('now'), ?2)",
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

    pub fn list_risk_events_after_cursor(
        &self,
        cursor: Option<(&str, &str)>,
        limit: u32,
    ) -> Result<Vec<RiskEventRow>> {
        let mut events = Vec::new();
        if let Some((last_ts, last_event_id)) = cursor {
            let mut stmt = self
                .conn
                .prepare(
                    "SELECT event_id, type, severity, ts, details_json
                     FROM risk_events
                     WHERE severity IN ('warn', 'error')
                       AND (ts > ?1 OR (ts = ?1 AND event_id > ?2))
                     ORDER BY ts ASC, event_id ASC
                     LIMIT ?3",
                )
                .context("failed to prepare risk events cursor query")?;
            let mut rows = stmt
                .query(params![last_ts, last_event_id, limit])
                .context("failed to query risk events after cursor")?;
            while let Some(row) = rows
                .next()
                .context("failed to iterate risk events after cursor")?
            {
                events.push(RiskEventRow {
                    event_id: row.get(0)?,
                    event_type: row.get(1)?,
                    severity: row.get(2)?,
                    ts: row.get(3)?,
                    details_json: row.get(4)?,
                });
            }
        } else {
            let mut stmt = self
                .conn
                .prepare(
                    "SELECT event_id, type, severity, ts, details_json
                     FROM risk_events
                     WHERE severity IN ('warn', 'error')
                     ORDER BY ts ASC, event_id ASC
                     LIMIT ?1",
                )
                .context("failed to prepare initial risk events query")?;
            let mut rows = stmt
                .query(params![limit])
                .context("failed to query initial risk events")?;
            while let Some(row) = rows
                .next()
                .context("failed to iterate initial risk events")?
            {
                events.push(RiskEventRow {
                    event_id: row.get(0)?,
                    event_type: row.get(1)?,
                    severity: row.get(2)?,
                    ts: row.get(3)?,
                    details_json: row.get(4)?,
                });
            }
        }
        Ok(events)
    }

    pub fn load_alert_delivery_cursor(&self, channel: &str) -> Result<Option<(String, String)>> {
        self.conn
            .query_row(
                "SELECT last_ts, last_event_id
                 FROM alert_delivery_state
                 WHERE channel = ?1",
                params![channel],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .context("failed to load alert delivery cursor")
    }

    pub fn upsert_alert_delivery_cursor(
        &self,
        channel: &str,
        last_ts: &str,
        last_event_id: &str,
    ) -> Result<()> {
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO alert_delivery_state(channel, last_ts, last_event_id, updated_at)
                 VALUES (?1, ?2, ?3, datetime('now'))
                 ON CONFLICT(channel) DO UPDATE SET
                     last_ts = excluded.last_ts,
                     last_event_id = excluded.last_event_id,
                     updated_at = datetime('now')",
                params![channel, last_ts, last_event_id],
            )
        })
        .context("failed to upsert alert delivery cursor")?;
        Ok(())
    }
}
