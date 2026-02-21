use super::SqliteStore;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::params;

impl SqliteStore {
    pub fn record_heartbeat(&self, component: &str, status: &str) -> Result<()> {
        self.conn
            .execute(
                "INSERT INTO system_heartbeat(component, ts, status) VALUES (?1, datetime('now'), ?2)",
                params![component, status],
            )
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
}
