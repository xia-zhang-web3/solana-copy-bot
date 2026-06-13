use crate::{RugWalletQuarantineRow, RugWalletQuarantineUpsert, SqliteDiscoveryStore};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::params;

impl SqliteDiscoveryStore {
    pub fn active_rug_wallet_quarantines(
        &self,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<Vec<RugWalletQuarantineRow>> {
        if !self.sqlite_table_exists("discovery_v2_wallet_quarantine")? {
            return Ok(Vec::new());
        }
        active_rug_wallet_quarantines_on_conn(&self.conn, reason, now)
    }

    pub fn upsert_rug_wallet_quarantines(&self, rows: &[RugWalletQuarantineUpsert]) -> Result<()> {
        crate::schema::ensure_discovery_v2_schema(self)?;
        upsert_rug_wallet_quarantines_on_conn(&self.conn, rows)
    }
}

pub(crate) fn active_rug_wallet_quarantines_on_conn(
    conn: &rusqlite::Connection,
    reason: &str,
    now: DateTime<Utc>,
) -> Result<Vec<RugWalletQuarantineRow>> {
    let mut stmt = conn
        .prepare(
            "SELECT wallet_id, reason, first_rejected_at, last_rejected_at,
                    quarantine_until, evidence_json
             FROM discovery_v2_wallet_quarantine
             WHERE reason = ?1 AND quarantine_until > ?2
             ORDER BY wallet_id",
        )
        .context("failed preparing active rug wallet quarantine query")?;
    let rows = stmt.query_map(params![reason, now.to_rfc3339()], |row| {
        Ok(RugWalletQuarantineRow {
            wallet_id: row.get(0)?,
            reason: row.get(1)?,
            first_rejected_at: parse_ts(row.get::<_, String>(2)?)?,
            last_rejected_at: parse_ts(row.get::<_, String>(3)?)?,
            quarantine_until: parse_ts(row.get::<_, String>(4)?)?,
            evidence_json: row.get(5)?,
        })
    })?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .context("failed reading active rug wallet quarantines")
}

pub(crate) fn upsert_rug_wallet_quarantines_on_conn(
    conn: &rusqlite::Connection,
    rows: &[RugWalletQuarantineUpsert],
) -> Result<()> {
    let mut stmt = conn.prepare_cached(
        "INSERT INTO discovery_v2_wallet_quarantine(
            wallet_id, reason, first_rejected_at, last_rejected_at,
            quarantine_until, evidence_json
         ) VALUES (?1, ?2, ?3, ?3, ?4, ?5)
         ON CONFLICT(wallet_id, reason) DO UPDATE SET
            first_rejected_at =
                CASE
                    WHEN excluded.first_rejected_at < discovery_v2_wallet_quarantine.first_rejected_at
                    THEN excluded.first_rejected_at
                    ELSE discovery_v2_wallet_quarantine.first_rejected_at
                END,
            last_rejected_at =
                CASE
                    WHEN excluded.last_rejected_at > discovery_v2_wallet_quarantine.last_rejected_at
                    THEN excluded.last_rejected_at
                    ELSE discovery_v2_wallet_quarantine.last_rejected_at
                END,
            quarantine_until =
                CASE
                    WHEN excluded.quarantine_until > discovery_v2_wallet_quarantine.quarantine_until
                    THEN excluded.quarantine_until
                    ELSE discovery_v2_wallet_quarantine.quarantine_until
                END,
            evidence_json = excluded.evidence_json",
    )?;
    for row in rows {
        stmt.execute(params![
            &row.wallet_id,
            &row.reason,
            row.rejected_at.to_rfc3339(),
            row.quarantine_until.to_rfc3339(),
            &row.evidence_json,
        ])?;
    }
    Ok(())
}

fn parse_ts(raw: String) -> rusqlite::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(&raw)
        .map(|ts| ts.with_timezone(&Utc))
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })
}
