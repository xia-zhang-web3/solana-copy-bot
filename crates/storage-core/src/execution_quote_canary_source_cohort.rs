use crate::{execution_quote_canary::ensure_execution_quote_canary_tables, SqliteDiscoveryStore};
use anyhow::{Context, Result};
use rusqlite::{params, OptionalExtension};
use std::collections::HashMap;

impl SqliteDiscoveryStore {
    pub fn load_execution_quote_canary_source_cohorts(
        &self,
        wallet_ids: &[String],
    ) -> Result<HashMap<String, String>> {
        if wallet_ids.is_empty() || !self.sqlite_table_exists("discovery_candidate_sources")? {
            return Ok(HashMap::new());
        }
        let mut stmt = self
            .conn
            .prepare(
                "SELECT source_cohort
                 FROM discovery_candidate_sources
                 WHERE wallet_id = ?1
                 LIMIT 1",
            )
            .context("failed preparing execution quote canary source cohort lookup")?;
        let mut out = HashMap::new();
        for wallet_id in wallet_ids {
            if let Some(source) = stmt
                .query_row(params![wallet_id], |row| row.get::<_, String>(0))
                .optional()
                .with_context(|| {
                    format!("failed loading execution quote canary source cohort for {wallet_id}")
                })?
            {
                out.insert(wallet_id.clone(), source);
            }
        }
        Ok(out)
    }

    pub fn stamp_execution_quote_canary_source_cohort(
        &self,
        event_id: &str,
        source_cohort: &str,
    ) -> Result<bool> {
        ensure_execution_quote_canary_tables(self)?;
        let updated = self
            .execute_with_retry(|conn| {
                conn.execute(
                    "UPDATE execution_quote_canary_events
                     SET source_cohort = ?2
                     WHERE event_id = ?1",
                    params![event_id, source_cohort],
                )
            })
            .with_context(|| {
                format!("failed stamping execution quote canary source cohort for {event_id}")
            })?;
        Ok(updated > 0)
    }
}
