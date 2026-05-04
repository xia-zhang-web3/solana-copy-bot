use crate::SqliteDiscoveryStore;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::TokenQualityCacheRow;
use rusqlite::{params, OptionalExtension};

impl SqliteDiscoveryStore {
    pub fn get_token_quality_cache(&self, mint: &str) -> Result<Option<TokenQualityCacheRow>> {
        if !self.sqlite_table_exists("token_quality_cache")? {
            return Ok(None);
        }
        let raw = self
            .conn
            .query_row(
                "SELECT mint, holders, liquidity_sol, token_age_seconds, fetched_at
                 FROM token_quality_cache
                 WHERE mint = ?1",
                [mint],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, Option<f64>>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
            .optional()
            .context("failed querying token quality cache")?;
        raw.map(
            |(mint, holders, liquidity_sol, token_age_seconds, fetched_at)| {
                Ok(TokenQualityCacheRow {
                    mint,
                    holders: holders.map(|value| value.max(0) as u64),
                    liquidity_sol,
                    token_age_seconds: token_age_seconds.map(|value| value.max(0) as u64),
                    fetched_at: DateTime::parse_from_rfc3339(&fetched_at)
                        .map(|value| value.with_timezone(&Utc))
                        .with_context(|| {
                            format!("invalid token_quality_cache.fetched_at: {fetched_at}")
                        })?,
                })
            },
        )
        .transpose()
    }

    pub fn upsert_token_quality_cache(
        &self,
        mint: &str,
        holders: Option<u64>,
        liquidity_sol: Option<f64>,
        token_age_seconds: Option<u64>,
        fetched_at: DateTime<Utc>,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO token_quality_cache(mint, holders, liquidity_sol, token_age_seconds, fetched_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(mint) DO UPDATE SET
                holders = excluded.holders,
                liquidity_sol = excluded.liquidity_sol,
                token_age_seconds = excluded.token_age_seconds,
                fetched_at = excluded.fetched_at",
            params![
                mint,
                holders.map(|value| value as i64),
                liquidity_sol,
                token_age_seconds.map(|value| value as i64),
                fetched_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }
}
