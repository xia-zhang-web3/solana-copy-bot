use crate::SqliteDiscoveryStore;
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
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
        raw.map(token_quality_cache_row).transpose()
    }

    pub fn fresh_token_quality_cache_read_only(
        &self,
        now: DateTime<Utc>,
        ttl: Duration,
    ) -> Result<Vec<TokenQualityCacheRow>> {
        if !self.sqlite_table_exists("token_quality_cache")? {
            return Ok(Vec::new());
        }
        let cutoff = now - ttl;
        let mut stmt = self
            .conn
            .prepare(
                "SELECT mint, holders, liquidity_sol, token_age_seconds, fetched_at
                 FROM token_quality_cache
                 WHERE fetched_at >= ?1",
            )
            .context("failed preparing fresh token quality cache query")?;
        let rows = stmt
            .query_map([cutoff.to_rfc3339()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, Option<f64>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })
            .context("failed querying fresh token quality cache")?;
        let mut fresh = Vec::new();
        for row in rows {
            let parsed = token_quality_cache_row(row.context("failed reading token quality row")?)?;
            if parsed.fetched_at <= now && now - parsed.fetched_at <= ttl {
                fresh.push(parsed);
            }
        }
        Ok(fresh)
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

fn token_quality_cache_row(
    row: (String, Option<i64>, Option<f64>, Option<i64>, String),
) -> Result<TokenQualityCacheRow> {
    let (mint, holders, liquidity_sol, token_age_seconds, fetched_at) = row;
    Ok(TokenQualityCacheRow {
        mint,
        holders: holders.map(|value| value.max(0) as u64),
        liquidity_sol,
        token_age_seconds: token_age_seconds.map(|value| value.max(0) as u64),
        fetched_at: DateTime::parse_from_rfc3339(&fetched_at)
            .map(|value| value.with_timezone(&Utc))
            .with_context(|| format!("invalid token_quality_cache.fetched_at: {fetched_at}"))?,
    })
}
