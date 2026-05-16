use crate::SqliteDiscoveryStore;
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::TokenQualityCacheRow;
use rusqlite::{params, OptionalExtension};

use crate::observed_row::parse_sqlite_slot;
use crate::observed_timestamp::parse_rfc3339_utc;
use crate::{
    DiscoveryRuntimeCursor, DiscoveryV2QualityEvidenceAggregate, DiscoveryV2QualityPrepareState,
    DiscoveryV2QualityPrepareUpsert,
};

const QUALITY_EVIDENCE_PRUNE_BATCH_ROWS: i64 = 4_096;

pub(crate) fn ensure_discovery_v2_quality_prepare_tables(
    store: &SqliteDiscoveryStore,
) -> Result<()> {
    store
        .conn
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS discovery_v2_quality_prepare_state (
                id INTEGER PRIMARY KEY CHECK(id = 1),
                covered_from_ts TEXT NOT NULL,
                cursor_ts TEXT NOT NULL,
                cursor_slot INTEGER NOT NULL,
                cursor_signature TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS discovery_v2_quality_observed_evidence (
                signature TEXT PRIMARY KEY,
                mint TEXT NOT NULL,
                wallet_id TEXT NOT NULL,
                ts TEXT NOT NULL,
                slot INTEGER NOT NULL,
                sol_notional REAL NOT NULL,
                is_buy INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_discovery_v2_quality_evidence_ts
                ON discovery_v2_quality_observed_evidence(ts, slot, signature);
            CREATE INDEX IF NOT EXISTS idx_discovery_v2_quality_evidence_mint_ts
                ON discovery_v2_quality_observed_evidence(mint, ts);",
        )
        .context("failed ensuring discovery v2 quality prepare tables")?;
    Ok(())
}

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
        self.execute_with_retry(|conn| {
            conn.execute(
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
            )
        })?;
        Ok(())
    }

    pub fn begin_discovery_v2_quality_prepare_update(&self) -> Result<()> {
        self.begin_immediate_transaction_with_operator_retry("discovery v2 quality prepare")
    }

    pub fn commit_discovery_v2_quality_prepare_update(&self) -> Result<()> {
        self.conn
            .execute_batch("COMMIT")
            .context("failed committing discovery v2 quality prepare transaction")?;
        Ok(())
    }

    pub fn rollback_discovery_v2_quality_prepare_update(&self) {
        let _ = self.conn.execute_batch("ROLLBACK");
    }

    pub fn discovery_v2_quality_prepare_state(
        &self,
    ) -> Result<Option<DiscoveryV2QualityPrepareState>> {
        if !self.sqlite_table_exists("discovery_v2_quality_prepare_state")? {
            return Ok(None);
        }
        let raw = self
            .conn
            .query_row(
                "SELECT covered_from_ts, cursor_ts, cursor_slot, cursor_signature, updated_at
                 FROM discovery_v2_quality_prepare_state
                 WHERE id = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
            .optional()
            .context("failed loading discovery v2 quality prepare state")?;
        raw.map(quality_prepare_state_row).transpose()
    }

    pub fn clear_discovery_v2_quality_prepare_state(&self) -> Result<()> {
        self.execute_with_retry(|conn| {
            conn.execute("DELETE FROM discovery_v2_quality_prepare_state", [])
        })
        .context("failed clearing discovery v2 quality prepare state")?;
        self.execute_with_retry(|conn| {
            conn.execute("DELETE FROM discovery_v2_quality_observed_evidence", [])
        })
        .context("failed clearing discovery v2 quality observed evidence")?;
        Ok(())
    }

    pub fn prune_discovery_v2_quality_observed_evidence(
        &self,
        window_start: DateTime<Utc>,
    ) -> Result<usize> {
        let cutoff = window_start.to_rfc3339();
        let mut total = 0usize;
        loop {
            let deleted = self
                .execute_with_retry(|conn| {
                    conn.execute(
                        "DELETE FROM discovery_v2_quality_observed_evidence
                         WHERE rowid IN (
                            SELECT rowid
                            FROM discovery_v2_quality_observed_evidence
                            WHERE ts < ?1
                            ORDER BY ts, slot, signature
                            LIMIT ?2
                         )",
                        params![cutoff.as_str(), QUALITY_EVIDENCE_PRUNE_BATCH_ROWS],
                    )
                })
                .context("failed pruning discovery v2 quality observed evidence")?;
            if deleted == 0 {
                break;
            }
            total = total.saturating_add(deleted);
            if deleted < QUALITY_EVIDENCE_PRUNE_BATCH_ROWS as usize {
                break;
            }
        }
        Ok(total)
    }

    pub fn insert_discovery_v2_quality_observed_evidence(
        &self,
        evidence: &DiscoveryV2QualityPrepareUpsert,
    ) -> Result<bool> {
        let changed = self
            .conn
            .execute(
                "INSERT OR IGNORE INTO discovery_v2_quality_observed_evidence(
                    signature, mint, wallet_id, ts, slot, sol_notional, is_buy
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    evidence.signature.as_str(),
                    evidence.mint.as_str(),
                    evidence.wallet_id.as_str(),
                    evidence.ts_utc.to_rfc3339(),
                    evidence.slot as i64,
                    evidence.sol_notional,
                    if evidence.is_buy { 1_i64 } else { 0_i64 },
                ],
            )
            .context("failed inserting discovery v2 quality observed evidence")?;
        Ok(changed > 0)
    }

    pub fn persist_discovery_v2_quality_prepare_state(
        &self,
        covered_from_ts: DateTime<Utc>,
        cursor: &DiscoveryRuntimeCursor,
        updated_at: DateTime<Utc>,
    ) -> Result<()> {
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO discovery_v2_quality_prepare_state(
                    id, covered_from_ts, cursor_ts, cursor_slot, cursor_signature, updated_at
                 ) VALUES (1, ?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(id) DO UPDATE SET
                    covered_from_ts = excluded.covered_from_ts,
                    cursor_ts = excluded.cursor_ts,
                    cursor_slot = excluded.cursor_slot,
                    cursor_signature = excluded.cursor_signature,
                    updated_at = excluded.updated_at",
                params![
                    covered_from_ts.to_rfc3339(),
                    cursor.ts_utc.to_rfc3339(),
                    cursor.slot as i64,
                    cursor.signature.as_str(),
                    updated_at.to_rfc3339(),
                ],
            )
        })
        .context("failed persisting discovery v2 quality prepare state")?;
        Ok(())
    }

    pub fn discovery_v2_quality_observed_evidence_count(
        &self,
        window_start: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<usize> {
        if !self.sqlite_table_exists("discovery_v2_quality_observed_evidence")? {
            return Ok(0);
        }
        let count = self
            .conn
            .query_row(
                "SELECT COUNT(*)
                 FROM discovery_v2_quality_observed_evidence
                 WHERE ts >= ?1 AND ts <= ?2",
                params![window_start.to_rfc3339(), now.to_rfc3339()],
                |row| row.get::<_, i64>(0),
            )
            .context("failed counting discovery v2 quality observed evidence")?;
        Ok(count.max(0) as usize)
    }

    pub fn discovery_v2_quality_observed_evidence_aggregates(
        &self,
        window_start: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<Vec<DiscoveryV2QualityEvidenceAggregate>> {
        if !self.sqlite_table_exists("discovery_v2_quality_observed_evidence")? {
            return Ok(Vec::new());
        }
        let mut stmt = self
            .conn
            .prepare(
                "SELECT mint, MIN(ts), MAX(sol_notional),
                    SUM(CASE WHEN is_buy != 0 THEN 1 ELSE 0 END),
                    COUNT(*),
                    COUNT(DISTINCT wallet_id)
                 FROM discovery_v2_quality_observed_evidence
                 WHERE ts >= ?1 AND ts <= ?2
                 GROUP BY mint
                 HAVING SUM(CASE WHEN is_buy != 0 THEN 1 ELSE 0 END) > 0",
            )
            .context("failed preparing discovery v2 quality evidence aggregate query")?;
        let rows = stmt
            .query_map(
                params![window_start.to_rfc3339(), now.to_rfc3339()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, f64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                    ))
                },
            )
            .context("failed querying discovery v2 quality evidence aggregates")?;
        let mut aggregates = Vec::new();
        for row in rows {
            let (mint, first_seen, max_sol_notional, buy_count, sol_trade_count, wallet_count) =
                row.context("failed reading discovery v2 quality evidence aggregate row")?;
            aggregates.push(DiscoveryV2QualityEvidenceAggregate {
                mint,
                first_seen: parse_rfc3339_utc(
                    &first_seen,
                    "discovery_v2_quality_observed_evidence.ts",
                )?,
                max_sol_notional,
                buy_count: buy_count.max(0) as u64,
                sol_trade_count: sol_trade_count.max(0) as u64,
                wallet_count: wallet_count.max(0) as u64,
            });
        }
        Ok(aggregates)
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

fn quality_prepare_state_row(
    row: (String, String, i64, String, String),
) -> Result<DiscoveryV2QualityPrepareState> {
    let (covered_from_ts, cursor_ts, cursor_slot, cursor_signature, updated_at) = row;
    Ok(DiscoveryV2QualityPrepareState {
        covered_from_ts: parse_rfc3339_utc(
            &covered_from_ts,
            "discovery_v2_quality_prepare_state.covered_from_ts",
        )?,
        cursor: DiscoveryRuntimeCursor {
            ts_utc: parse_rfc3339_utc(&cursor_ts, "discovery_v2_quality_prepare_state.cursor_ts")?,
            slot: parse_sqlite_slot(
                cursor_slot,
                "discovery_v2_quality_prepare_state.cursor_slot",
            )?,
            signature: cursor_signature,
        },
        updated_at: parse_rfc3339_utc(
            &updated_at,
            "discovery_v2_quality_prepare_state.updated_at",
        )?,
    })
}
