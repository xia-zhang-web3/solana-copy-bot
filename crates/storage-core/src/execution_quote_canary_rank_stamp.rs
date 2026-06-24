use crate::{
    execution_quote_canary::ensure_execution_quote_canary_tables,
    observed_timestamp::parse_rfc3339_utc, SqliteDiscoveryStore,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::params;
use std::collections::{HashMap, HashSet};

const RANK_WINDOW_SCAN_LIMIT: i64 = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionQuoteCanaryDiscoveryRankStamp {
    pub rank: u64,
    pub cohort: String,
    pub window_start: DateTime<Utc>,
}

impl SqliteDiscoveryStore {
    pub fn load_execution_quote_canary_discovery_rank_stamps(
        &self,
        as_of: DateTime<Utc>,
        wallet_ids: &[String],
    ) -> Result<HashMap<String, ExecutionQuoteCanaryDiscoveryRankStamp>> {
        if wallet_ids.is_empty() || !self.sqlite_table_exists("wallet_metrics")? {
            return Ok(HashMap::new());
        }
        let Some(window_start) = latest_rank_window_at_or_before(self, as_of)? else {
            return Ok(HashMap::new());
        };
        let requested: HashSet<&str> = wallet_ids.iter().map(String::as_str).collect();
        load_rank_stamps_for_window(self, window_start, &requested)
    }

    pub fn stamp_execution_quote_canary_discovery_rank(
        &self,
        event_id: &str,
        stamp: &ExecutionQuoteCanaryDiscoveryRankStamp,
    ) -> Result<bool> {
        ensure_execution_quote_canary_tables(self)?;
        let rank = i64::try_from(stamp.rank)
            .with_context(|| format!("discovery rank exceeds i64: {}", stamp.rank))?;
        let updated = self
            .execute_with_retry(|conn| {
                conn.execute(
                    "UPDATE execution_quote_canary_events
                     SET discovery_rank = ?2,
                         discovery_rank_cohort = ?3,
                         discovery_rank_window_start = ?4
                     WHERE event_id = ?1",
                    params![
                        event_id,
                        rank,
                        &stamp.cohort,
                        stamp.window_start.to_rfc3339()
                    ],
                )
            })
            .with_context(|| {
                format!("failed stamping execution quote canary discovery rank for {event_id}")
            })?;
        Ok(updated > 0)
    }
}

fn latest_rank_window_at_or_before(
    store: &SqliteDiscoveryStore,
    as_of: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>> {
    let mut stmt = store
        .conn
        .prepare(
            "SELECT DISTINCT window_start
             FROM wallet_metrics INDEXED BY idx_wallet_metrics_window_start
             ORDER BY window_start DESC
             LIMIT ?1",
        )
        .context("failed preparing wallet_metrics rank window lookup")?;
    let rows = stmt
        .query_map(params![RANK_WINDOW_SCAN_LIMIT], |row| {
            row.get::<_, String>(0)
        })
        .context("failed querying wallet_metrics rank windows")?;
    let mut latest = None;
    for raw in rows {
        let raw = raw.context("failed reading wallet_metrics.window_start")?;
        let window_start = parse_rfc3339_utc(&raw, "wallet_metrics.window_start")?;
        if window_start <= as_of && latest.map_or(true, |current| window_start > current) {
            latest = Some(window_start);
        }
    }
    Ok(latest)
}

fn load_rank_stamps_for_window(
    store: &SqliteDiscoveryStore,
    window_start: DateTime<Utc>,
    requested: &HashSet<&str>,
) -> Result<HashMap<String, ExecutionQuoteCanaryDiscoveryRankStamp>> {
    let canonical = window_start.to_rfc3339();
    let legacy_z = window_start.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let mut stmt = store
        .conn
        .prepare(
            "SELECT wallet_id
             FROM wallet_metrics INDEXED BY idx_wallet_metrics_window_start
             WHERE window_start IN (?1, ?2)
             ORDER BY score DESC, wallet_id ASC",
        )
        .context("failed preparing wallet_metrics rank stamp query")?;
    let rows = stmt
        .query_map(params![canonical, legacy_z], |row| row.get::<_, String>(0))
        .context("failed querying wallet_metrics rank stamps")?;
    let mut ranks = HashMap::new();
    for (index, wallet) in rows.enumerate() {
        let wallet = wallet.context("failed reading wallet_metrics.wallet_id")?;
        if requested.contains(wallet.as_str()) {
            let rank = index as u64 + 1;
            ranks
                .entry(wallet)
                .or_insert_with(|| ExecutionQuoteCanaryDiscoveryRankStamp {
                    rank,
                    cohort: discovery_rank_cohort(rank).to_string(),
                    window_start,
                });
            if ranks.len() == requested.len() {
                break;
            }
        }
    }
    Ok(ranks)
}

fn discovery_rank_cohort(rank: u64) -> &'static str {
    match rank {
        1..=15 => "rank_1_15",
        16..=30 => "rank_16_30",
        _ => "rank_gt_30",
    }
}
