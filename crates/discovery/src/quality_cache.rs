use super::{
    is_sol_buy, is_sol_sell, DiscoveryService, SolLegTrade, TokenRollingState,
    QUALITY_CACHE_TTL_SECONDS, QUALITY_MAX_FETCH_PER_CYCLE, QUALITY_MAX_SIGNATURE_PAGES,
    QUALITY_RPC_BUDGET_MS, QUALITY_RPC_TIMEOUT_MS,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::SwapEvent;
use copybot_storage::{
    is_fatal_sqlite_anyhow_error, SqliteStore, TokenQualityCacheRow, TokenQualityRpcRow,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration as StdDuration, Instant};
use tracing::{info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) enum TokenQualityResolution {
    Fresh(TokenQualityCacheRow),
    Stale(TokenQualityCacheRow),
    Deferred,
    Missing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BuyTradability {
    Tradable,
    Rejected,
    Deferred,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(super) struct TokenQualityResolutionProgress {
    pub next_mint_index: usize,
    pub rpc_attempted: usize,
    pub rpc_spent_ms: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct TokenQualityResolutionChunkOutcome {
    pub processed_mints: usize,
    pub source_exhausted: bool,
}

#[path = "quality_cache/10_resolve.rs"]
mod resolve;
#[path = "quality_cache/20_state.rs"]
mod state;
#[path = "quality_cache/30_evaluate.rs"]
mod evaluate;

fn discovery_quality_cache_error_requires_abort(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

fn fetch_token_quality_from_helius_guarded(
    helius_http_url: &str,
    mint: &str,
    timeout_ms: u64,
    max_signature_pages: u32,
    min_age_hint_seconds: Option<u64>,
) -> Result<TokenQualityRpcRow> {
    SqliteStore::fetch_token_quality_from_helius(
        helius_http_url,
        mint,
        timeout_ms,
        max_signature_pages,
        min_age_hint_seconds,
    )
}

#[cfg(test)]
#[path = "quality_cache/tests.rs"]
mod tests;
