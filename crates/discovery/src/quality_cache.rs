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

include!("quality_cache/10_resolve.rs");
include!("quality_cache/20_state.rs");
include!("quality_cache/30_evaluate.rs");

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
mod tests {
    use super::*;
    use anyhow::{anyhow, Context, Result};
    use copybot_config::{DiscoveryConfig, ShadowConfig};
    use copybot_storage::SqliteStore;
    use rusqlite::Connection;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::path::Path;
    use std::thread;
    use tempfile::tempdir;

    fn make_service(shadow_quality: ShadowConfig) -> DiscoveryService {
        DiscoveryService::new(DiscoveryConfig::default(), shadow_quality)
    }

    fn make_service_with_helius(
        shadow_quality: ShadowConfig,
        helius_http_url: String,
    ) -> DiscoveryService {
        DiscoveryService::new_with_helius(
            DiscoveryConfig::default(),
            shadow_quality,
            Some(helius_http_url),
        )
    }

    fn spawn_fake_helius_server() -> Result<(String, thread::JoinHandle<Result<()>>)> {
        let listener =
            TcpListener::bind("127.0.0.1:0").context("failed to bind fake helius test server")?;
        let addr = listener
            .local_addr()
            .context("failed to read fake helius addr")?;
        let handle = thread::spawn(move || -> Result<()> {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().context("failed accepting test request")?;
                let mut headers = Vec::new();
                let mut byte = [0u8; 1];
                while !headers.ends_with(b"\r\n\r\n") {
                    stream
                        .read_exact(&mut byte)
                        .context("failed reading test request headers")?;
                    headers.push(byte[0]);
                }
                let headers_raw =
                    String::from_utf8(headers).context("request headers must be utf-8")?;
                let content_length = headers_raw
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .unwrap_or(0);
                let mut body = vec![0u8; content_length];
                if content_length > 0 {
                    stream
                        .read_exact(&mut body)
                        .context("failed reading test request body")?;
                }
                let body = String::from_utf8(body).context("request body must be utf-8")?;
                let response_body = if body.contains("\"method\":\"getProgramAccounts\"") {
                    "{\"jsonrpc\":\"2.0\",\"result\":[{\"account\":{\"data\":{\"parsed\":{\"info\":{\"owner\":\"OwnerA\",\"tokenAmount\":{\"amount\":\"10\"}}}}}}]}".to_string()
                } else if body.contains("\"method\":\"getSignaturesForAddress\"") {
                    format!(
                        "{{\"jsonrpc\":\"2.0\",\"result\":[{{\"signature\":\"sig-quality-test\",\"blockTime\":{}}}]}}",
                        Utc::now().timestamp() - 3600
                    )
                } else {
                    "{\"jsonrpc\":\"2.0\",\"result\":[]}".to_string()
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    response_body.len(),
                    response_body
                );
                stream
                    .write_all(response.as_bytes())
                    .context("failed writing fake helius response")?;
                stream
                    .flush()
                    .context("failed flushing fake helius response")?;
            }
            Ok(())
        });
        Ok((format!("http://{addr}"), handle))
    }

    fn make_state(sol_notional: f64, unique_traders: usize) -> TokenRollingState {
        let mut state = TokenRollingState::default();
        let now = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        for idx in 0..unique_traders {
            let wallet = format!("wallet-{idx}");
            state.sol_traders_5m.insert(wallet.clone(), 1);
            state.sol_trades_5m.push_back(SolLegTrade {
                ts: now,
                wallet_id: wallet,
                sol_notional,
            });
            state.sol_volume_5m += sol_notional;
        }
        state
    }

    fn make_cache_row() -> TokenQualityCacheRow {
        TokenQualityCacheRow {
            mint: "TokenA".to_string(),
            holders: Some(42),
            liquidity_sol: Some(3.0),
            token_age_seconds: Some(3_600),
            fetched_at: DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        }
    }

    include!("quality_cache/tests.rs");
}
