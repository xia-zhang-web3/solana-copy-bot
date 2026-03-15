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
use std::collections::HashMap;
use std::time::Instant;
use tracing::{info, warn};

#[derive(Debug, Clone)]
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

impl DiscoveryService {
    pub(super) fn resolve_token_quality_for_mints(
        &self,
        store: &SqliteStore,
        mints: &[String],
        now: DateTime<Utc>,
    ) -> Result<HashMap<String, TokenQualityResolution>> {
        if mints.is_empty() {
            return Ok(HashMap::new());
        }

        let mut out = HashMap::new();
        let ttl = Duration::seconds(QUALITY_CACHE_TTL_SECONDS);
        let mut to_fetch = Vec::new();
        let mut fresh_hits = 0usize;
        let mut stale_hits = 0usize;
        let mut misses = 0usize;

        for mint in mints {
            match store.get_token_quality_cache(mint) {
                Ok(Some(row)) => {
                    if now - row.fetched_at <= ttl {
                        fresh_hits += 1;
                        out.insert(mint.clone(), TokenQualityResolution::Fresh(row));
                    } else {
                        stale_hits += 1;
                        to_fetch.push((mint.clone(), Some(row)));
                    }
                }
                Ok(None) => {
                    misses += 1;
                    to_fetch.push((mint.clone(), None))
                }
                Err(error) => {
                    if discovery_quality_cache_error_requires_abort(&error) {
                        return Err(error).context(
                            "discovery token quality cache lookup failed with fatal sqlite I/O",
                        );
                    }
                    warn!(error = %error, mint = %mint, "failed reading token quality cache");
                }
            }
        }

        let Some(helius_http_url) = self.helius_http_url.as_deref() else {
            for (mint, stale_row) in to_fetch {
                if let Some(row) = stale_row {
                    out.insert(mint, TokenQualityResolution::Stale(row));
                } else {
                    out.insert(mint, TokenQualityResolution::Missing);
                }
            }
            info!(
                quality_source = "cache+db_proxy",
                rpc_enabled = false,
                mints_total = mints.len(),
                cache_fresh = fresh_hits,
                cache_stale = stale_hits,
                cache_miss = misses,
                fetched_ok = 0usize,
                fetched_fail = 0usize,
                fallback_from_stale = stale_hits,
                budget_exhausted = 0usize,
                rpc_attempted = 0usize,
                rpc_budget_ms = QUALITY_RPC_BUDGET_MS,
                rpc_spent_ms = 0u64,
                "discovery token quality cache summary"
            );
            return Ok(out);
        };

        let mut fetched_ok = 0usize;
        let mut fetched_fail = 0usize;
        let mut fallback_from_stale = 0usize;
        let mut budget_exhausted = 0usize;
        let mut deferred_missing = 0usize;
        let mut missing_hard = 0usize;
        let mut rpc_attempted = 0usize;
        let refresh_started = Instant::now();
        for (mint, stale_row) in to_fetch {
            let mut stale_fallback = stale_row;
            if rpc_attempted >= QUALITY_MAX_FETCH_PER_CYCLE {
                if let Some(stale_row) = stale_fallback.take() {
                    fallback_from_stale += 1;
                    out.insert(mint, TokenQualityResolution::Stale(stale_row));
                } else {
                    deferred_missing += 1;
                    out.insert(mint, TokenQualityResolution::Deferred);
                }
                continue;
            }
            if refresh_started.elapsed().as_millis() as u64 >= QUALITY_RPC_BUDGET_MS {
                budget_exhausted += 1;
                if let Some(stale_row) = stale_fallback.take() {
                    fallback_from_stale += 1;
                    out.insert(mint, TokenQualityResolution::Stale(stale_row));
                } else {
                    deferred_missing += 1;
                    out.insert(mint, TokenQualityResolution::Deferred);
                }
                continue;
            }

            rpc_attempted += 1;
            match fetch_token_quality_from_helius_guarded(
                helius_http_url,
                &mint,
                QUALITY_RPC_TIMEOUT_MS,
                QUALITY_MAX_SIGNATURE_PAGES,
                Some(self.shadow_quality.min_token_age_seconds),
            ) {
                Ok(fetched) => {
                    if let Err(error) = store.upsert_token_quality_cache(
                        &mint,
                        fetched.holders,
                        fetched.liquidity_sol,
                        fetched.token_age_seconds,
                        now,
                    ) {
                        if discovery_quality_cache_error_requires_abort(&error) {
                            return Err(error).context(
                                "discovery token quality cache refresh write failed with fatal sqlite I/O",
                            );
                        }
                        warn!(error = %error, mint = %mint, "failed updating token quality cache");
                    }
                    match store.get_token_quality_cache(&mint) {
                        Ok(Some(row)) => {
                            fetched_ok += 1;
                            out.insert(mint, TokenQualityResolution::Fresh(row));
                        }
                        Ok(None) => {
                            if let Some(stale_row) = stale_fallback.take() {
                                fallback_from_stale += 1;
                                out.insert(mint, TokenQualityResolution::Stale(stale_row));
                            } else {
                                missing_hard += 1;
                                out.insert(mint.clone(), TokenQualityResolution::Missing);
                                warn!(
                                    mint = %mint,
                                    "token quality cache row missing after refresh"
                                );
                            }
                        }
                        Err(error) => {
                            if discovery_quality_cache_error_requires_abort(&error) {
                                return Err(error).context(
                                    "discovery token quality cache refresh readback failed with fatal sqlite I/O",
                                );
                            }
                            warn!(
                                error = %error,
                                mint = %mint,
                                "failed reading token quality cache after refresh"
                            );
                            if let Some(stale_row) = stale_fallback.take() {
                                fallback_from_stale += 1;
                                out.insert(mint, TokenQualityResolution::Stale(stale_row));
                            } else {
                                missing_hard += 1;
                                out.insert(mint, TokenQualityResolution::Missing);
                            }
                        }
                    }
                }
                Err(error) => {
                    fetched_fail += 1;
                    warn!(
                        error = %error,
                        mint = %mint,
                        "failed to refresh token quality via helius, using fallback"
                    );
                    if let Some(stale_row) = stale_fallback.take() {
                        fallback_from_stale += 1;
                        out.insert(mint, TokenQualityResolution::Stale(stale_row));
                    } else {
                        missing_hard += 1;
                        out.insert(mint, TokenQualityResolution::Missing);
                    }
                }
            }
        }

        let rpc_spent_ms = refresh_started.elapsed().as_millis() as u64;
        info!(
            quality_source = "cache+rpc+db_proxy",
            rpc_enabled = true,
            mints_total = mints.len(),
            cache_fresh = fresh_hits,
            cache_stale = stale_hits,
            cache_miss = misses,
            fetched_ok,
            fetched_fail,
            fallback_from_stale,
            budget_exhausted,
            deferred_missing,
            missing_hard,
            rpc_attempted,
            rpc_budget_ms = QUALITY_RPC_BUDGET_MS,
            rpc_spent_ms,
            "discovery token quality cache summary"
        );

        Ok(out)
    }

    pub(super) fn update_token_quality_state(
        &self,
        token_states: &mut HashMap<String, TokenRollingState>,
        token_sol_history: &mut HashMap<String, Vec<SolLegTrade>>,
        token_quality_cache: &HashMap<String, TokenQualityResolution>,
        swap: &SwapEvent,
    ) -> Option<BuyTradability> {
        self.touch_token_state(token_states, &swap.token_in, &swap.wallet, swap.ts_utc);
        self.touch_token_state(token_states, &swap.token_out, &swap.wallet, swap.ts_utc);

        let (token, sol_notional) = if is_sol_buy(swap) {
            (swap.token_out.as_str(), swap.amount_in)
        } else if is_sol_sell(swap) {
            (swap.token_in.as_str(), swap.amount_out)
        } else {
            return None;
        };

        self.push_sol_leg_trade(
            token_states,
            token_sol_history,
            token,
            swap.wallet.as_str(),
            swap.ts_utc,
            sol_notional.max(0.0),
        );

        if !is_sol_buy(swap) {
            return None;
        }

        let state = token_states
            .get_mut(token)
            .expect("token state is initialized when push_sol_leg_trade is called");
        Self::evict_expired_5m(state, swap.ts_utc);
        Some(self.evaluate_buy_tradability(state, token_quality_cache.get(token), swap.ts_utc))
    }

    fn touch_token_state(
        &self,
        token_states: &mut HashMap<String, TokenRollingState>,
        token: &str,
        wallet_id: &str,
        ts: DateTime<Utc>,
    ) {
        let state = token_states.entry(token.to_string()).or_default();
        state.first_seen = Some(
            state
                .first_seen
                .map(|current| current.min(ts))
                .unwrap_or(ts),
        );
        state.wallets_seen.insert(wallet_id.to_string());
    }

    fn push_sol_leg_trade(
        &self,
        token_states: &mut HashMap<String, TokenRollingState>,
        token_sol_history: &mut HashMap<String, Vec<SolLegTrade>>,
        token: &str,
        wallet_id: &str,
        ts: DateTime<Utc>,
        sol_notional: f64,
    ) {
        let trade = SolLegTrade {
            ts,
            wallet_id: wallet_id.to_string(),
            sol_notional,
        };
        token_sol_history
            .entry(token.to_string())
            .or_default()
            .push(trade.clone());

        let state = token_states.entry(token.to_string()).or_default();
        Self::evict_expired_5m(state, ts);
        state.sol_volume_5m += trade.sol_notional;
        state
            .sol_traders_5m
            .entry(trade.wallet_id.clone())
            .and_modify(|count| *count += 1)
            .or_insert(1);
        state.sol_trades_5m.push_back(trade);
    }

    fn evict_expired_5m(state: &mut TokenRollingState, now: DateTime<Utc>) {
        let cutoff = now - Duration::minutes(5);
        while let Some(front) = state.sol_trades_5m.front() {
            if front.ts >= cutoff {
                break;
            }
            let expired = state
                .sol_trades_5m
                .pop_front()
                .expect("checked front exists above");
            state.sol_volume_5m = (state.sol_volume_5m - expired.sol_notional).max(0.0);
            if let Some(count) = state.sol_traders_5m.get_mut(&expired.wallet_id) {
                *count -= 1;
                if *count == 0 {
                    state.sol_traders_5m.remove(&expired.wallet_id);
                }
            }
        }
    }

    fn evaluate_buy_tradability(
        &self,
        state: &TokenRollingState,
        quality: Option<&TokenQualityResolution>,
        signal_ts: DateTime<Utc>,
    ) -> BuyTradability {
        let liquidity_proxy = state
            .sol_trades_5m
            .iter()
            .map(|trade| trade.sol_notional)
            .fold(0.0, f64::max);
        if self.shadow_quality.min_volume_5m_sol > 0.0
            && state.sol_volume_5m + 1e-12 < self.shadow_quality.min_volume_5m_sol
        {
            return BuyTradability::Rejected;
        }
        if self.shadow_quality.min_unique_traders_5m > 0
            && state.sol_traders_5m.len() < self.shadow_quality.min_unique_traders_5m as usize
        {
            return BuyTradability::Rejected;
        }

        let mut deferred = false;
        if self.shadow_quality.min_token_age_seconds > 0 {
            let token_age_seconds = match quality {
                Some(TokenQualityResolution::Fresh(row)) => row.token_age_seconds,
                Some(TokenQualityResolution::Stale(row)) => row.token_age_seconds.map(|age| {
                    age.saturating_add(
                        signal_ts
                            .signed_duration_since(row.fetched_at)
                            .num_seconds()
                            .max(0) as u64,
                    )
                }),
                Some(TokenQualityResolution::Deferred) => {
                    deferred = true;
                    None
                }
                Some(TokenQualityResolution::Missing) | None => None,
            };
            let Some(token_age_seconds) = token_age_seconds else {
                return if deferred {
                    BuyTradability::Deferred
                } else {
                    BuyTradability::Rejected
                };
            };
            if token_age_seconds < self.shadow_quality.min_token_age_seconds {
                return BuyTradability::Rejected;
            }
        }
        if self.shadow_quality.min_holders > 0 {
            match quality {
                Some(TokenQualityResolution::Fresh(row)) => {
                    let Some(holders) = row.holders else {
                        return BuyTradability::Rejected;
                    };
                    if holders < self.shadow_quality.min_holders {
                        return BuyTradability::Rejected;
                    }
                }
                Some(TokenQualityResolution::Stale(row)) => {
                    if let Some(holders) = row.holders {
                        if holders < self.shadow_quality.min_holders {
                            return BuyTradability::Rejected;
                        }
                    }
                    deferred = true;
                }
                Some(TokenQualityResolution::Deferred) => deferred = true,
                Some(TokenQualityResolution::Missing) | None => {
                    return BuyTradability::Rejected;
                }
            }
        }
        if self.shadow_quality.min_liquidity_sol > 0.0 {
            let liquidity_sol = match quality {
                Some(TokenQualityResolution::Fresh(row)) => {
                    row.liquidity_sol.unwrap_or(liquidity_proxy)
                }
                _ => liquidity_proxy,
            };
            if liquidity_sol + 1e-12 < self.shadow_quality.min_liquidity_sol {
                return BuyTradability::Rejected;
            }
        }
        if deferred {
            BuyTradability::Deferred
        } else {
            BuyTradability::Tradable
        }
    }
}

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

    #[test]
    fn is_tradable_token_rejects_missing_rpc_age_and_holders_when_thresholded() {
        let mut shadow_quality = ShadowConfig::default();
        shadow_quality.min_token_age_seconds = 600;
        shadow_quality.min_holders = 10;
        shadow_quality.min_volume_5m_sol = 0.5;
        shadow_quality.min_unique_traders_5m = 1;
        let discovery = make_service(shadow_quality);
        let state = make_state(1.0, 1);

        assert!(
            matches!(
                discovery.evaluate_buy_tradability(&state, None, Utc::now()),
                BuyTradability::Rejected
            ),
            "missing rpc/cache quality must fail closed when age/holders thresholds are enabled"
        );
    }

    #[test]
    fn is_tradable_token_rejects_missing_holders_field_when_thresholded() {
        let mut shadow_quality = ShadowConfig::default();
        shadow_quality.min_token_age_seconds = 0;
        shadow_quality.min_holders = 10;
        shadow_quality.min_volume_5m_sol = 0.5;
        shadow_quality.min_unique_traders_5m = 1;
        let discovery = make_service(shadow_quality);
        let state = make_state(1.0, 1);
        let mut row = make_cache_row();
        row.holders = None;

        assert!(
            matches!(
                discovery.evaluate_buy_tradability(
                    &state,
                    Some(&TokenQualityResolution::Fresh(row)),
                    Utc::now()
                ),
                BuyTradability::Rejected
            ),
            "missing holders field must fail closed when min_holders is enabled"
        );
    }

    #[test]
    fn is_tradable_token_uses_liquidity_proxy_when_rpc_quality_missing() {
        let mut shadow_quality = ShadowConfig::default();
        shadow_quality.min_token_age_seconds = 0;
        shadow_quality.min_holders = 0;
        shadow_quality.min_liquidity_sol = 0.75;
        shadow_quality.min_volume_5m_sol = 0.5;
        shadow_quality.min_unique_traders_5m = 1;
        let discovery = make_service(shadow_quality);
        let state = make_state(1.0, 1);

        assert!(
            matches!(
                discovery.evaluate_buy_tradability(&state, None, Utc::now()),
                BuyTradability::Tradable
            ),
            "liquidity gate should still use rolling proxy when rpc quality is unavailable"
        );
    }

    #[test]
    fn is_tradable_token_rejects_when_liquidity_proxy_is_below_threshold() {
        let mut shadow_quality = ShadowConfig::default();
        shadow_quality.min_token_age_seconds = 0;
        shadow_quality.min_holders = 0;
        shadow_quality.min_liquidity_sol = 1.5;
        shadow_quality.min_volume_5m_sol = 0.5;
        shadow_quality.min_unique_traders_5m = 1;
        let discovery = make_service(shadow_quality);
        let state = make_state(1.0, 1);

        assert!(
            matches!(
                discovery.evaluate_buy_tradability(&state, None, Utc::now()),
                BuyTradability::Rejected
            ),
            "missing rpc quality must not bypass liquidity threshold"
        );
    }

    #[test]
    fn evaluate_buy_tradability_defers_missing_age_holders_when_fetch_was_deferred() {
        let mut shadow_quality = ShadowConfig::default();
        shadow_quality.min_token_age_seconds = 600;
        shadow_quality.min_holders = 10;
        shadow_quality.min_liquidity_sol = 0.5;
        shadow_quality.min_volume_5m_sol = 0.5;
        shadow_quality.min_unique_traders_5m = 1;
        let discovery = make_service(shadow_quality);
        let state = make_state(1.0, 1);

        assert!(
            matches!(
                discovery.evaluate_buy_tradability(
                    &state,
                    Some(&TokenQualityResolution::Deferred),
                    Utc::now()
                ),
                BuyTradability::Deferred
            ),
            "budget-deferred quality should not hard-reject tradability"
        );
    }

    #[test]
    fn evaluate_buy_tradability_does_not_trust_stale_holders_for_pass() {
        let mut shadow_quality = ShadowConfig::default();
        shadow_quality.min_token_age_seconds = 0;
        shadow_quality.min_holders = 10;
        shadow_quality.min_liquidity_sol = 0.5;
        shadow_quality.min_volume_5m_sol = 0.5;
        shadow_quality.min_unique_traders_5m = 1;
        let discovery = make_service(shadow_quality);
        let state = make_state(1.0, 1);
        let row = make_cache_row();

        assert!(
            matches!(
                discovery.evaluate_buy_tradability(
                    &state,
                    Some(&TokenQualityResolution::Stale(row)),
                    Utc::now()
                ),
                BuyTradability::Deferred
            ),
            "stale holders above threshold must not be treated as authoritative pass"
        );
    }

    #[test]
    fn evaluate_buy_tradability_uses_liquidity_proxy_for_stale_rows() {
        let mut shadow_quality = ShadowConfig::default();
        shadow_quality.min_token_age_seconds = 0;
        shadow_quality.min_holders = 0;
        shadow_quality.min_liquidity_sol = 1.5;
        shadow_quality.min_volume_5m_sol = 0.5;
        shadow_quality.min_unique_traders_5m = 1;
        let discovery = make_service(shadow_quality);
        let state = make_state(1.0, 1);
        let mut row = make_cache_row();
        row.liquidity_sol = Some(10.0);

        assert!(
            matches!(
                discovery.evaluate_buy_tradability(
                    &state,
                    Some(&TokenQualityResolution::Stale(row)),
                    Utc::now()
                ),
                BuyTradability::Rejected
            ),
            "stale liquidity rows must not keep a token passing if current proxy is below threshold"
        );
    }

    #[test]
    fn discovery_quality_cache_error_requires_abort_on_xshmmap_io_failure() {
        let error = anyhow!("disk I/O error: Error code 4874: I/O error within the xShmMap method");
        assert!(discovery_quality_cache_error_requires_abort(&error));
    }

    #[test]
    fn discovery_quality_cache_error_does_not_require_abort_on_busy_lock() {
        let error = anyhow!("database is locked");
        assert!(!discovery_quality_cache_error_requires_abort(&error));
    }

    #[test]
    fn resolve_token_quality_for_mints_returns_error_on_fatal_cache_write_failure() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("quality-cache-fatal.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for token quality trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_token_quality_cache_insert
             BEFORE INSERT ON token_quality_cache
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let (helius_http_url, server_handle) = spawn_fake_helius_server()?;
        let discovery = make_service_with_helius(ShadowConfig::default(), helius_http_url);
        let now = DateTime::parse_from_rfc3339("2026-03-14T14:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let mints = vec!["TokenQualityFatal11111111111111111111111".to_string()];

        let error = discovery
            .resolve_token_quality_for_mints(&store, &mints, now)
            .expect_err("fatal token quality cache write must propagate");
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains(
                "discovery token quality cache refresh write failed with fatal sqlite I/O"
            ),
            "expected fatal token quality cache write context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed upserting token_quality_cache row"),
            "expected token quality cache upsert context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert!(
            store.get_token_quality_cache(&mints[0])?.is_none(),
            "fatal token quality cache write failure must not leave a persisted cache row"
        );

        server_handle
            .join()
            .map_err(|_| anyhow!("fake helius server thread panicked"))??;
        drop(trigger_conn);
        Ok(())
    }
}
