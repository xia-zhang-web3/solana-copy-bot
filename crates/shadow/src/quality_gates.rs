use super::{
    ShadowDropReason, ShadowService, EPS, QUALITY_CACHE_TTL_SECONDS, QUALITY_MAX_SIGNATURE_PAGES,
    QUALITY_RPC_TIMEOUT_MS,
};
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_storage::{SqliteStore, TokenQualityCacheRow, TokenQualityRpcRow};
use tracing::{info, warn};

impl ShadowService {
    pub(super) fn drop_reason_for_buy_quality_gate(
        &self,
        store: &SqliteStore,
        token: &str,
        signal_ts: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<Option<ShadowDropReason>> {
        if !self.config.quality_gates_enabled {
            return Ok(None);
        }

        let stats = store.token_market_stats(token, signal_ts)?;
        let rpc_quality = self.resolve_token_quality(store, token, now)?;
        let proxy_age_seconds = stats
            .first_seen
            .map(|first_seen| (signal_ts - first_seen).num_seconds().max(0))
            .unwrap_or(0) as u64;
        let token_age_seconds = rpc_quality
            .as_ref()
            .and_then(|row| row.token_age_seconds)
            .unwrap_or(proxy_age_seconds);
        let holders = rpc_quality
            .as_ref()
            .and_then(|row| row.holders)
            .unwrap_or(stats.holders_proxy);
        let liquidity_sol = rpc_quality
            .as_ref()
            .and_then(|row| row.liquidity_sol)
            .unwrap_or(stats.liquidity_sol_proxy);
        let quality_source = if let Some(row) = rpc_quality.as_ref() {
            if now - row.fetched_at <= Duration::seconds(QUALITY_CACHE_TTL_SECONDS) {
                "rpc_cache"
            } else {
                "rpc_cache_stale"
            }
        } else {
            "db_proxy"
        };
        info!(
            token = %token,
            quality_source,
            token_age_seconds,
            holders,
            liquidity_sol,
            "shadow quality metrics evaluated"
        );

        if self.config.min_token_age_seconds > 0
            && token_age_seconds < self.config.min_token_age_seconds
        {
            return Ok(Some(ShadowDropReason::TooNew));
        }

        if self.config.min_holders > 0 && holders < self.config.min_holders {
            return Ok(Some(ShadowDropReason::LowHolders));
        }

        if self.config.min_liquidity_sol > 0.0
            && liquidity_sol + EPS < self.config.min_liquidity_sol
        {
            return Ok(Some(ShadowDropReason::LowLiquidity));
        }

        if self.config.min_volume_5m_sol > 0.0
            && stats.volume_5m_sol + EPS < self.config.min_volume_5m_sol
        {
            return Ok(Some(ShadowDropReason::LowVolume));
        }

        if self.config.min_unique_traders_5m > 0
            && stats.unique_traders_5m < self.config.min_unique_traders_5m
        {
            return Ok(Some(ShadowDropReason::ThinMarket));
        }

        Ok(None)
    }

    fn resolve_token_quality(
        &self,
        store: &SqliteStore,
        token: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<TokenQualityCacheRow>> {
        let cached = store.get_token_quality_cache(token)?;
        let is_fresh = cached
            .as_ref()
            .map(|row| now - row.fetched_at <= Duration::seconds(QUALITY_CACHE_TTL_SECONDS))
            .unwrap_or(false);
        if is_fresh {
            return Ok(cached);
        }

        let Some(helius_http_url) = self.helius_http_url.as_deref() else {
            return Ok(cached);
        };

        match Self::fetch_token_quality_from_helius_guarded(
            helius_http_url,
            token,
            QUALITY_RPC_TIMEOUT_MS,
            QUALITY_MAX_SIGNATURE_PAGES,
            Some(self.config.min_token_age_seconds),
        ) {
            Ok(fetched) => {
                store.upsert_token_quality_cache(
                    token,
                    fetched.holders,
                    fetched.liquidity_sol,
                    fetched.token_age_seconds,
                    now,
                )?;
                store.get_token_quality_cache(token)
            }
            Err(error) => {
                warn!(
                    error = %error,
                    token = %token,
                    "failed to refresh token quality via helius, falling back"
                );
                Ok(cached)
            }
        }
    }

    fn fetch_token_quality_from_helius_guarded(
        helius_http_url: &str,
        token: &str,
        timeout_ms: u64,
        max_signature_pages: u32,
        min_age_hint_seconds: Option<u64>,
    ) -> Result<TokenQualityRpcRow> {
        SqliteStore::fetch_token_quality_from_helius(
            helius_http_url,
            token,
            timeout_ms,
            max_signature_pages,
            min_age_hint_seconds,
        )
    }
}
