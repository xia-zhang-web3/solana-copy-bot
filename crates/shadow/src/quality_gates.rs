use super::types::{
    ShadowDropReason, ShadowService, EPS, QUALITY_CACHE_TTL_SECONDS, QUALITY_MAX_SIGNATURE_PAGES,
    QUALITY_RPC_TIMEOUT_MS,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{TokenQualityCacheRow, TokenQualityRpcRow};
use copybot_storage_core::SqliteStore;
use reqwest::blocking::Client;
use serde_json::{json, Value};
use tracing::{info, warn};

const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

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
        fetch_token_quality_from_helius(
            helius_http_url,
            token,
            timeout_ms,
            max_signature_pages,
            min_age_hint_seconds,
        )
    }
}

fn fetch_token_quality_from_helius(
    helius_http_url: &str,
    mint: &str,
    timeout_ms: u64,
    max_signature_pages: u32,
    min_age_hint_seconds: Option<u64>,
) -> Result<TokenQualityRpcRow> {
    let client = Client::builder()
        .timeout(std::time::Duration::from_millis(timeout_ms.max(100)))
        .build()
        .context("failed building reqwest blocking client for token quality fetch")?;
    let holders = fetch_token_holders(&client, helius_http_url, mint).ok();
    let token_age_seconds = fetch_token_age_seconds(
        &client,
        helius_http_url,
        mint,
        max_signature_pages.max(1),
        min_age_hint_seconds,
    )
    .ok()
    .flatten();
    if holders.is_none() && token_age_seconds.is_none() {
        return Err(anyhow!(
            "failed to fetch token quality fields for mint {mint} via helius"
        ));
    }
    Ok(TokenQualityRpcRow {
        holders,
        liquidity_sol: None,
        token_age_seconds,
    })
}

fn fetch_token_holders(client: &Client, helius_http_url: &str, mint: &str) -> Result<u64> {
    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getProgramAccounts",
        "params": [
            TOKEN_PROGRAM_ID,
            {
                "encoding": "jsonParsed",
                "filters": [
                    { "dataSize": 165 },
                    { "memcmp": { "offset": 0, "bytes": mint } }
                ]
            },
        ],
    });
    let response = post_helius_json(client, helius_http_url, &payload)?;
    parse_token_holders_from_program_accounts_response(&response)
}

fn parse_token_holders_from_program_accounts_response(response: &Value) -> Result<u64> {
    let rpc_result = rpc_result(response);
    let accounts = rpc_result
        .as_array()
        .or_else(|| rpc_result.get("value").and_then(Value::as_array))
        .ok_or_else(|| anyhow!("missing token accounts array in rpc response"))?;
    let mut unique_owners = std::collections::HashSet::new();
    for (index, account) in accounts.iter().enumerate() {
        let info = account
            .get("account")
            .and_then(|value| value.get("data"))
            .and_then(|value| value.get("parsed"))
            .and_then(|value| value.get("info"))
            .ok_or_else(|| anyhow!("missing parsed token account info at index={index}"))?;
        let owner = info
            .get("owner")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing token account owner at index={index}"))?;
        let amount_raw = info
            .get("tokenAmount")
            .and_then(|value| value.get("amount"))
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing token amount at index={index}"))?;
        let amount = amount_raw
            .parse::<u64>()
            .with_context(|| format!("invalid token amount at index={index}: {amount_raw}"))?;
        if amount > 0 {
            unique_owners.insert(owner.to_string());
        }
    }
    Ok(unique_owners.len() as u64)
}

fn fetch_token_age_seconds(
    client: &Client,
    helius_http_url: &str,
    mint: &str,
    max_pages: u32,
    min_age_hint_seconds: Option<u64>,
) -> Result<Option<u64>> {
    let now_ts = Utc::now().timestamp();
    let min_block_time = min_age_hint_seconds
        .and_then(|hint| now_ts.checked_sub(hint as i64))
        .unwrap_or(i64::MIN);
    let mut oldest_seen: Option<i64> = None;
    let mut before_sig: Option<String> = None;
    for _page in 0..max_pages {
        let mut options = json!({ "limit": 1000 });
        if let Some(before) = before_sig.as_deref() {
            options["before"] = Value::String(before.to_string());
        }
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [mint, options],
        });
        let response = post_helius_json(client, helius_http_url, &payload)?;
        let entries = rpc_result(&response)
            .as_array()
            .ok_or_else(|| anyhow!("missing signatures array in helius response"))?;
        if entries.is_empty() {
            break;
        }
        for entry in entries {
            if let Some(value) = entry.get("blockTime").and_then(Value::as_i64) {
                oldest_seen = Some(oldest_seen.map_or(value, |current| current.min(value)));
            }
            if let Some(signature) = entry.get("signature").and_then(Value::as_str) {
                before_sig = Some(signature.to_string());
            }
        }
        if oldest_seen.is_some_and(|value| value <= min_block_time) {
            break;
        }
    }
    let Some(oldest) = oldest_seen else {
        return Ok(None);
    };
    if oldest > now_ts {
        return Ok(None);
    }
    Ok(Some((now_ts - oldest) as u64))
}

fn post_helius_json(client: &Client, helius_http_url: &str, payload: &Value) -> Result<Value> {
    let response = client
        .post(helius_http_url)
        .json(payload)
        .send()
        .with_context(|| format!("failed helius rpc request to {helius_http_url}"))?;
    let status = response.status();
    let body = response
        .json::<Value>()
        .context("failed parsing helius rpc response json")?;
    if !status.is_success() {
        return Err(anyhow!("helius rpc returned http status {status}: {body}"));
    }
    Ok(body)
}

fn rpc_result(payload: &Value) -> &Value {
    payload.get("result").unwrap_or(payload)
}
