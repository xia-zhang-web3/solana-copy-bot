//! Closed request-bound finalized collector. RPC is trusted, never local consensus.
#[path = "execution_owned_sell_rpc_decode.rs"]
mod decode;
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::{ExecutionConfig, RPC_FINALIZED_OWNED_SELL_V1};
use copybot_storage_core::rpc_owned_sell_snapshot::OwnedSellSnapshot;
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
#[derive(Debug, Serialize)]
pub(crate) struct Exchange {
    request: Value,
    request_sha256: String,
    response: Value,
    response_json: String,
    response_sha256: String,
    started: DateTime<Utc>,
    completed: DateTime<Utc>,
}
impl Exchange {
    pub(crate) fn value(&self) -> &Value {
        &self.response
    }
}
#[derive(Debug, Serialize)]
enum SourceOrder {
    RpcFinalizedCrossSlot {
        buy_slots: Vec<u64>,
        sell_slot: u64,
        source_utc: Option<DateTime<Utc>>,
        absolute_age_ms: Option<u64>,
    },
}
#[derive(Debug, Serialize)]
pub(crate) struct Authority {
    policy: &'static str,
    config_sha256: String,
    config_identity: String,
    endpoint_sha256: String,
    pinned_genesis: String,
    snapshot: OwnedSellSnapshot,
    order: SourceOrder,
    exchanges: Vec<Exchange>,
}
pub(crate) fn digest(bytes: impl AsRef<[u8]>) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub(crate) fn endpoint(c: &ExecutionConfig) -> Result<reqwest::Url> {
    let p = c
        .owned_sell_preparation
        .as_ref()
        .context("owned_sell_authority_off")?;
    ensure!(
        p.policy == RPC_FINALIZED_OWNED_SELL_V1,
        "owned_sell_policy_unsupported"
    );
    let u = reqwest::Url::parse(&p.rpc_url).map_err(|_| anyhow::anyhow!("owned_sell_rpc_url"))?;
    ensure!(
        u.scheme() == "https"
            || (u.scheme() == "http"
                && matches!(u.host_str(), Some("127.0.0.1" | "[::1]" | "localhost"))),
        "owned_sell_rpc_transport"
    );
    ensure!(u.fragment().is_none(), "owned_sell_rpc_fragment");
    crate::execution_pumpswap_accounts::parse_pubkey(&p.genesis_hash, "owned_sell_genesis")?;
    Ok(u)
}
pub(crate) fn identity(c: &ExecutionConfig) -> Result<String> {
    let p = c
        .owned_sell_preparation
        .as_ref()
        .context("owned_sell_authority_off")?;
    let base = digest(serde_json::to_vec(&(
        p.policy.as_str(),
        endpoint(c)?.as_str(),
        &p.genesis_hash,
        &p.identity,
        &c.canary_wallet_pubkey,
        &c.tiny_experiment.id,
        format!("{:?}", c.tiny_experiment.policy_mode),
        c.pretrade_max_priority_fee_lamports,
        c.pretrade_min_sol_reserve,
    ))?);
    if copybot_config::owned_sell_dispatch(c) {
        return Ok(digest(serde_json::to_vec(&(
            base,
            "tiny_dispatch_v1",
            c.enabled,
            c.canary_tiny_submit_enabled,
            c.canary_enabled,
            c.quote_canary_enabled,
            &c.execution_signer_pubkey,
            &c.execution_signer_keypair_path,
            &c.canary_kill_switch_path,
            &c.canary_route,
            &c.quote_canary_base_url,
            c.submit_timeout_ms,
            c.canary_max_open_positions,
            c.canary_max_daily_loss_sol,
        ))?));
    }
    Ok(base)
}
impl Authority {
    pub(crate) fn binding(&self, c: &ExecutionConfig, s: &OwnedSellSnapshot) -> Result<String> {
        ensure!(
            self.snapshot == *s && self.config_sha256 == identity(c)?,
            "owned_sell_authority_changed"
        );
        Ok(serde_json::to_string(self)?)
    }
}
pub(crate) async fn collect(
    http: &reqwest::Client,
    c: &ExecutionConfig,
    s: &OwnedSellSnapshot,
    check: &mut impl FnMut() -> Result<()>,
) -> Result<Authority> {
    let url = endpoint(c)?;
    let p = c
        .owned_sell_preparation
        .as_ref()
        .context("owned_sell_authority_off")?;
    let mut exchanges = vec![];
    let g = exchange(
        http,
        c,
        &url,
        json!({"jsonrpc":"2.0","id":"owned-sell-genesis","method":"getGenesisHash","params":[]}),
        check,
    )
    .await?;
    ensure!(
        g.response["result"].as_str() == Some(&p.genesis_hash),
        "owned_sell_genesis_mismatch"
    );
    exchanges.push(g);
    let mut buys = vec![];
    ensure!(s.receipts.len() == s.facts.len(), "owned_sell_receipt_set");
    for (r, f) in s.receipts.iter().zip(&s.facts) {
        let e = transaction(http, c, &url, &r.contributor.tx_signature, check).await?;
        decode::buy(&e.response["result"], r, f)?;
        buys.push(r.slot);
        exchanges.push(e);
    }
    let e = transaction(http, c, &url, &s.sell.facts.signature, check).await?;
    let observed_slot = e.response["result"]["slot"]
        .as_u64()
        .context("owned_sell_transaction_slot")?;
    ensure!(
        !buys.is_empty() && buys.iter().all(|b| observed_slot > *b),
        "owned_sell_cross_slot_order"
    );
    let sell_slot = decode::sell(&e.response["result"], &s.sell)?;
    ensure!(
        !buys.is_empty() && buys.iter().all(|b| sell_slot > *b),
        "owned_sell_cross_slot_order"
    );
    exchanges.push(e);
    check()?;
    Ok(Authority {
        policy: RPC_FINALIZED_OWNED_SELL_V1,
        config_sha256: identity(c)?,
        config_identity: p.identity.clone(),
        endpoint_sha256: digest(url.as_str()),
        pinned_genesis: p.genesis_hash.clone(),
        snapshot: s.clone(),
        order: SourceOrder::RpcFinalizedCrossSlot {
            buy_slots: buys,
            sell_slot,
            source_utc: None,
            absolute_age_ms: None,
        },
        exchanges,
    })
}
async fn transaction(
    http: &reqwest::Client,
    c: &ExecutionConfig,
    url: &reqwest::Url,
    sig: &str,
    check: &mut impl FnMut() -> Result<()>,
) -> Result<Exchange> {
    exchange(http,c,url,json!({"jsonrpc":"2.0","id":format!("owned-sell-tx:{sig}"),"method":"getTransaction","params":[sig,{"encoding":"jsonParsed","commitment":"finalized","maxSupportedTransactionVersion":0}]}),check).await
}
pub(crate) async fn exchange(
    http: &reqwest::Client,
    c: &ExecutionConfig,
    url: &reqwest::Url,
    request: Value,
    check: &mut impl FnMut() -> Result<()>,
) -> Result<Exchange> {
    check()?;
    let started = Utc::now();
    let mut response = http
        .post(url.clone())
        .json(&request)
        .timeout(std::time::Duration::from_millis(
            c.quote_canary_timeout_ms.clamp(1, 10000),
        ))
        .send()
        .await
        .map_err(|_| anyhow::anyhow!("owned_sell_rpc_transport"))?;
    check()?;
    ensure!(
        response.url() == url && response.status().is_success(),
        "owned_sell_rpc_endpoint_status"
    );
    ensure!(
        response.content_length().unwrap_or(0) <= 1 << 20,
        "owned_sell_rpc_response_bound"
    );
    let mut bytes = vec![];
    loop {
        let chunk = response
            .chunk()
            .await
            .map_err(|_| anyhow::anyhow!("owned_sell_rpc_body"))?;
        check()?;
        let Some(chunk) = chunk else { break };
        ensure!(
            bytes.len() + chunk.len() <= 1 << 20,
            "owned_sell_rpc_response_bound"
        );
        bytes.extend_from_slice(&chunk);
    }
    let value: Value =
        serde_json::from_slice(&bytes).map_err(|_| anyhow::anyhow!("owned_sell_rpc_json"))?;
    ensure!(
        value["jsonrpc"] == "2.0"
            && value.get("id") == request.get("id")
            && value.get("error").is_none(),
        "owned_sell_rpc_request_binding"
    );
    ensure!(
        value.get("result").is_some_and(|v| !v.is_null()),
        "owned_sell_rpc_result_missing"
    );
    Ok(Exchange {
        request_sha256: digest(serde_json::to_vec(&request)?),
        request,
        response: value,
        response_json: String::from_utf8(bytes.clone())?,
        response_sha256: digest(&bytes),
        started,
        completed: Utc::now(),
    })
}
