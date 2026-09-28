//! Confirmed history from the bounded local budget broker. HTTP observations
//! retain their response bytes; conversion does not claim a protobuf wire match.
mod block;
mod error;
pub(crate) mod identity;
mod meta;
mod transaction;
mod value;

/// Normalize a confirmed full JSON block without inventing a wall-clock time.
/// The caller must retain its original RPC evidence and enforce response bounds.
pub fn normalize_confirmed_http_block(
    slot: u64,
    result: &serde_json::Value,
) -> anyhow::Result<yellowstone_grpc_proto::prelude::SubscribeUpdateBlock> {
    block::parse(slot, result)
}

use anyhow::{bail, ensure, Context, Result};
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    Client, Url,
};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;

#[derive(Clone)]
pub(crate) struct ConfirmedHttpRecovery {
    client: Client,
    url: Url,
    range_slots: u64,
    max_response_bytes: usize,
    sequence: Arc<AtomicU64>,
}
#[derive(Debug)]
pub(crate) struct RecoveredBlock {
    pub block: SubscribeUpdateBlock,
    /// Exact bounded JSON-RPC response, independent from the constructed proto.
    pub raw_response: Vec<u8>,
}
impl ConfirmedHttpRecovery {
    pub(crate) fn new(
        url: &str,
        header: Option<(&str, &str)>,
        range_slots: u64,
        max_response_bytes: usize,
        timeout: Duration,
    ) -> Result<Self> {
        let url = Url::parse(url).context("http_recovery_invalid_broker_url")?;
        ensure!(
            url.scheme() == "http"
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
                && url.host_str().is_some_and(|host| host == "localhost"
                    || host
                        .parse::<std::net::IpAddr>()
                        .is_ok_and(|ip| ip.is_loopback())),
            "http_recovery_requires_loopback_broker"
        );
        ensure!(
            range_slots > 0
                && range_slots <= 500_001
                && max_response_bytes > 0
                && !timeout.is_zero(),
            "http_recovery_invalid_bounds"
        );
        let mut headers = HeaderMap::new();
        if let Some((name, value)) = header {
            let mut value = HeaderValue::from_str(value).context("invalid broker token")?;
            value.set_sensitive(true);
            headers.insert(HeaderName::from_bytes(name.as_bytes())?, value);
        }
        let client = Client::builder()
            .default_headers(headers)
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()?;
        Ok(Self {
            client,
            url,
            range_slots,
            max_response_bytes,
            sequence: Arc::new(AtomicU64::new(1)),
        })
    }
    async fn request(&self, method: &str, params: Value) -> Result<(Value, Vec<u8>)> {
        let id = self.sequence.fetch_add(1, Ordering::Relaxed);
        let mut response = self
            .client
            .post(self.url.clone())
            .json(&json!({"jsonrpc":"2.0", "id":id, "method":method, "params":params}))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("http_recovery_send: {}", e.without_url()))?;
        let status = response.status();
        ensure!(
            response
                .content_length()
                .is_none_or(|n| n <= self.max_response_bytes as u64),
            "http_recovery_response_limit"
        );
        let mut raw = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|e| anyhow::anyhow!("http_recovery_read: {}", e.without_url()))?
        {
            ensure!(
                chunk.len() <= self.max_response_bytes.saturating_sub(raw.len()),
                "http_recovery_response_limit"
            );
            raw.extend_from_slice(&chunk);
        }
        let envelope: Value = serde_json::from_slice(&raw).with_context(|| {
            format!("http_recovery_invalid_json http_status={}", status.as_u16())
        })?;
        ensure!(
            envelope["jsonrpc"] == "2.0" && envelope["id"].as_u64() == Some(id),
            "http_recovery_response_identity"
        );
        if let Some(error) = envelope.get("error").filter(|v| !v.is_null()) {
            let code = error["code"]
                .as_i64()
                .context("http_recovery_invalid_rpc_error")?;
            let message: String = error["message"]
                .as_str()
                .unwrap_or("missing message")
                .chars()
                .take(512)
                .collect();
            bail!(
                "http_recovery_rpc_error http_status={} code={code} message={message}",
                status.as_u16()
            );
        }
        ensure!(
            status.is_success(),
            "http_recovery_http_status {}",
            status.as_u16()
        );
        let result = envelope
            .get("result")
            .context("http_recovery_missing_result")?
            .clone();
        Ok((result, raw))
    }
    pub(crate) async fn slots(&self, start: u64, end: u64) -> Result<Vec<u64>> {
        ensure!(
            end >= start && end - start < self.range_slots,
            "http_recovery_range_limit"
        );
        let (result, _) = self
            .request("getBlocks", json!([start,end,{"commitment":"confirmed"}]))
            .await?;
        let rows = result.as_array().context("http_recovery_slot_list")?;
        ensure!(
            rows.len() as u64 <= self.range_slots,
            "http_recovery_slot_list_limit"
        );
        let mut slots = Vec::with_capacity(rows.len());
        for row in rows {
            let slot = row.as_u64().context("http_recovery_noninteger_slot")?;
            ensure!(
                slot >= start && slot <= end && slots.last().is_none_or(|last| *last < slot),
                "http_recovery_unsorted_or_outside_slots"
            );
            slots.push(slot);
        }
        Ok(slots)
    }
    pub(crate) async fn block(&self, slot: u64) -> Result<RecoveredBlock> {
        let (result, raw_response) = self
            .request(
                "getBlock",
                json!([slot, {
                    "commitment":"confirmed", "encoding":"json", "transactionDetails":"full",
                    "maxSupportedTransactionVersion":1, "rewards":true
                }]),
            )
            .await?;
        // null means unavailable/not yet confirmed; it is never a skipped slot.
        let block = block::parse(slot, &result)?;
        Ok(RecoveredBlock {
            block,
            raw_response,
        })
    }
}

#[cfg(test)]
#[path = "../../source_tests/http_recovery_tests.rs"]
mod tests;
