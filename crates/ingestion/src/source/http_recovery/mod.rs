//! Confirmed history from the bounded local budget broker. HTTP observations
//! retain their response bytes; conversion does not claim a protobuf wire match.
pub(crate) mod anchor_diagnostic;
mod anchor_evidence;
mod anchor_fetch;
mod block;
pub(crate) mod blocking_normalization;
mod delivery;
mod delivery_error;
mod envelope;
mod error;
pub(crate) mod identity;
mod identity_difference;
mod index_identity;
mod meta;
pub(crate) mod ordered_pipeline;
mod response;
mod session_deadline;
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

use anyhow::{ensure, Context, Result};
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    Client, Url,
};
use serde_json::{json, Value};
use std::{
    sync::{atomic::AtomicU64, Arc},
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
    timeout: Duration,
    session_deadline: Arc<session_deadline::SessionDeadline>,
    pub(crate) normalization: blocking_normalization::BlockingNormalization,
}
#[derive(Debug)]
pub(crate) struct RecoveredBlock {
    pub block: SubscribeUpdateBlock,
    /// Exact bounded JSON-RPC response, independent from the constructed proto.
    pub raw_response: Vec<u8>,
    anchor_evidence: Option<anchor_evidence::Pair>,
}
pub(super) struct RawResponse {
    id: u64,
    status: u16,
    bytes: Vec<u8>,
}
/// The ahead window retains exact bytes, never a JSON tree or normalized proto.
pub(crate) struct RawRecoveredBlock {
    slot: u64,
    response: RawResponse,
}
impl RawRecoveredBlock {
    pub(crate) fn from_response(slot: u64, status: u16, id: u64, bytes: Vec<u8>) -> Self {
        Self {
            slot,
            response: RawResponse { id, status, bytes },
        }
    }
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
            timeout,
            session_deadline: Arc::new(session_deadline::SessionDeadline::default()),
            normalization: blocking_normalization::BlockingNormalization::default(),
        })
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
        self.normalize_raw_block(self.raw_block(slot).await?)
    }
    pub(crate) async fn raw_block(&self, slot: u64) -> Result<RawRecoveredBlock> {
        let response = self
            .raw_request_with_evidence(
                "getBlock",
                json!([slot, {
                    "commitment":"confirmed", "encoding":"json", "transactionDetails":"full",
                    "maxSupportedTransactionVersion":1, "rewards":true
                }]),
                None,
            )
            .await?;
        Ok(RawRecoveredBlock::from_response(
            slot,
            response.status,
            response.id,
            response.bytes,
        ))
    }
    pub(crate) fn normalize_raw_block(&self, raw: RawRecoveredBlock) -> Result<RecoveredBlock> {
        // A completed response waiting behind application cannot renew or
        // outlive the broker's immutable session ceiling.
        self.session_deadline.remaining()?;
        let RawRecoveredBlock { slot, response } = raw;
        let result =
            response::interpret(response.status, response.id, "getBlock", &response.bytes)?;
        // null means unavailable/not yet confirmed; it is never a skipped slot.
        let block = block::parse(slot, &result)?;
        Ok(RecoveredBlock {
            block,
            raw_response: response.bytes,
            anchor_evidence: None,
        })
    }
}

#[cfg(test)]
#[path = "../../source_tests/http_recovery_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "../../source_tests/http_anchor_evidence_io_tests.rs"]
pub(crate) mod anchor_evidence_io_tests;

#[cfg(test)]
#[path = "../../source_tests/index_recovery_tests.rs"]
mod index_recovery_tests;

#[cfg(test)]
#[path = "../../source_tests/http_raw_envelope_tests.rs"]
mod raw_envelope_tests;

#[cfg(test)]
#[path = "../../source_tests/http_raw_fetch_tests.rs"]
mod raw_fetch_tests;

#[cfg(test)]
#[path = "../../source_tests/http_saved_raw_profile_tests.rs"]
mod saved_raw_profile_tests;

#[cfg(test)]
#[path = "../../source_tests/http_base58_equivalence_tests.rs"]
mod base58_equivalence_tests;
