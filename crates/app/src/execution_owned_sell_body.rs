//! Shared HTTP byte boundary. No large allowance for ordinary financial RPCs.
use anyhow::{ensure, Result};
use serde_json::Value;
use std::{future::Future, pin::Pin};
pub(crate) type Chunk<'a, C> = Pin<Box<dyn Future<Output = Result<Option<C>>> + Send + 'a>>;
use tokio::time::Instant;

pub(crate) const SMALL_RPC_BYTES: usize = 1 << 20;
// Largest saved body 6,500,048 bytes; full blocks alone get 8 MiB, not unlimited.
pub(crate) const FULL_BLOCK_BYTES: usize = 8 << 20;
pub(crate) const AGGREGATE_WIRE_BYTES: usize = 32 << 20;
pub(crate) const RETAINED_JSON_CHARGE: usize = 256 << 20;
#[derive(Clone, Copy)]
pub(crate) struct Head {
    pub endpoint_matches: bool,
    pub status: u16,
    pub content_length: Option<u64>,
}
pub(crate) fn fractional_limit(request: &Value) -> usize {
    if request["method"] == "getBlock" && request["params"][1]["transactionDetails"] == "full" {
        FULL_BLOCK_BYTES
    } else {
        SMALL_RPC_BYTES
    }
}
pub(crate) async fn bytes<S: Send + 'static, C: AsRef<[u8]> + Send + 'static>(
    head: Head,
    cap: usize,
    deadline: Instant,
    check: &mut (impl FnMut() -> Result<()> + ?Sized),
    source: &mut S,
    mut next: impl for<'a> FnMut(&'a mut S) -> Chunk<'a, C>,
) -> Result<Vec<u8>> {
    check()?;
    ensure!(Instant::now() < deadline, "owned_sell_rpc_deadline");
    ensure!(
        head.endpoint_matches && (200..300).contains(&head.status),
        "owned_sell_rpc_endpoint_status"
    );
    ensure!(
        head.content_length.unwrap_or(0) <= cap as u64,
        "owned_sell_rpc_response_bound"
    );
    let mut bytes = Vec::new();
    loop {
        check()?;
        ensure!(Instant::now() < deadline, "owned_sell_rpc_deadline");
        let chunk = tokio::time::timeout_at(deadline, next(source))
            .await
            .map_err(|_| anyhow::anyhow!("owned_sell_rpc_deadline"))??;
        check()?;
        ensure!(Instant::now() < deadline, "owned_sell_rpc_deadline");
        let Some(chunk) = chunk else { break };
        let chunk = chunk.as_ref();
        ensure!(
            chunk.len() <= cap - bytes.len(),
            "owned_sell_rpc_response_bound"
        );
        let needed = bytes.len() + chunk.len();
        if needed > bytes.capacity() {
            let capacity = needed.max((bytes.capacity().max(8192) * 2).min(cap));
            bytes.try_reserve_exact(capacity - bytes.len())?;
        }
        bytes.extend_from_slice(chunk);
    }
    ensure!(
        head.content_length.is_none_or(|n| n == bytes.len() as u64),
        "owned_sell_rpc_length_conflict"
    );
    Ok(bytes)
}
pub(crate) fn envelope(bytes: &[u8], request: &Value) -> Result<Value> {
    let value: Value =
        serde_json::from_slice(bytes).map_err(|_| anyhow::anyhow!("owned_sell_rpc_json"))?;
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
    Ok(value)
}
/// Conservative admission accounting before JSON allocation, not an RSS measurement.
/// Each structural mark bounds a Value/map/vector slot; 4*wire covers strings/keys
/// and parser scratch. Aggregate charges are never refunded within a collection.
#[derive(Default)]
pub(crate) struct Budget {
    wire: usize,
    retained: usize,
}
impl Budget {
    pub(crate) fn remaining_wire(&self) -> usize {
        AGGREGATE_WIRE_BYTES - self.wire
    }
    pub(crate) fn charge(&mut self, bytes: &[u8]) -> Result<()> {
        ensure!(
            bytes.len() <= AGGREGATE_WIRE_BYTES - self.wire,
            "fraction_wire_capacity"
        );
        let (mut inside, mut escape, mut marks) = (false, false, 1usize);
        for &c in bytes {
            if inside {
                if escape {
                    escape = false;
                } else if c == b'\\' {
                    escape = true;
                } else if c == b'"' {
                    inside = false;
                }
            } else if c == b'"' {
                inside = true;
                marks += 1;
            } else if b"[]{},:".contains(&c) {
                marks += 1;
            }
        }
        let charge = 4 * bytes.len() + 256 * marks;
        ensure!(
            charge <= RETAINED_JSON_CHARGE - self.retained,
            "fraction_retained_capacity"
        );
        self.wire += bytes.len();
        self.retained += charge;
        Ok(())
    }
    pub(crate) fn decode(&mut self, bytes: &[u8], request: &Value) -> Result<Value> {
        self.charge(bytes)?;
        envelope(bytes, request)
    }
}
