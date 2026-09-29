//! Read-only retry bounds are independent from the broker's immutable owner clock.
use super::{delivery_error as errors, response, ConfirmedHttpRecovery};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use std::{
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

const ATTEMPTS: u8 = 3;
fn backoff(attempt: u8) -> Duration {
    Duration::from_millis(250 * u64::from(attempt))
}
impl ConfirmedHttpRecovery {
    pub(super) async fn request(
        &self,
        method: &'static str,
        params: Value,
    ) -> Result<(Value, Vec<u8>)> {
        ensure!(
            matches!(method, "getBlocks" | "getBlock"),
            "http_recovery_read_only_method_required"
        );
        let operation = Instant::now();
        let limit = self.timeout.saturating_mul(u32::from(ATTEMPTS)) + Duration::from_millis(750);
        let slot = params[0].as_u64().context("http_recovery_request_slot")?;
        for attempt in 1..=ATTEMPTS {
            let left = limit
                .checked_sub(operation.elapsed())
                .filter(|d| !d.is_zero())
                .context("http_recovery_operation_deadline_exhausted")?;
            let mut timeout = self.timeout.min(left);
            if let Some(session) = self.session_deadline.remaining()? {
                timeout = timeout.min(session);
            }
            let id = self.sequence.fetch_add(1, Ordering::Relaxed);
            let started = Instant::now();
            match self
                .attempt(method, &params, id, slot, attempt, started, timeout)
                .await
            {
                Ok(value) => return Ok(value),
                Err(error) => {
                    let transient = errors::retryable(&error);
                    let retry = attempt < ATTEMPTS && transient;
                    errors::report(&error, method, id, slot, attempt, started, timeout, retry);
                    if !retry {
                        if transient && attempt == ATTEMPTS {
                            return Err(error)
                                .context("http_recovery_attempts_exhausted attempts=3");
                        }
                        return Err(error);
                    }
                    let wait = backoff(attempt);
                    if operation.elapsed().saturating_add(wait) >= limit {
                        return Err(error).context("http_recovery_operation_deadline_exhausted");
                    }
                    match self.session_deadline.remaining() {
                        Ok(Some(session)) if wait >= session => {
                            return Err(error).context("http_recovery_session_deadline_exhausted")
                        }
                        Err(ceiling) => return Err(error).context(ceiling.to_string()),
                        _ => {}
                    }
                    tokio::time::sleep(wait).await;
                }
            }
        }
        unreachable!("bounded attempts always return")
    }
    async fn attempt(
        &self,
        method: &'static str,
        params: &Value,
        id: u64,
        slot: u64,
        attempt: u8,
        started: Instant,
        timeout: Duration,
    ) -> Result<(Value, Vec<u8>)> {
        let mut response = self
            .client
            .post(self.url.clone())
            .timeout(timeout)
            .json(&json!({"jsonrpc":"2.0", "id":id, "method":method, "params":params}))
            .send()
            .await
            .map_err(|e| {
                errors::ClientFailure::new(
                    e,
                    method,
                    id,
                    slot,
                    "client_headers",
                    attempt,
                    started,
                    timeout,
                )
            })?;
        self.session_deadline.observe(response.headers())?;
        let status = response.status();
        ensure!(
            response
                .content_length()
                .is_none_or(|n| n <= self.max_response_bytes as u64),
            "http_recovery_response_limit"
        );
        let mut raw = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|e| {
            errors::ClientFailure::new(
                e,
                method,
                id,
                slot,
                "client_body",
                attempt,
                started,
                timeout,
            )
        })? {
            ensure!(
                chunk.len() <= self.max_response_bytes.saturating_sub(raw.len()),
                "http_recovery_response_limit"
            );
            raw.extend_from_slice(&chunk);
        }
        let result = match response::interpret(status.as_u16(), id, method, &raw) {
            Err(error) => {
                if let Some(broker) = error.downcast_ref::<errors::BrokerFailure>() {
                    ensure!(
                        broker.slot.is_none_or(|value| value == slot),
                        "http_recovery_broker_slot_identity"
                    );
                }
                return Err(error);
            }
            Ok(result) => result,
        };
        // A late success cannot extend the immutable clock after body delivery.
        self.session_deadline.remaining()?;
        Ok((result, raw))
    }
}
