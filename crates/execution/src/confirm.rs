use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::TokenQuantity;
use reqwest::blocking::Client;
use serde_json::{json, Value};
use std::time::Duration as StdDuration;
use tracing::warn;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfirmationStatus {
    Confirmed,
    Failed,
    Timeout,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ObservedExecutionFill {
    pub signer_balance_delta_lamports: i64,
    pub token_delta_qty: f64,
    pub token_delta_exact: Option<TokenQuantity>,
}

#[derive(Debug, Clone)]
pub struct ConfirmationResult {
    pub status: ConfirmationStatus,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub network_fee_lamports: Option<u64>,
    pub network_fee_lookup_error: Option<String>,
    pub observed_fill: Option<ObservedExecutionFill>,
    pub detail: String,
}

pub trait OrderConfirmer {
    fn confirm(
        &self,
        tx_signature: &str,
        token_mint: &str,
        deadline: DateTime<Utc>,
    ) -> Result<ConfirmationResult>;
}

#[derive(Debug, Clone)]
pub struct FailClosedOrderConfirmer {
    detail: String,
}

impl FailClosedOrderConfirmer {
    pub fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }
}

impl OrderConfirmer for FailClosedOrderConfirmer {
    fn confirm(
        &self,
        _tx_signature: &str,
        _token_mint: &str,
        _deadline: DateTime<Utc>,
    ) -> Result<ConfirmationResult> {
        Err(anyhow!("{}", self.detail))
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PaperOrderConfirmer;

impl OrderConfirmer for PaperOrderConfirmer {
    fn confirm(
        &self,
        _tx_signature: &str,
        _token_mint: &str,
        deadline: DateTime<Utc>,
    ) -> Result<ConfirmationResult> {
        let now = Utc::now();
        if now > deadline {
            return Ok(ConfirmationResult {
                status: ConfirmationStatus::Timeout,
                confirmed_at: None,
                network_fee_lamports: None,
                network_fee_lookup_error: None,
                observed_fill: None,
                detail: "paper_confirm_timeout".to_string(),
            });
        }
        Ok(ConfirmationResult {
            status: ConfirmationStatus::Confirmed,
            confirmed_at: Some(now),
            network_fee_lamports: None,
            network_fee_lookup_error: None,
            observed_fill: None,
            detail: "paper_confirm_ok".to_string(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct RpcOrderConfirmer {
    endpoints: Vec<String>,
    execution_signer_pubkey: String,
    client: Client,
}

impl RpcOrderConfirmer {
    pub fn new(
        primary_url: &str,
        fallback_url: &str,
        timeout_ms: u64,
        execution_signer_pubkey: &str,
    ) -> Option<Self> {
        let mut endpoints = Vec::new();
        let primary = primary_url.trim();
        if !primary.is_empty() {
            endpoints.push(primary.to_string());
        }
        let fallback = fallback_url.trim();
        if !fallback.is_empty() && fallback != primary {
            endpoints.push(fallback.to_string());
        }
        if endpoints.is_empty() {
            return None;
        }

        let timeout = StdDuration::from_millis(timeout_ms.max(500));
        let client = match Client::builder().timeout(timeout).build() {
            Ok(value) => value,
            Err(_) => return None,
        };
        Some(Self {
            endpoints,
            execution_signer_pubkey: execution_signer_pubkey.trim().to_string(),
            client,
        })
    }

    fn query_signature_status(
        &self,
        endpoint: &str,
        tx_signature: &str,
        now: DateTime<Utc>,
    ) -> Result<ConfirmationResult> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignatureStatuses",
            "params": [[tx_signature], {"searchTransactionHistory": true}]
        });
        let response = self
            .client
            .post(endpoint)
            .json(&payload)
            .send()
            .context("rpc request failed")?;
        let body: Value = response.json().context("invalid rpc json")?;
        parse_confirmation_from_rpc_body(&body, now)
    }

    fn query_confirmed_transaction_observation(
        &self,
        endpoint: &str,
        tx_signature: &str,
        token_mint: &str,
    ) -> std::result::Result<ConfirmedTransactionObservation, FeeLookupErrorClass> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [
                tx_signature,
                {
                    "encoding": "json",
                    "commitment": "confirmed",
                    "maxSupportedTransactionVersion": 0
                }
            ]
        });
        let response = self
            .client
            .post(endpoint)
            .json(&payload)
            .send()
            .map_err(classify_reqwest_send_error)?;
        let body: Value = response
            .json()
            .map_err(|_| FeeLookupErrorClass::InvalidJson)?;
        parse_confirmed_transaction_observation_from_rpc_body(
            &body,
            &self.execution_signer_pubkey,
            token_mint,
        )
    }
}

impl OrderConfirmer for RpcOrderConfirmer {
    fn confirm(
        &self,
        tx_signature: &str,
        token_mint: &str,
        deadline: DateTime<Utc>,
    ) -> Result<ConfirmationResult> {
        let now = Utc::now();
        if tx_signature.trim().is_empty() {
            return Err(anyhow!("empty tx signature for confirmation"));
        }
        if now > deadline {
            return Ok(ConfirmationResult {
                status: ConfirmationStatus::Timeout,
                confirmed_at: None,
                network_fee_lamports: None,
                network_fee_lookup_error: None,
                observed_fill: None,
                detail: "deadline_exceeded_before_rpc_query".to_string(),
            });
        }

        let mut last_error: Option<anyhow::Error> = None;
        let mut last_timeout: Option<ConfirmationResult> = None;
        let mut confirmed: Option<ConfirmationResult> = None;
        for endpoint in &self.endpoints {
            match self.query_signature_status(endpoint, tx_signature, now) {
                Ok(result) => match result.status {
                    ConfirmationStatus::Confirmed => {
                        confirmed = Some(result);
                        break;
                    }
                    ConfirmationStatus::Failed => return Ok(result),
                    ConfirmationStatus::Timeout => {
                        if last_timeout.is_none() {
                            last_timeout = Some(result);
                        }
                    }
                },
                Err(error) => last_error = Some(error),
            }
        }

        if let Some(mut result) = confirmed {
            let mut best_observation = ConfirmedTransactionObservation {
                network_fee_lamports: None,
                observed_fill: None,
            };
            let mut last_observation_error: Option<FeeLookupErrorClass> = None;
            for endpoint in &self.endpoints {
                match self.query_confirmed_transaction_observation(
                    endpoint,
                    tx_signature,
                    token_mint,
                ) {
                    Ok(observation) => {
                        if best_observation.network_fee_lamports.is_none() {
                            best_observation.network_fee_lamports =
                                observation.network_fee_lamports;
                        }
                        if best_observation.observed_fill.is_none() {
                            best_observation.observed_fill = observation.observed_fill;
                        }
                        if best_observation.network_fee_lamports.is_some()
                            && best_observation.observed_fill.is_some()
                        {
                            last_observation_error = None;
                            break;
                        }
                    }
                    Err(error_class) => {
                        last_observation_error = Some(error_class);
                        warn!(
                            endpoint = %redacted_endpoint_label(endpoint),
                            tx_signature,
                            error_class = error_class.as_str(),
                            "confirmed transaction observation lookup failed; trying next RPC endpoint"
                        );
                    }
                }
            }
            result.network_fee_lamports = best_observation.network_fee_lamports;
            result.observed_fill = best_observation.observed_fill;
            result.network_fee_lookup_error = if result.network_fee_lamports.is_none() {
                last_observation_error.map(|value| value.as_str().to_string())
            } else {
                None
            };
            return Ok(result);
        }

        if let Some(result) = last_timeout {
            return Ok(result);
        }
        Err(last_error.unwrap_or_else(|| anyhow!("all rpc confirmation endpoints failed")))
    }
}

fn parse_confirmation_from_rpc_body(
    body: &Value,
    now: DateTime<Utc>,
) -> Result<ConfirmationResult> {
    if let Some(error_payload) = body.get("error") {
        return Err(anyhow!("rpc returned error payload: {}", error_payload));
    }

    let value = body
        .get("result")
        .and_then(|result| result.get("value"))
        .and_then(|value| value.get(0));

    let Some(status_row) = value else {
        return Ok(ConfirmationResult {
            status: ConfirmationStatus::Timeout,
            confirmed_at: None,
            network_fee_lamports: None,
            network_fee_lookup_error: None,
            observed_fill: None,
            detail: "signature_not_found_yet".to_string(),
        });
    };

    if status_row.is_null() {
        return Ok(ConfirmationResult {
            status: ConfirmationStatus::Timeout,
            confirmed_at: None,
            network_fee_lamports: None,
            network_fee_lookup_error: None,
            observed_fill: None,
            detail: "signature_pending".to_string(),
        });
    }

    if let Some(err_payload) = status_row.get("err") {
        if !err_payload.is_null() {
            return Ok(ConfirmationResult {
                status: ConfirmationStatus::Failed,
                confirmed_at: None,
                network_fee_lamports: None,
                network_fee_lookup_error: None,
                observed_fill: None,
                detail: format!("signature_failed err={}", err_payload),
            });
        }
    }

    let confirmation_status = status_row
        .get("confirmationStatus")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if matches!(confirmation_status, "confirmed" | "finalized") {
        return Ok(ConfirmationResult {
            status: ConfirmationStatus::Confirmed,
            confirmed_at: Some(now),
            network_fee_lamports: None,
            network_fee_lookup_error: None,
            observed_fill: None,
            detail: format!("signature_{}", confirmation_status),
        });
    }

    Ok(ConfirmationResult {
        status: ConfirmationStatus::Timeout,
        confirmed_at: None,
        network_fee_lamports: None,
        network_fee_lookup_error: None,
        observed_fill: None,
        detail: format!(
            "signature_not_confirmed_yet confirmation_status={}",
            if confirmation_status.is_empty() {
                "unknown"
            } else {
                confirmation_status
            }
        ),
    })
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct ConfirmedTransactionObservation {
    network_fee_lamports: Option<u64>,
    observed_fill: Option<ObservedExecutionFill>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct OwnedTokenBalance {
    qty: f64,
    exact: Option<TokenQuantity>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct OwnedTokenDelta {
    qty: f64,
    exact: Option<TokenQuantity>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct ParsedUiTokenAmount {
    qty: f64,
    raw_amount: Option<u64>,
    decimals: Option<u8>,
}

fn parse_confirmed_transaction_observation_from_rpc_body(
    body: &Value,
    execution_signer_pubkey: &str,
    token_mint: &str,
) -> std::result::Result<ConfirmedTransactionObservation, FeeLookupErrorClass> {
    if let Some(error_payload) = body.get("error") {
        let _ = error_payload;
        return Err(FeeLookupErrorClass::RpcErrorPayload);
    }
    let Some(result) = body.get("result") else {
        return Ok(ConfirmedTransactionObservation {
            network_fee_lamports: None,
            observed_fill: None,
        });
    };
    if result.is_null() {
        return Ok(ConfirmedTransactionObservation {
            network_fee_lamports: None,
            observed_fill: None,
        });
    }
    let fee = result
        .get("meta")
        .and_then(|meta| meta.get("fee"))
        .and_then(Value::as_u64);
    if fee.map(|value| value > i64::MAX as u64).unwrap_or(false) {
        return Err(FeeLookupErrorClass::OutOfRange);
    }

    let observed_fill = if execution_signer_pubkey.is_empty() || token_mint.is_empty() {
        None
    } else {
        match (
            parse_signer_balance_delta_lamports(result, execution_signer_pubkey)?,
            parse_owned_token_delta(result, execution_signer_pubkey, token_mint)?,
        ) {
            (Some(signer_balance_delta_lamports), Some(token_delta))
                if token_delta.qty.abs() > 1e-12 =>
            {
                Some(ObservedExecutionFill {
                    signer_balance_delta_lamports,
                    token_delta_qty: token_delta.qty,
                    token_delta_exact: token_delta.exact,
                })
            }
            _ => None,
        }
    };

    Ok(ConfirmedTransactionObservation {
        network_fee_lamports: fee,
        observed_fill,
    })
}

fn parse_signer_balance_delta_lamports(
    result: &Value,
    execution_signer_pubkey: &str,
) -> std::result::Result<Option<i64>, FeeLookupErrorClass> {
    let Some(account_keys) = result
        .get("transaction")
        .and_then(|tx| tx.get("message"))
        .and_then(|message| message.get("accountKeys"))
        .and_then(Value::as_array)
    else {
        return Ok(None);
    };
    let Some(signer_index) = account_keys
        .iter()
        .position(|value| account_key_matches(value, execution_signer_pubkey))
    else {
        return Ok(None);
    };
    let Some(pre_balance) = result
        .get("meta")
        .and_then(|meta| meta.get("preBalances"))
        .and_then(Value::as_array)
        .and_then(|balances| balances.get(signer_index))
        .and_then(Value::as_u64)
    else {
        return Ok(None);
    };
    let Some(post_balance) = result
        .get("meta")
        .and_then(|meta| meta.get("postBalances"))
        .and_then(Value::as_array)
        .and_then(|balances| balances.get(signer_index))
        .and_then(Value::as_u64)
    else {
        return Ok(None);
    };

    let delta = i128::from(post_balance) - i128::from(pre_balance);
    if delta < i128::from(i64::MIN) || delta > i128::from(i64::MAX) {
        return Err(FeeLookupErrorClass::OutOfRange);
    }
    Ok(Some(delta as i64))
}

fn parse_owned_token_delta(
    result: &Value,
    execution_signer_pubkey: &str,
    token_mint: &str,
) -> std::result::Result<Option<OwnedTokenDelta>, FeeLookupErrorClass> {
    let pre_qty = sum_owned_token_balance(
        result
            .get("meta")
            .and_then(|meta| meta.get("preTokenBalances")),
        execution_signer_pubkey,
        token_mint,
    )?;
    let post_qty = sum_owned_token_balance(
        result
            .get("meta")
            .and_then(|meta| meta.get("postTokenBalances")),
        execution_signer_pubkey,
        token_mint,
    )?;

    match (pre_qty, post_qty) {
        (None, None) => Ok(None),
        (Some(pre), Some(post)) => Ok(Some(OwnedTokenDelta {
            qty: post.qty - pre.qty,
            exact: exact_token_delta(pre.exact, post.exact),
        })),
        (Some(pre), None) => Ok(Some(OwnedTokenDelta {
            qty: -pre.qty,
            exact: None,
        })),
        (None, Some(post)) => Ok(Some(OwnedTokenDelta {
            qty: post.qty,
            exact: None,
        })),
    }
}

fn sum_owned_token_balance(
    entries: Option<&Value>,
    execution_signer_pubkey: &str,
    token_mint: &str,
) -> std::result::Result<Option<OwnedTokenBalance>, FeeLookupErrorClass> {
    let Some(entries) = entries.and_then(Value::as_array) else {
        return Ok(None);
    };

    let mut matched = false;
    let mut total = 0.0;
    let mut exact_unavailable = false;
    let mut exact_total_raw: Option<u64> = None;
    let mut exact_decimals: Option<u8> = None;
    for entry in entries {
        if entry.get("mint").and_then(Value::as_str) != Some(token_mint) {
            continue;
        }
        if entry.get("owner").and_then(Value::as_str) != Some(execution_signer_pubkey) {
            continue;
        }
        let qty = parse_ui_token_amount(
            entry
                .get("uiTokenAmount")
                .ok_or(FeeLookupErrorClass::InvalidJson)?,
        )?;
        matched = true;
        total += qty.qty;
        if exact_unavailable {
            continue;
        }
        match (qty.raw_amount, qty.decimals) {
            (Some(raw_amount), Some(decimals)) => match exact_decimals {
                Some(existing) if existing != decimals => {
                    exact_unavailable = true;
                    exact_total_raw = None;
                    exact_decimals = None;
                }
                Some(_) => {
                    exact_total_raw = Some(
                        exact_total_raw
                            .unwrap_or(0)
                            .checked_add(raw_amount)
                            .ok_or(FeeLookupErrorClass::OutOfRange)?,
                    );
                }
                None => {
                    exact_decimals = Some(decimals);
                    exact_total_raw = Some(raw_amount);
                }
            },
            _ => {
                exact_unavailable = true;
                exact_total_raw = None;
                exact_decimals = None;
            }
        }
    }

    Ok(matched.then_some(OwnedTokenBalance {
        qty: total,
        exact: if exact_unavailable {
            None
        } else {
            match (exact_total_raw, exact_decimals) {
                (Some(raw), Some(decimals)) => Some(TokenQuantity::new(raw, decimals)),
                _ => None,
            }
        },
    }))
}

fn exact_token_delta(
    pre: Option<TokenQuantity>,
    post: Option<TokenQuantity>,
) -> Option<TokenQuantity> {
    match (pre, post) {
        (Some(pre), Some(post)) if pre.decimals() == post.decimals() => {
            let raw_delta = if post.raw() >= pre.raw() {
                post.raw() - pre.raw()
            } else {
                pre.raw() - post.raw()
            };
            Some(TokenQuantity::new(raw_delta, post.decimals()))
        }
        _ => None,
    }
}

fn parse_ui_token_amount(
    value: &Value,
) -> std::result::Result<ParsedUiTokenAmount, FeeLookupErrorClass> {
    if let Some(text) = value.get("uiAmountString").and_then(Value::as_str) {
        let parsed = text
            .parse::<f64>()
            .map_err(|_| FeeLookupErrorClass::InvalidJson)?;
        if parsed.is_finite() {
            return Ok(ParsedUiTokenAmount {
                qty: parsed,
                raw_amount: value
                    .get("amount")
                    .and_then(Value::as_str)
                    .and_then(|raw| raw.parse::<u64>().ok()),
                decimals: value
                    .get("decimals")
                    .and_then(Value::as_u64)
                    .and_then(|raw| u8::try_from(raw).ok()),
            });
        }
        return Err(FeeLookupErrorClass::InvalidJson);
    }
    if let Some(parsed) = value.get("uiAmount").and_then(Value::as_f64) {
        if parsed.is_finite() {
            return Ok(ParsedUiTokenAmount {
                qty: parsed,
                raw_amount: value
                    .get("amount")
                    .and_then(Value::as_str)
                    .and_then(|raw| raw.parse::<u64>().ok()),
                decimals: value
                    .get("decimals")
                    .and_then(Value::as_u64)
                    .and_then(|raw| u8::try_from(raw).ok()),
            });
        }
        return Err(FeeLookupErrorClass::InvalidJson);
    }
    let Some(amount_raw) = value.get("amount").and_then(Value::as_str) else {
        return Err(FeeLookupErrorClass::InvalidJson);
    };
    let Some(decimals_raw) = value.get("decimals").and_then(Value::as_u64) else {
        return Err(FeeLookupErrorClass::InvalidJson);
    };
    let raw_amount = amount_raw
        .parse::<f64>()
        .map_err(|_| FeeLookupErrorClass::InvalidJson)?;
    if !raw_amount.is_finite() {
        return Err(FeeLookupErrorClass::InvalidJson);
    }
    let decimals = i32::try_from(decimals_raw).map_err(|_| FeeLookupErrorClass::OutOfRange)?;
    let normalized = raw_amount / 10f64.powi(decimals);
    if !normalized.is_finite() {
        return Err(FeeLookupErrorClass::InvalidJson);
    }
    let raw_amount = amount_raw
        .parse::<u64>()
        .map_err(|_| FeeLookupErrorClass::InvalidJson)?;
    Ok(ParsedUiTokenAmount {
        qty: normalized,
        raw_amount: Some(raw_amount),
        decimals: Some(u8::try_from(decimals_raw).map_err(|_| FeeLookupErrorClass::OutOfRange)?),
    })
}

fn account_key_matches(value: &Value, pubkey: &str) -> bool {
    value.as_str() == Some(pubkey) || value.get("pubkey").and_then(Value::as_str) == Some(pubkey)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FeeLookupErrorClass {
    Timeout,
    Connect,
    InvalidJson,
    RpcErrorPayload,
    OutOfRange,
    Other,
}

impl FeeLookupErrorClass {
    fn as_str(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Connect => "connect",
            Self::InvalidJson => "invalid_json",
            Self::RpcErrorPayload => "rpc_error_payload",
            Self::OutOfRange => "out_of_range",
            Self::Other => "other",
        }
    }
}

fn classify_reqwest_send_error(error: reqwest::Error) -> FeeLookupErrorClass {
    if error.is_timeout() {
        FeeLookupErrorClass::Timeout
    } else if error.is_connect() {
        FeeLookupErrorClass::Connect
    } else {
        FeeLookupErrorClass::Other
    }
}

fn redacted_endpoint_label(endpoint: &str) -> String {
    let endpoint = endpoint.trim();
    if endpoint.is_empty() {
        return "unknown".to_string();
    }
    match reqwest::Url::parse(endpoint) {
        Ok(url) => {
            let host = url.host_str().unwrap_or("unknown");
            match url.port() {
                Some(port) => format!("{}://{}:{}", url.scheme(), host, port),
                None => format!("{}://{}", url.scheme(), host),
            }
        }
        Err(_) => "invalid_endpoint".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread::{self, JoinHandle};
    use std::time::{Duration as StdDuration, Instant};

    fn spawn_jsonrpc_server(responses: Vec<String>) -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind jsonrpc test listener");
        listener
            .set_nonblocking(true)
            .expect("set nonblocking on test listener");
        let addr = listener.local_addr().expect("read listener addr");
        let handle = thread::spawn(move || {
            let deadline = Instant::now() + StdDuration::from_secs(5);
            let mut served = 0usize;
            while served < responses.len() {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream
                            .set_read_timeout(Some(StdDuration::from_secs(1)))
                            .expect("set read timeout");
                        let mut request = [0_u8; 4096];
                        let _ = stream.read(&mut request);
                        let body = &responses[served];
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        );
                        stream
                            .write_all(response.as_bytes())
                            .expect("write jsonrpc response");
                        served += 1;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            Instant::now() < deadline,
                            "jsonrpc test server timed out after serving {} of {} responses",
                            served,
                            responses.len()
                        );
                        thread::sleep(StdDuration::from_millis(10));
                    }
                    Err(error) => panic!("jsonrpc test server accept failed: {error}"),
                }
            }
        });
        (format!("http://{}", addr), handle)
    }

    #[test]
    fn parse_confirmation_from_rpc_body_returns_confirmed() -> Result<()> {
        let body = json!({
            "jsonrpc": "2.0",
            "result": {
                "value": [{
                    "err": null,
                    "confirmationStatus": "confirmed"
                }]
            },
            "id": 1
        });
        let result = parse_confirmation_from_rpc_body(&body, Utc::now())?;
        assert_eq!(result.status, ConfirmationStatus::Confirmed);
        assert_eq!(result.network_fee_lamports, None);
        assert_eq!(result.network_fee_lookup_error, None);
        assert_eq!(result.observed_fill, None);
        Ok(())
    }

    #[test]
    fn parse_confirmation_from_rpc_body_returns_failed_on_err_payload() -> Result<()> {
        let body = json!({
            "jsonrpc": "2.0",
            "result": {
                "value": [{
                    "err": {"InstructionError": [0, "Custom"]},
                    "confirmationStatus": "confirmed"
                }]
            },
            "id": 1
        });
        let result = parse_confirmation_from_rpc_body(&body, Utc::now())?;
        assert_eq!(result.status, ConfirmationStatus::Failed);
        assert_eq!(result.network_fee_lamports, None);
        assert_eq!(result.network_fee_lookup_error, None);
        assert_eq!(result.observed_fill, None);
        Ok(())
    }

    #[test]
    fn parse_confirmation_from_rpc_body_returns_timeout_for_pending() -> Result<()> {
        let body = json!({
            "jsonrpc": "2.0",
            "result": {
                "value": [null]
            },
            "id": 1
        });
        let result = parse_confirmation_from_rpc_body(&body, Utc::now())?;
        assert_eq!(result.status, ConfirmationStatus::Timeout);
        assert_eq!(result.network_fee_lamports, None);
        assert_eq!(result.network_fee_lookup_error, None);
        assert_eq!(result.observed_fill, None);
        Ok(())
    }

    #[test]
    fn parse_confirmed_transaction_observation_from_rpc_body_returns_fee() -> Result<()> {
        let body = json!({
            "jsonrpc": "2.0",
            "result": {
                "meta": {
                    "fee": 5000
                }
            },
            "id": 1
        });
        let observed = parse_confirmed_transaction_observation_from_rpc_body(
            &body,
            "signer-pubkey",
            "token-a",
        )
        .map_err(|value| anyhow!("unexpected fee parser error: {:?}", value))?;
        assert_eq!(observed.network_fee_lamports, Some(5000));
        assert_eq!(observed.observed_fill, None);
        Ok(())
    }

    #[test]
    fn parse_confirmed_transaction_observation_from_rpc_body_handles_not_found() -> Result<()> {
        let body = json!({
            "jsonrpc": "2.0",
            "result": null,
            "id": 1
        });
        let observed = parse_confirmed_transaction_observation_from_rpc_body(
            &body,
            "signer-pubkey",
            "token-a",
        )
        .map_err(|value| anyhow!("unexpected fee parser error: {:?}", value))?;
        assert_eq!(observed.network_fee_lamports, None);
        assert_eq!(observed.observed_fill, None);
        Ok(())
    }

    #[test]
    fn parse_confirmed_transaction_observation_from_rpc_body_classifies_rpc_error_payload() {
        let body = json!({
            "jsonrpc": "2.0",
            "error": {"code": -32000, "message": "rpc error"},
            "id": 1
        });
        let error = parse_confirmed_transaction_observation_from_rpc_body(
            &body,
            "signer-pubkey",
            "token-a",
        )
        .expect_err("rpc error payload should return classified error");
        assert_eq!(error, FeeLookupErrorClass::RpcErrorPayload);
    }

    #[test]
    fn parse_confirmed_transaction_observation_from_rpc_body_rejects_fee_above_i64_max() {
        let body = json!({
            "jsonrpc": "2.0",
            "result": {
                "meta": {
                    "fee": (i64::MAX as u64).saturating_add(1)
                }
            },
            "id": 1
        });
        let error = parse_confirmed_transaction_observation_from_rpc_body(
            &body,
            "signer-pubkey",
            "token-a",
        )
        .expect_err("fee above i64::MAX should return classified error");
        assert_eq!(error, FeeLookupErrorClass::OutOfRange);
    }

    #[test]
    fn parse_confirmed_transaction_observation_from_rpc_body_extracts_buy_fill() -> Result<()> {
        let body = json!({
            "jsonrpc": "2.0",
            "result": {
                "transaction": {
                    "message": {
                        "accountKeys": ["signer-pubkey", "other-account"]
                    }
                },
                "meta": {
                    "fee": 5000,
                    "preBalances": [1_000_000_000_u64, 0],
                    "postBalances": [899_995_000_u64, 0],
                    "preTokenBalances": [],
                    "postTokenBalances": [{
                        "mint": "token-a",
                        "owner": "signer-pubkey",
                        "uiTokenAmount": {
                            "uiAmountString": "2.5"
                        }
                    }]
                }
            },
            "id": 1
        });
        let observed = parse_confirmed_transaction_observation_from_rpc_body(
            &body,
            "signer-pubkey",
            "token-a",
        )
        .map_err(|value| anyhow!("unexpected parser error: {:?}", value))?;
        assert_eq!(observed.network_fee_lamports, Some(5000));
        assert_eq!(
            observed.observed_fill,
            Some(ObservedExecutionFill {
                signer_balance_delta_lamports: -100_005_000,
                token_delta_qty: 2.5,
                token_delta_exact: None,
            })
        );
        Ok(())
    }

    #[test]
    fn parse_confirmed_transaction_observation_from_rpc_body_extracts_sell_fill() -> Result<()> {
        let body = json!({
            "jsonrpc": "2.0",
            "result": {
                "transaction": {
                    "message": {
                        "accountKeys": [
                            {"pubkey": "signer-pubkey"},
                            {"pubkey": "other-account"}
                        ]
                    }
                },
                "meta": {
                    "fee": 5000,
                    "preBalances": [500_000_000_u64, 0],
                    "postBalances": [619_995_000_u64, 0],
                    "preTokenBalances": [{
                        "mint": "token-a",
                        "owner": "signer-pubkey",
                        "uiTokenAmount": {
                            "amount": "2500000",
                            "decimals": 6
                        }
                    }],
                    "postTokenBalances": []
                }
            },
            "id": 1
        });
        let observed = parse_confirmed_transaction_observation_from_rpc_body(
            &body,
            "signer-pubkey",
            "token-a",
        )
        .map_err(|value| anyhow!("unexpected parser error: {:?}", value))?;
        assert_eq!(observed.network_fee_lamports, Some(5000));
        assert_eq!(
            observed.observed_fill,
            Some(ObservedExecutionFill {
                signer_balance_delta_lamports: 119_995_000,
                token_delta_qty: -2.5,
                token_delta_exact: None,
            })
        );
        Ok(())
    }

    #[test]
    fn rpc_confirmer_uses_fallback_endpoint_for_observation_after_primary_confirm() -> Result<()> {
        let confirmed_status_body = json!({
            "jsonrpc": "2.0",
            "result": {
                "value": [{
                    "err": null,
                    "confirmationStatus": "confirmed"
                }]
            },
            "id": 1
        })
        .to_string();
        let fallback_observation_body = json!({
            "jsonrpc": "2.0",
            "result": {
                "transaction": {
                    "message": {
                        "accountKeys": ["signer-pubkey", "other-account"]
                    }
                },
                "meta": {
                    "fee": 5000,
                    "preBalances": [1_000_000_000_u64, 0],
                    "postBalances": [899_995_000_u64, 0],
                    "preTokenBalances": [],
                    "postTokenBalances": [{
                        "mint": "token-a",
                        "owner": "signer-pubkey",
                        "uiTokenAmount": {
                            "uiAmountString": "2.0"
                        }
                    }]
                }
            },
            "id": 1
        })
        .to_string();

        let (primary_url, primary_handle) =
            spawn_jsonrpc_server(vec![confirmed_status_body, "{".to_string()]);
        let (fallback_url, fallback_handle) = spawn_jsonrpc_server(vec![fallback_observation_body]);
        let confirmer = RpcOrderConfirmer::new(&primary_url, &fallback_url, 1_000, "signer-pubkey")
            .expect("rpc confirmer should initialize");

        let result = confirmer.confirm(
            "sig-observed-fill",
            "token-a",
            Utc::now() + chrono::Duration::seconds(5),
        )?;
        assert_eq!(result.status, ConfirmationStatus::Confirmed);
        assert_eq!(result.network_fee_lamports, Some(5_000));
        assert_eq!(result.network_fee_lookup_error, None);
        assert_eq!(
            result.observed_fill,
            Some(ObservedExecutionFill {
                signer_balance_delta_lamports: -100_005_000,
                token_delta_qty: 2.0,
                token_delta_exact: None,
            })
        );

        primary_handle.join().expect("join primary jsonrpc server");
        fallback_handle
            .join()
            .expect("join fallback jsonrpc server");
        Ok(())
    }

    #[test]
    fn parse_confirmed_transaction_observation_from_rpc_body_extracts_exact_qty_when_both_sides_are_exact(
    ) -> Result<()> {
        let body = json!({
            "jsonrpc": "2.0",
            "result": {
                "transaction": {
                    "message": {
                        "accountKeys": ["signer-pubkey", "other-account"]
                    }
                },
                "meta": {
                    "fee": 5000,
                    "preBalances": [1_000_000_000_u64, 0],
                    "postBalances": [899_995_000_u64, 0],
                    "preTokenBalances": [{
                        "mint": "token-a",
                        "owner": "signer-pubkey",
                        "uiTokenAmount": {
                            "amount": "500000",
                            "decimals": 6
                        }
                    }],
                    "postTokenBalances": [{
                        "mint": "token-a",
                        "owner": "signer-pubkey",
                        "uiTokenAmount": {
                            "amount": "2500000",
                            "decimals": 6
                        }
                    }]
                }
            },
            "id": 1
        });
        let observed = parse_confirmed_transaction_observation_from_rpc_body(
            &body,
            "signer-pubkey",
            "token-a",
        )
        .map_err(|value| anyhow!("unexpected parser error: {:?}", value))?;
        assert_eq!(
            observed.observed_fill,
            Some(ObservedExecutionFill {
                signer_balance_delta_lamports: -100_005_000,
                token_delta_qty: 2.0,
                token_delta_exact: Some(TokenQuantity::new(2_000_000, 6)),
            })
        );
        Ok(())
    }

    #[test]
    fn redacted_endpoint_label_drops_path_and_query() {
        let label = redacted_endpoint_label("https://rpc.example.org/v1?api-key=secret");
        assert_eq!(label, "https://rpc.example.org");
    }
}
