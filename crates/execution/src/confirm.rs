use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
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

#[derive(Debug, Clone)]
pub struct ConfirmationResult {
    pub status: ConfirmationStatus,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub network_fee_lamports: Option<u64>,
    pub network_fee_lookup_error: Option<String>,
    pub detail: String,
}

pub trait OrderConfirmer {
    fn confirm(&self, tx_signature: &str, deadline: DateTime<Utc>) -> Result<ConfirmationResult>;
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
    fn confirm(&self, _tx_signature: &str, _deadline: DateTime<Utc>) -> Result<ConfirmationResult> {
        Err(anyhow!("{}", self.detail))
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PaperOrderConfirmer;

impl OrderConfirmer for PaperOrderConfirmer {
    fn confirm(&self, _tx_signature: &str, deadline: DateTime<Utc>) -> Result<ConfirmationResult> {
        let now = Utc::now();
        if now > deadline {
            return Ok(ConfirmationResult {
                status: ConfirmationStatus::Timeout,
                confirmed_at: None,
                network_fee_lamports: None,
                network_fee_lookup_error: None,
                detail: "paper_confirm_timeout".to_string(),
            });
        }
        Ok(ConfirmationResult {
            status: ConfirmationStatus::Confirmed,
            confirmed_at: Some(now),
            network_fee_lamports: None,
            network_fee_lookup_error: None,
            detail: "paper_confirm_ok".to_string(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct RpcOrderConfirmer {
    endpoints: Vec<String>,
    client: Client,
}

impl RpcOrderConfirmer {
    pub fn new(primary_url: &str, fallback_url: &str, timeout_ms: u64) -> Option<Self> {
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
        Some(Self { endpoints, client })
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
        let mut confirmation = parse_confirmation_from_rpc_body(&body, now)?;
        if matches!(confirmation.status, ConfirmationStatus::Confirmed) {
            match self.query_transaction_fee_lamports(endpoint, tx_signature) {
                Ok(value) => {
                    confirmation.network_fee_lamports = value;
                    confirmation.network_fee_lookup_error = None;
                }
                Err(error_class) => {
                    warn!(
                        endpoint = %redacted_endpoint_label(endpoint),
                        tx_signature,
                        error_class = error_class.as_str(),
                        "fee lookup failed for confirmed signature; proceeding without network fee"
                    );
                    confirmation.network_fee_lamports = None;
                    confirmation.network_fee_lookup_error = Some(error_class.as_str().to_string());
                }
            }
        }
        Ok(confirmation)
    }

    fn query_transaction_fee_lamports(
        &self,
        endpoint: &str,
        tx_signature: &str,
    ) -> std::result::Result<Option<u64>, FeeLookupErrorClass> {
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
        parse_transaction_fee_lamports_from_rpc_body(&body)
    }
}

impl OrderConfirmer for RpcOrderConfirmer {
    fn confirm(&self, tx_signature: &str, deadline: DateTime<Utc>) -> Result<ConfirmationResult> {
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
                detail: "deadline_exceeded_before_rpc_query".to_string(),
            });
        }

        let mut last_error: Option<anyhow::Error> = None;
        for endpoint in &self.endpoints {
            match self.query_signature_status(endpoint, tx_signature, now) {
                Ok(result) => return Ok(result),
                Err(error) => last_error = Some(error),
            }
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
            detail: "signature_not_found_yet".to_string(),
        });
    };

    if status_row.is_null() {
        return Ok(ConfirmationResult {
            status: ConfirmationStatus::Timeout,
            confirmed_at: None,
            network_fee_lamports: None,
            network_fee_lookup_error: None,
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
            detail: format!("signature_{}", confirmation_status),
        });
    }

    Ok(ConfirmationResult {
        status: ConfirmationStatus::Timeout,
        confirmed_at: None,
        network_fee_lamports: None,
        network_fee_lookup_error: None,
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

fn parse_transaction_fee_lamports_from_rpc_body(
    body: &Value,
) -> std::result::Result<Option<u64>, FeeLookupErrorClass> {
    if let Some(error_payload) = body.get("error") {
        let _ = error_payload;
        return Err(FeeLookupErrorClass::RpcErrorPayload);
    }
    let Some(result) = body.get("result") else {
        return Ok(None);
    };
    if result.is_null() {
        return Ok(None);
    }
    let fee = result
        .get("meta")
        .and_then(|meta| meta.get("fee"))
        .and_then(Value::as_u64);
    if fee.map(|value| value > i64::MAX as u64).unwrap_or(false) {
        return Err(FeeLookupErrorClass::OutOfRange);
    }
    Ok(fee)
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
        Ok(())
    }

    #[test]
    fn parse_transaction_fee_lamports_from_rpc_body_returns_fee() -> Result<()> {
        let body = json!({
            "jsonrpc": "2.0",
            "result": {
                "meta": {
                    "fee": 5000
                }
            },
            "id": 1
        });
        let fee = parse_transaction_fee_lamports_from_rpc_body(&body)
            .map_err(|value| anyhow!("unexpected fee parser error: {:?}", value))?;
        assert_eq!(fee, Some(5000));
        Ok(())
    }

    #[test]
    fn parse_transaction_fee_lamports_from_rpc_body_handles_not_found() -> Result<()> {
        let body = json!({
            "jsonrpc": "2.0",
            "result": null,
            "id": 1
        });
        let fee = parse_transaction_fee_lamports_from_rpc_body(&body)
            .map_err(|value| anyhow!("unexpected fee parser error: {:?}", value))?;
        assert_eq!(fee, None);
        Ok(())
    }

    #[test]
    fn parse_transaction_fee_lamports_from_rpc_body_classifies_rpc_error_payload() {
        let body = json!({
            "jsonrpc": "2.0",
            "error": {"code": -32000, "message": "rpc error"},
            "id": 1
        });
        let error = parse_transaction_fee_lamports_from_rpc_body(&body)
            .expect_err("rpc error payload should return classified error");
        assert_eq!(error, FeeLookupErrorClass::RpcErrorPayload);
    }

    #[test]
    fn parse_transaction_fee_lamports_from_rpc_body_rejects_fee_above_i64_max() {
        let body = json!({
            "jsonrpc": "2.0",
            "result": {
                "meta": {
                    "fee": (i64::MAX as u64).saturating_add(1)
                }
            },
            "id": 1
        });
        let error = parse_transaction_fee_lamports_from_rpc_body(&body)
            .expect_err("fee above i64::MAX should return classified error");
        assert_eq!(error, FeeLookupErrorClass::OutOfRange);
    }

    #[test]
    fn redacted_endpoint_label_drops_path_and_query() {
        let label = redacted_endpoint_label("https://rpc.example.org/v1?api-key=secret");
        assert_eq!(label, "https://rpc.example.org");
    }
}
