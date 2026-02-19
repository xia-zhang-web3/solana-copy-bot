use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use reqwest::blocking::Client;
use serde_json::{json, Value};
use std::time::Duration as StdDuration;

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
    pub detail: String,
}

pub trait OrderConfirmer {
    fn confirm(&self, tx_signature: &str, deadline: DateTime<Utc>) -> Result<ConfirmationResult>;
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
                detail: "paper_confirm_timeout".to_string(),
            });
        }
        Ok(ConfirmationResult {
            status: ConfirmationStatus::Confirmed,
            confirmed_at: Some(now),
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
            .with_context(|| format!("rpc request failed endpoint={endpoint}"))?;
        let body: Value = response
            .json()
            .with_context(|| format!("invalid rpc json endpoint={endpoint}"))?;
        parse_confirmation_from_rpc_body(&body, now)
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
            detail: "signature_not_found_yet".to_string(),
        });
    };

    if status_row.is_null() {
        return Ok(ConfirmationResult {
            status: ConfirmationStatus::Timeout,
            confirmed_at: None,
            detail: "signature_pending".to_string(),
        });
    }

    if let Some(err_payload) = status_row.get("err") {
        if !err_payload.is_null() {
            return Ok(ConfirmationResult {
                status: ConfirmationStatus::Failed,
                confirmed_at: None,
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
            detail: format!("signature_{}", confirmation_status),
        });
    }

    Ok(ConfirmationResult {
        status: ConfirmationStatus::Timeout,
        confirmed_at: None,
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
        Ok(())
    }
}
