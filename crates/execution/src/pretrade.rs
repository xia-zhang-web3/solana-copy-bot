use anyhow::{anyhow, Context, Result};
use reqwest::blocking::Client;
use serde_json::{json, Value};
use std::time::Duration as StdDuration;

use crate::intent::ExecutionIntent;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreTradeDecisionKind {
    Allow,
    RetryableReject,
    TerminalReject,
}

#[derive(Debug, Clone)]
pub struct PreTradeDecision {
    pub kind: PreTradeDecisionKind,
    pub reason_code: String,
    pub detail: String,
}

impl PreTradeDecision {
    pub fn allow(detail: impl Into<String>) -> Self {
        Self {
            kind: PreTradeDecisionKind::Allow,
            reason_code: "ok".to_string(),
            detail: detail.into(),
        }
    }

    pub fn retryable(reason_code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            kind: PreTradeDecisionKind::RetryableReject,
            reason_code: reason_code.into(),
            detail: detail.into(),
        }
    }

    pub fn reject(reason_code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            kind: PreTradeDecisionKind::TerminalReject,
            reason_code: reason_code.into(),
            detail: detail.into(),
        }
    }
}

pub trait PreTradeChecker {
    fn check(&self, intent: &ExecutionIntent, route: &str) -> Result<PreTradeDecision>;
}

fn pretrade_contract_sanity(intent: &ExecutionIntent, route: &str) -> Option<PreTradeDecision> {
    if route.trim().is_empty() {
        return Some(PreTradeDecision::reject(
            "route_missing",
            "execution route is empty",
        ));
    }
    if intent.token.trim().is_empty() {
        return Some(PreTradeDecision::reject(
            "token_missing",
            "execution intent token is empty",
        ));
    }
    if !intent.notional_sol.is_finite() || intent.notional_sol <= 0.0 {
        return Some(PreTradeDecision::reject(
            "invalid_notional",
            "execution intent has invalid notional",
        ));
    }
    None
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PaperPreTradeChecker;

impl PreTradeChecker for PaperPreTradeChecker {
    fn check(&self, intent: &ExecutionIntent, route: &str) -> Result<PreTradeDecision> {
        if let Some(decision) = pretrade_contract_sanity(intent, route) {
            return Ok(decision);
        }

        // Paper-mode contract placeholder for future live checks:
        // balance, ATA existence/create policy, blockhash freshness, compute-budget limits.
        Ok(PreTradeDecision::allow("paper_pretrade_ok"))
    }
}

#[derive(Debug, Clone)]
pub struct FailClosedPreTradeChecker {
    reason_code: String,
    detail: String,
}

impl FailClosedPreTradeChecker {
    pub fn new(reason_code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            reason_code: reason_code.into(),
            detail: detail.into(),
        }
    }
}

impl PreTradeChecker for FailClosedPreTradeChecker {
    fn check(&self, _intent: &ExecutionIntent, _route: &str) -> Result<PreTradeDecision> {
        Ok(PreTradeDecision::retryable(
            self.reason_code.clone(),
            self.detail.clone(),
        ))
    }
}

#[derive(Debug, Clone)]
pub struct RpcPreTradeChecker {
    endpoints: Vec<String>,
    execution_signer_pubkey: String,
    min_sol_reserve: f64,
    require_token_account: bool,
    max_priority_fee_lamports: Option<u64>,
    client: Client,
}

impl RpcPreTradeChecker {
    pub fn new(
        primary_url: &str,
        fallback_url: &str,
        timeout_ms: u64,
        execution_signer_pubkey: &str,
        min_sol_reserve: f64,
        require_token_account: bool,
        pretrade_max_priority_fee_lamports: u64,
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
        let signer = execution_signer_pubkey.trim();
        if endpoints.is_empty() || signer.is_empty() {
            return None;
        }
        if !min_sol_reserve.is_finite() || min_sol_reserve < 0.0 {
            return None;
        }

        let timeout = StdDuration::from_millis(timeout_ms.max(500));
        let client = match Client::builder().timeout(timeout).build() {
            Ok(value) => value,
            Err(_) => return None,
        };

        Some(Self {
            endpoints,
            execution_signer_pubkey: signer.to_string(),
            min_sol_reserve,
            require_token_account,
            max_priority_fee_lamports: if pretrade_max_priority_fee_lamports == 0 {
                None
            } else {
                Some(pretrade_max_priority_fee_lamports)
            },
            client,
        })
    }

    fn post_rpc(&self, endpoint: &str, payload: &Value) -> Result<Value> {
        let response = self
            .client
            .post(endpoint)
            .json(payload)
            .send()
            .with_context(|| format!("rpc request failed endpoint={endpoint}"))?;
        let status = response.status();
        if !status.is_success() {
            return Err(anyhow!(
                "rpc http status={} endpoint={endpoint}",
                status.as_u16()
            ));
        }
        response
            .json()
            .with_context(|| format!("invalid rpc json endpoint={endpoint}"))
    }

    fn query_latest_blockhash(&self, endpoint: &str) -> Result<String> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getLatestBlockhash",
            "params": [{ "commitment": "processed" }]
        });
        let body = self.post_rpc(endpoint, &payload)?;
        parse_latest_blockhash_from_rpc_body(&body)
    }

    fn query_balance_lamports(&self, endpoint: &str) -> Result<u64> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBalance",
            "params": [self.execution_signer_pubkey, { "commitment": "processed" }]
        });
        let body = self.post_rpc(endpoint, &payload)?;
        parse_balance_lamports_from_rpc_body(&body)
    }

    fn query_token_account_exists(&self, endpoint: &str, mint: &str) -> Result<bool> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenAccountsByOwner",
            "params": [
                self.execution_signer_pubkey,
                { "mint": mint },
                { "encoding": "base64", "commitment": "processed" }
            ]
        });
        let body = self.post_rpc(endpoint, &payload)?;
        parse_token_account_exists_from_rpc_body(&body)
    }

    fn query_recent_priority_fee_lamports(&self, endpoint: &str) -> Result<u64> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getRecentPrioritizationFees",
            "params": [[]]
        });
        let body = self.post_rpc(endpoint, &payload)?;
        parse_recent_priority_fee_lamports_from_rpc_body(&body)
    }

    fn evaluate_balance(
        &self,
        intent: &ExecutionIntent,
        endpoint: &str,
        blockhash: &str,
        balance_lamports: u64,
    ) -> Result<PreTradeDecision> {
        let required_sol = intent.notional_sol + self.min_sol_reserve;
        let required_lamports = sol_to_lamports(required_sol)?;
        if balance_lamports < required_lamports {
            return Ok(PreTradeDecision::reject(
                "pretrade_balance_insufficient",
                format!(
                    "signer_pubkey={} endpoint={} balance_sol={:.6} required_sol={:.6} reserve_sol={:.6}",
                    self.execution_signer_pubkey,
                    endpoint,
                    lamports_to_sol(balance_lamports),
                    lamports_to_sol(required_lamports),
                    self.min_sol_reserve
                ),
            ));
        }
        Ok(PreTradeDecision::allow(format!(
            "rpc_pretrade_ok endpoint={} blockhash={} balance_sol={:.6}",
            endpoint,
            short_hash(blockhash),
            lamports_to_sol(balance_lamports)
        )))
    }
}

impl PreTradeChecker for RpcPreTradeChecker {
    fn check(&self, intent: &ExecutionIntent, route: &str) -> Result<PreTradeDecision> {
        if let Some(decision) = pretrade_contract_sanity(intent, route) {
            return Ok(decision);
        }

        let mut last_error: Option<anyhow::Error> = None;
        for endpoint in &self.endpoints {
            let blockhash = match self.query_latest_blockhash(endpoint) {
                Ok(value) => value,
                Err(error) => {
                    last_error = Some(error);
                    continue;
                }
            };
            let balance_lamports = match self.query_balance_lamports(endpoint) {
                Ok(value) => value,
                Err(error) => {
                    last_error = Some(error);
                    continue;
                }
            };
            let balance_decision =
                match self.evaluate_balance(intent, endpoint, &blockhash, balance_lamports) {
                    Ok(value) => value,
                    Err(error) => {
                        last_error = Some(error);
                        continue;
                    }
                };
            if balance_decision.kind != PreTradeDecisionKind::Allow {
                return Ok(balance_decision);
            }

            let mut allow_detail = balance_decision.detail;

            if self.require_token_account {
                let token_account_exists =
                    match self.query_token_account_exists(endpoint, intent.token.trim()) {
                        Ok(value) => value,
                        Err(error) => {
                            last_error = Some(error);
                            continue;
                        }
                    };
                if !token_account_exists {
                    return Ok(PreTradeDecision::reject(
                        "pretrade_token_account_missing",
                        format!(
                            "signer_pubkey={} endpoint={} token_mint={}",
                            self.execution_signer_pubkey, endpoint, intent.token
                        ),
                    ));
                }
                allow_detail.push_str(" token_account=present");
            }

            if let Some(max_priority_fee_lamports) = self.max_priority_fee_lamports {
                let recent_priority_fee_lamports =
                    match self.query_recent_priority_fee_lamports(endpoint) {
                        Ok(value) => value,
                        Err(error) => {
                            last_error = Some(error);
                            continue;
                        }
                    };
                if recent_priority_fee_lamports > max_priority_fee_lamports {
                    return Ok(PreTradeDecision::retryable(
                        "pretrade_priority_fee_too_high",
                        format!(
                            "endpoint={} recent_priority_fee_lamports={} max_priority_fee_lamports={}",
                            endpoint, recent_priority_fee_lamports, max_priority_fee_lamports
                        ),
                    ));
                }
                allow_detail.push_str(&format!(
                    " priority_fee_lamports={}",
                    recent_priority_fee_lamports
                ));
            }

            return Ok(PreTradeDecision::allow(allow_detail));
        }

        Ok(PreTradeDecision::retryable(
            "pretrade_rpc_unavailable",
            last_error
                .map(|error| error.to_string())
                .unwrap_or_else(|| "all pre-trade rpc endpoints unavailable".to_string()),
        ))
    }
}

fn parse_latest_blockhash_from_rpc_body(body: &Value) -> Result<String> {
    if let Some(error_payload) = body.get("error") {
        return Err(anyhow!("rpc returned error payload: {}", error_payload));
    }
    let blockhash = body
        .get("result")
        .and_then(|result| result.get("value"))
        .and_then(|value| value.get("blockhash"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("missing result.value.blockhash"))?;
    Ok(blockhash.to_string())
}

fn parse_balance_lamports_from_rpc_body(body: &Value) -> Result<u64> {
    if let Some(error_payload) = body.get("error") {
        return Err(anyhow!("rpc returned error payload: {}", error_payload));
    }
    body.get("result")
        .and_then(|result| result.get("value"))
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("missing result.value lamports"))
}

fn parse_token_account_exists_from_rpc_body(body: &Value) -> Result<bool> {
    if let Some(error_payload) = body.get("error") {
        return Err(anyhow!("rpc returned error payload: {}", error_payload));
    }
    let value = body
        .get("result")
        .and_then(|result| result.get("value"))
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("missing result.value token accounts array"))?;
    Ok(!value.is_empty())
}

fn parse_recent_priority_fee_lamports_from_rpc_body(body: &Value) -> Result<u64> {
    if let Some(error_payload) = body.get("error") {
        return Err(anyhow!("rpc returned error payload: {}", error_payload));
    }
    let values = body
        .get("result")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("missing result prioritization fee array"))?;
    let max_fee = values
        .iter()
        .filter_map(|entry| entry.get("prioritizationFee"))
        .filter_map(Value::as_u64)
        .max()
        .ok_or_else(|| anyhow!("missing prioritizationFee values"))?;
    Ok(max_fee)
}

fn sol_to_lamports(sol: f64) -> Result<u64> {
    if !sol.is_finite() || sol < 0.0 {
        return Err(anyhow!("invalid sol amount: {sol}"));
    }
    let lamports = (sol * 1_000_000_000.0).ceil();
    if lamports > u64::MAX as f64 {
        return Err(anyhow!("sol amount overflow: {sol}"));
    }
    Ok(lamports as u64)
}

fn lamports_to_sol(lamports: u64) -> f64 {
    (lamports as f64) / 1_000_000_000.0
}

fn short_hash(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.len() <= 16 {
        return trimmed.to_string();
    }
    format!("{}...{}", &trimmed[..8], &trimmed[trimmed.len() - 8..])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent::ExecutionSide;
    use chrono::Utc;

    fn make_intent(notional_sol: f64) -> ExecutionIntent {
        ExecutionIntent {
            signal_id: "shadow:test:wallet:buy:token".to_string(),
            leader_wallet: "leader-wallet".to_string(),
            side: ExecutionSide::Buy,
            token: "token-a".to_string(),
            notional_sol,
            signal_ts: Utc::now(),
        }
    }

    #[test]
    fn paper_pretrade_rejects_empty_route() -> Result<()> {
        let checker = PaperPreTradeChecker;
        let decision = checker.check(&make_intent(0.1), "")?;
        assert_eq!(decision.kind, PreTradeDecisionKind::TerminalReject);
        assert_eq!(decision.reason_code, "route_missing");
        Ok(())
    }

    #[test]
    fn parse_latest_blockhash_from_rpc_body_returns_blockhash() -> Result<()> {
        let body = json!({
            "jsonrpc": "2.0",
            "result": {
                "context": {"slot": 1},
                "value": {
                    "blockhash": "3Q3swfYxFxYt5m4T9f2xZ2JKeQX2DAX7T5q6YQnM7n8p",
                    "lastValidBlockHeight": 100
                }
            },
            "id": 1
        });
        let blockhash = parse_latest_blockhash_from_rpc_body(&body)?;
        assert_eq!(blockhash, "3Q3swfYxFxYt5m4T9f2xZ2JKeQX2DAX7T5q6YQnM7n8p");
        Ok(())
    }

    #[test]
    fn parse_latest_blockhash_from_rpc_body_errors_on_error_payload() {
        let body = json!({
            "jsonrpc": "2.0",
            "error": {
                "code": -32000,
                "message": "node is unhealthy"
            },
            "id": 1
        });
        assert!(parse_latest_blockhash_from_rpc_body(&body).is_err());
    }

    #[test]
    fn parse_balance_lamports_from_rpc_body_returns_value() -> Result<()> {
        let body = json!({
            "jsonrpc": "2.0",
            "result": {
                "context": {"slot": 1},
                "value": 1250000000
            },
            "id": 1
        });
        let balance = parse_balance_lamports_from_rpc_body(&body)?;
        assert_eq!(balance, 1_250_000_000);
        Ok(())
    }

    #[test]
    fn rpc_pretrade_evaluate_balance_rejects_when_insufficient() -> Result<()> {
        let checker = RpcPreTradeChecker::new(
            "https://rpc.primary.example",
            "",
            1_000,
            "11111111111111111111111111111111",
            0.05,
            false,
            0,
        )
        .expect("checker should initialize");

        let decision = checker.evaluate_balance(
            &make_intent(0.1),
            "https://rpc.primary.example",
            "3Q3swfYxFxYt5m4T9f2xZ2JKeQX2DAX7T5q6YQnM7n8p",
            100_000_000,
        )?;
        assert_eq!(decision.kind, PreTradeDecisionKind::TerminalReject);
        assert_eq!(decision.reason_code, "pretrade_balance_insufficient");
        Ok(())
    }

    #[test]
    fn parse_token_account_exists_from_rpc_body_true_when_value_present() -> Result<()> {
        let body = json!({
            "jsonrpc": "2.0",
            "result": {
                "context": { "slot": 1 },
                "value": [
                    { "pubkey": "token-account-1", "account": { "data": ["", "base64"] } }
                ]
            },
            "id": 1
        });
        let exists = parse_token_account_exists_from_rpc_body(&body)?;
        assert!(exists);
        Ok(())
    }

    #[test]
    fn parse_token_account_exists_from_rpc_body_false_when_value_empty() -> Result<()> {
        let body = json!({
            "jsonrpc": "2.0",
            "result": {
                "context": { "slot": 1 },
                "value": []
            },
            "id": 1
        });
        let exists = parse_token_account_exists_from_rpc_body(&body)?;
        assert!(!exists);
        Ok(())
    }

    #[test]
    fn parse_recent_priority_fee_lamports_from_rpc_body_returns_max_value() -> Result<()> {
        let body = json!({
            "jsonrpc": "2.0",
            "result": [
                { "slot": 1, "prioritizationFee": 3000 },
                { "slot": 2, "prioritizationFee": 1200 },
                { "slot": 3, "prioritizationFee": 5000 }
            ],
            "id": 1
        });
        let fee = parse_recent_priority_fee_lamports_from_rpc_body(&body)?;
        assert_eq!(fee, 5_000);
        Ok(())
    }
}
