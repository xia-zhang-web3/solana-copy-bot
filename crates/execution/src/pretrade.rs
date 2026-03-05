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
    allow_missing_token_account_for_buy: bool,
    max_priority_fee_micro_lamports: Option<u64>,
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
        allow_missing_token_account_for_buy: bool,
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
            allow_missing_token_account_for_buy,
            max_priority_fee_micro_lamports: if pretrade_max_priority_fee_lamports == 0 {
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

    fn query_recent_priority_fee_micro_lamports(&self, endpoint: &str) -> Result<u64> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getRecentPrioritizationFees",
            "params": [[]]
        });
        let body = self.post_rpc(endpoint, &payload)?;
        parse_recent_priority_fee_micro_lamports_from_rpc_body(&body)
    }

    fn evaluate_balance(
        &self,
        intent: &ExecutionIntent,
        endpoint: &str,
        blockhash: &str,
        balance_lamports: u64,
    ) -> Result<PreTradeDecision> {
        let required_sol = self.required_sol_for_side(intent);
        let required_lamports = sol_to_lamports(required_sol)?;
        if balance_lamports < required_lamports {
            return Ok(PreTradeDecision::reject(
                "pretrade_balance_insufficient",
                format!(
                    "signer_pubkey={} endpoint={} side={} balance_sol={:.6} required_sol={:.6} reserve_sol={:.6}",
                    self.execution_signer_pubkey,
                    endpoint,
                    intent.side.as_str(),
                    lamports_to_sol(balance_lamports),
                    lamports_to_sol(required_lamports),
                    self.min_sol_reserve
                ),
            ));
        }
        Ok(PreTradeDecision::allow(format!(
            "rpc_pretrade_ok endpoint={} blockhash={} side={} balance_sol={:.6}",
            endpoint,
            short_hash(blockhash),
            intent.side.as_str(),
            lamports_to_sol(balance_lamports)
        )))
    }

    fn required_sol_for_side(&self, intent: &ExecutionIntent) -> f64 {
        match intent.side {
            crate::intent::ExecutionSide::Buy => intent.notional_sol + self.min_sol_reserve,
            crate::intent::ExecutionSide::Sell => self.min_sol_reserve,
        }
    }

    fn evaluate_token_account_policy(
        &self,
        intent: &ExecutionIntent,
        endpoint: &str,
        token_account_exists: bool,
        allow_detail: &mut String,
    ) -> Option<PreTradeDecision> {
        if !self.require_token_account {
            return None;
        }
        if token_account_exists {
            allow_detail.push_str(" token_account=present");
            return None;
        }
        if self.allow_missing_token_account_for_buy
            && matches!(intent.side, crate::intent::ExecutionSide::Buy)
        {
            allow_detail.push_str(" token_account=create_on_submit");
            return None;
        }

        Some(PreTradeDecision::reject(
            "pretrade_token_account_missing",
            format!(
                "signer_pubkey={} endpoint={} token_mint={}",
                self.execution_signer_pubkey, endpoint, intent.token
            ),
        ))
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
                if let Some(decision) = self.evaluate_token_account_policy(
                    intent,
                    endpoint,
                    token_account_exists,
                    &mut allow_detail,
                ) {
                    return Ok(decision);
                }
            }

            if let Some(max_priority_fee_micro_lamports) = self.max_priority_fee_micro_lamports {
                let recent_priority_fee_micro_lamports =
                    match self.query_recent_priority_fee_micro_lamports(endpoint) {
                        Ok(value) => value,
                        Err(error) => {
                            last_error = Some(error);
                            continue;
                        }
                    };
                if recent_priority_fee_micro_lamports > max_priority_fee_micro_lamports {
                    return Ok(PreTradeDecision::retryable(
                        "pretrade_priority_fee_too_high",
                        format!(
                            "endpoint={} recent_priority_fee_micro_lamports_per_cu={} max_priority_fee_micro_lamports_per_cu={}",
                            endpoint, recent_priority_fee_micro_lamports, max_priority_fee_micro_lamports
                        ),
                    ));
                }
                allow_detail.push_str(&format!(
                    " priority_fee_micro_lamports_per_cu={}",
                    recent_priority_fee_micro_lamports
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

fn parse_recent_priority_fee_micro_lamports_from_rpc_body(body: &Value) -> Result<u64> {
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
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread::{self, JoinHandle};
    use std::time::{Duration as StdDuration, Instant};

    fn make_intent(side: ExecutionSide, notional_sol: f64) -> ExecutionIntent {
        ExecutionIntent {
            signal_id: "shadow:test:wallet:buy:token".to_string(),
            leader_wallet: "leader-wallet".to_string(),
            side,
            token: "token-a".to_string(),
            notional_sol,
            signal_ts: Utc::now(),
        }
    }

    fn spawn_pretrade_rpc_server(responses: Vec<String>) -> Option<(String, JoinHandle<()>)> {
        let listener = match TcpListener::bind("127.0.0.1:0") {
            Ok(value) => value,
            Err(error) => {
                eprintln!(
                    "skipping pretrade RPC integration test: failed to bind 127.0.0.1:0: {}",
                    error
                );
                return None;
            }
        };
        listener
            .set_nonblocking(true)
            .expect("set pretrade rpc server nonblocking");
        let addr = listener.local_addr().expect("pretrade rpc server addr");
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
                            .expect("write pretrade rpc response");
                        served += 1;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            Instant::now() < deadline,
                            "pretrade rpc test server timed out after serving {} of {} responses",
                            served,
                            responses.len()
                        );
                        thread::sleep(StdDuration::from_millis(10));
                    }
                    Err(error) => panic!("pretrade rpc test server accept failed: {error}"),
                }
            }
        });
        Some((format!("http://{}", addr), handle))
    }

    #[test]
    fn paper_pretrade_rejects_empty_route() -> Result<()> {
        let checker = PaperPreTradeChecker;
        let decision = checker.check(&make_intent(ExecutionSide::Buy, 0.1), "")?;
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
    fn rpc_pretrade_evaluate_balance_rejects_buy_when_insufficient() -> Result<()> {
        let checker = RpcPreTradeChecker::new(
            "https://rpc.primary.example",
            "",
            1_000,
            "11111111111111111111111111111111",
            0.05,
            false,
            false,
            0,
        )
        .expect("checker should initialize");

        let decision = checker.evaluate_balance(
            &make_intent(ExecutionSide::Buy, 0.1),
            "https://rpc.primary.example",
            "3Q3swfYxFxYt5m4T9f2xZ2JKeQX2DAX7T5q6YQnM7n8p",
            100_000_000,
        )?;
        assert_eq!(decision.kind, PreTradeDecisionKind::TerminalReject);
        assert_eq!(decision.reason_code, "pretrade_balance_insufficient");
        Ok(())
    }

    #[test]
    fn rpc_pretrade_evaluate_balance_allows_sell_when_reserve_present() -> Result<()> {
        let checker = RpcPreTradeChecker::new(
            "https://rpc.primary.example",
            "",
            1_000,
            "11111111111111111111111111111111",
            0.05,
            false,
            false,
            0,
        )
        .expect("checker should initialize");

        let decision = checker.evaluate_balance(
            &make_intent(ExecutionSide::Sell, 0.9),
            "https://rpc.primary.example",
            "3Q3swfYxFxYt5m4T9f2xZ2JKeQX2DAX7T5q6YQnM7n8p",
            60_000_000,
        )?;
        assert_eq!(decision.kind, PreTradeDecisionKind::Allow);
        Ok(())
    }

    #[test]
    fn rpc_pretrade_evaluate_balance_rejects_sell_without_reserve() -> Result<()> {
        let checker = RpcPreTradeChecker::new(
            "https://rpc.primary.example",
            "",
            1_000,
            "11111111111111111111111111111111",
            0.05,
            false,
            false,
            0,
        )
        .expect("checker should initialize");

        let decision = checker.evaluate_balance(
            &make_intent(ExecutionSide::Sell, 0.9),
            "https://rpc.primary.example",
            "3Q3swfYxFxYt5m4T9f2xZ2JKeQX2DAX7T5q6YQnM7n8p",
            1_000_000,
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
    fn rpc_pretrade_token_account_policy_allows_buy_when_submit_can_create_ata() -> Result<()> {
        let checker = RpcPreTradeChecker::new(
            "https://rpc.primary.example",
            "",
            1_000,
            "11111111111111111111111111111111",
            0.05,
            true,
            true,
            0,
        )
        .expect("checker should initialize");
        let mut allow_detail = "rpc_pretrade_ok".to_string();

        let decision = checker.evaluate_token_account_policy(
            &make_intent(ExecutionSide::Buy, 0.1),
            "https://rpc.primary.example",
            false,
            &mut allow_detail,
        );

        assert!(
            decision.is_none(),
            "buy should be allowed via ATA create path"
        );
        assert!(
            allow_detail.contains("token_account=create_on_submit"),
            "unexpected allow detail: {}",
            allow_detail
        );
        Ok(())
    }

    #[test]
    fn rpc_pretrade_token_account_policy_rejects_sell_when_token_account_missing() -> Result<()> {
        let checker = RpcPreTradeChecker::new(
            "https://rpc.primary.example",
            "",
            1_000,
            "11111111111111111111111111111111",
            0.05,
            true,
            true,
            0,
        )
        .expect("checker should initialize");
        let mut allow_detail = "rpc_pretrade_ok".to_string();

        let decision = checker
            .evaluate_token_account_policy(
                &make_intent(ExecutionSide::Sell, 0.1),
                "https://rpc.primary.example",
                false,
                &mut allow_detail,
            )
            .expect("sell should reject when token account is missing");

        assert_eq!(decision.kind, PreTradeDecisionKind::TerminalReject);
        assert_eq!(decision.reason_code, "pretrade_token_account_missing");
        Ok(())
    }

    #[test]
    fn rpc_pretrade_token_account_policy_rejects_buy_when_create_path_disabled() -> Result<()> {
        let checker = RpcPreTradeChecker::new(
            "https://rpc.primary.example",
            "",
            1_000,
            "11111111111111111111111111111111",
            0.05,
            true,
            false,
            0,
        )
        .expect("checker should initialize");
        let mut allow_detail = "rpc_pretrade_ok".to_string();

        let decision = checker
            .evaluate_token_account_policy(
                &make_intent(ExecutionSide::Buy, 0.1),
                "https://rpc.primary.example",
                false,
                &mut allow_detail,
            )
            .expect("buy should reject when create path is disabled");

        assert_eq!(decision.kind, PreTradeDecisionKind::TerminalReject);
        assert_eq!(decision.reason_code, "pretrade_token_account_missing");
        Ok(())
    }

    #[test]
    fn rpc_pretrade_token_account_policy_marks_present_account_in_allow_detail() -> Result<()> {
        let checker = RpcPreTradeChecker::new(
            "https://rpc.primary.example",
            "",
            1_000,
            "11111111111111111111111111111111",
            0.05,
            true,
            false,
            0,
        )
        .expect("checker should initialize");
        let mut allow_detail = "rpc_pretrade_ok".to_string();

        let decision = checker.evaluate_token_account_policy(
            &make_intent(ExecutionSide::Buy, 0.1),
            "https://rpc.primary.example",
            true,
            &mut allow_detail,
        );

        assert!(decision.is_none(), "present token account should allow");
        assert!(
            allow_detail.contains("token_account=present"),
            "unexpected allow detail: {}",
            allow_detail
        );
        Ok(())
    }

    #[test]
    fn rpc_pretrade_check_allows_missing_buy_token_account_when_submit_can_create_ata() -> Result<()>
    {
        let Some((endpoint, handle)) = spawn_pretrade_rpc_server(vec![
            json!({
                "jsonrpc": "2.0",
                "result": {
                    "value": {
                        "blockhash": "3Q3swfYxFxYt5m4T9f2xZ2JKeQX2DAX7T5q6YQnM7n8p",
                        "lastValidBlockHeight": 100
                    }
                },
                "id": 1
            })
            .to_string(),
            json!({
                "jsonrpc": "2.0",
                "result": { "value": 2_000_000_000_u64 },
                "id": 1
            })
            .to_string(),
            json!({
                "jsonrpc": "2.0",
                "result": { "value": [] },
                "id": 1
            })
            .to_string(),
        ]) else {
            return Ok(());
        };

        let checker = RpcPreTradeChecker::new(
            &endpoint,
            "",
            1_000,
            "11111111111111111111111111111111",
            0.05,
            true,
            true,
            0,
        )
        .expect("checker should initialize");

        let decision = checker.check(&make_intent(ExecutionSide::Buy, 0.1), "rpc")?;
        assert_eq!(decision.kind, PreTradeDecisionKind::Allow);
        assert!(
            decision.detail.contains("token_account=create_on_submit"),
            "unexpected allow detail: {}",
            decision.detail
        );

        handle.join().expect("join pretrade rpc test server");
        Ok(())
    }

    #[test]
    fn parse_recent_priority_fee_micro_lamports_from_rpc_body_returns_max_value() -> Result<()> {
        let body = json!({
            "jsonrpc": "2.0",
            "result": [
                { "slot": 1, "prioritizationFee": 3000 },
                { "slot": 2, "prioritizationFee": 1200 },
                { "slot": 3, "prioritizationFee": 5000 }
            ],
            "id": 1
        });
        let fee = parse_recent_priority_fee_micro_lamports_from_rpc_body(&body)?;
        assert_eq!(fee, 5_000);
        Ok(())
    }
}
