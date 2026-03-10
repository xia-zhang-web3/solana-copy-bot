use anyhow::{anyhow, Context, Result};
use copybot_core_types::Lamports;
use reqwest::blocking::Client;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::time::Duration as StdDuration;

use crate::intent::ExecutionIntent;
use crate::money::{checked_add_lamports, lamports_to_sol, sol_to_lamports_ceil};
use crate::submitter::{
    dynamic_tip_lamports_from_compute_budget, priority_fee_lamports_from_compute_budget,
};

const DEFAULT_BASE_FEE_LAMPORTS: u64 = 5_000;
const ESTIMATED_ATA_CREATE_RENT_LAMPORTS: u64 = 2_039_280;

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
    if intent.notional_lamports().is_err() {
        return Some(PreTradeDecision::reject(
            "invalid_notional",
            "execution intent notional is not representable as lamports",
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
    min_sol_reserve_lamports: Lamports,
    max_fee_overhead_bps: Option<u32>,
    require_token_account: bool,
    allow_missing_token_account_for_buy: bool,
    max_priority_fee_micro_lamports: Option<u64>,
    submit_dynamic_cu_price_enabled: bool,
    submit_dynamic_tip_lamports_enabled: bool,
    submit_dynamic_tip_lamports_multiplier_bps: u32,
    route_tip_lamports: HashMap<String, u64>,
    route_compute_unit_limit: HashMap<String, u32>,
    route_compute_unit_price_micro_lamports: HashMap<String, u64>,
    client: Client,
}

impl RpcPreTradeChecker {
    pub fn new(
        primary_url: &str,
        fallback_url: &str,
        timeout_ms: u64,
        execution_signer_pubkey: &str,
        min_sol_reserve: f64,
        pretrade_max_fee_overhead_bps: u32,
        require_token_account: bool,
        allow_missing_token_account_for_buy: bool,
        pretrade_max_priority_fee_lamports: u64,
        route_tip_lamports: &BTreeMap<String, u64>,
        route_compute_unit_limit: &BTreeMap<String, u32>,
        route_compute_unit_price_micro_lamports: &BTreeMap<String, u64>,
        submit_dynamic_cu_price_enabled: bool,
        submit_dynamic_tip_lamports_enabled: bool,
        submit_dynamic_tip_lamports_multiplier_bps: u32,
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
        let min_sol_reserve_lamports =
            match sol_to_lamports_ceil(min_sol_reserve, "execution.pretrade_min_sol_reserve") {
                Ok(value) => value,
                Err(_) => return None,
            };
        let max_fee_overhead_bps = if pretrade_max_fee_overhead_bps == 0 {
            None
        } else {
            Some(pretrade_max_fee_overhead_bps)
        };
        let route_tip_lamports = normalize_route_u64_policy(route_tip_lamports);
        let route_compute_unit_limit = normalize_route_u32_policy(route_compute_unit_limit);
        let route_compute_unit_price_micro_lamports =
            normalize_route_u64_policy(route_compute_unit_price_micro_lamports);
        if max_fee_overhead_bps.is_some()
            && (route_tip_lamports.is_empty()
                || route_compute_unit_limit.is_empty()
                || route_compute_unit_price_micro_lamports.is_empty())
        {
            return None;
        }
        if submit_dynamic_tip_lamports_enabled
            && (submit_dynamic_tip_lamports_multiplier_bps == 0
                || submit_dynamic_tip_lamports_multiplier_bps > 100_000)
        {
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
            min_sol_reserve_lamports,
            max_fee_overhead_bps,
            require_token_account,
            allow_missing_token_account_for_buy,
            max_priority_fee_micro_lamports: if pretrade_max_priority_fee_lamports == 0 {
                None
            } else {
                Some(pretrade_max_priority_fee_lamports)
            },
            submit_dynamic_cu_price_enabled,
            submit_dynamic_tip_lamports_enabled,
            submit_dynamic_tip_lamports_multiplier_bps,
            route_tip_lamports,
            route_compute_unit_limit,
            route_compute_unit_price_micro_lamports,
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
        let required_lamports = self.required_lamports_for_side(intent)?;
        if balance_lamports < required_lamports.as_u64() {
            return Ok(PreTradeDecision::reject(
                "pretrade_balance_insufficient",
                format!(
                    "signer_pubkey={} endpoint={} side={} balance_sol={:.6} required_sol={:.6} reserve_sol={:.6}",
                    self.execution_signer_pubkey,
                    endpoint,
                    intent.side.as_str(),
                    lamports_to_sol(Lamports::new(balance_lamports)),
                    lamports_to_sol(required_lamports),
                    lamports_to_sol(self.min_sol_reserve_lamports)
                ),
            ));
        }
        Ok(PreTradeDecision::allow(format!(
            "rpc_pretrade_ok endpoint={} blockhash={} side={} balance_sol={:.6}",
            endpoint,
            short_hash(blockhash),
            intent.side.as_str(),
            lamports_to_sol(Lamports::new(balance_lamports))
        )))
    }

    fn required_lamports_for_side(&self, intent: &ExecutionIntent) -> Result<Lamports> {
        match intent.side {
            crate::intent::ExecutionSide::Buy => checked_add_lamports(
                intent.notional_lamports()?,
                self.min_sol_reserve_lamports,
                "pretrade buy required lamports",
            ),
            crate::intent::ExecutionSide::Sell => Ok(self.min_sol_reserve_lamports),
        }
    }

    fn needs_token_account_lookup(&self, intent: &ExecutionIntent) -> bool {
        self.require_token_account
            || (self.max_fee_overhead_bps.is_some()
                && matches!(intent.side, crate::intent::ExecutionSide::Buy))
    }

    fn evaluate_fee_overhead_policy(
        &self,
        intent: &ExecutionIntent,
        route: &str,
        token_account_exists: bool,
        allow_detail: &mut String,
    ) -> Option<PreTradeDecision> {
        let Some(max_fee_overhead_bps) = self.max_fee_overhead_bps else {
            return None;
        };
        if !matches!(intent.side, crate::intent::ExecutionSide::Buy) {
            return None;
        }

        let normalized_route = route.trim().to_ascii_lowercase();
        let evaluation = (|| -> Result<Option<PreTradeDecision>> {
            let static_tip_lamports = self
                .route_tip_lamports
                .get(normalized_route.as_str())
                .copied()
                .ok_or_else(|| {
                    anyhow!(
                        "missing route tip policy for pretrade fee overhead route={}",
                        normalized_route
                    )
                })?;
            let route_cu_limit = self
                .route_compute_unit_limit
                .get(normalized_route.as_str())
                .copied()
                .ok_or_else(|| {
                    anyhow!(
                        "missing compute unit limit policy for pretrade fee overhead route={}",
                        normalized_route
                    )
                })?;
            let static_route_cu_price_micro_lamports = self
                .route_compute_unit_price_micro_lamports
                .get(normalized_route.as_str())
                .copied()
                .ok_or_else(|| {
                    anyhow!(
                        "missing compute unit price policy for pretrade fee overhead route={}",
                        normalized_route
                    )
                })?;
            let effective_route_cu_price_micro_lamports = if self.submit_dynamic_cu_price_enabled {
                self.max_priority_fee_micro_lamports
                    .unwrap_or(static_route_cu_price_micro_lamports)
                    .max(static_route_cu_price_micro_lamports)
            } else {
                static_route_cu_price_micro_lamports
            };
            let priority_fee_lamports = priority_fee_lamports_from_compute_budget(
                route_cu_limit,
                effective_route_cu_price_micro_lamports,
            );
            let dynamic_tip_lamports = if self.submit_dynamic_tip_lamports_enabled {
                dynamic_tip_lamports_from_compute_budget(
                    route_cu_limit,
                    effective_route_cu_price_micro_lamports,
                    self.submit_dynamic_tip_lamports_multiplier_bps,
                )
            } else {
                0
            };
            let estimated_tip_lamports = static_tip_lamports.max(dynamic_tip_lamports);
            let estimated_network_fee_lamports = DEFAULT_BASE_FEE_LAMPORTS
                .checked_add(priority_fee_lamports)
                .ok_or_else(|| anyhow!("estimated network fee lamports overflow"))?;
            let estimated_ata_create_rent_lamports = if token_account_exists {
                0
            } else {
                ESTIMATED_ATA_CREATE_RENT_LAMPORTS
            };
            let estimated_total_fee_lamports = estimated_network_fee_lamports
                .checked_add(estimated_tip_lamports)
                .and_then(|value| value.checked_add(estimated_ata_create_rent_lamports))
                .ok_or_else(|| anyhow!("estimated total fee lamports overflow"))?;
            let notional_lamports = intent.notional_lamports()?.as_u64();
            let fee_overhead_bps =
                fee_overhead_bps_ceil(estimated_total_fee_lamports, notional_lamports);
            allow_detail.push_str(&format!(
                " estimated_fee_overhead_bps={} estimated_total_fee_sol={:.6}",
                fee_overhead_bps,
                lamports_to_sol(Lamports::new(estimated_total_fee_lamports))
            ));
            if estimated_ata_create_rent_lamports > 0
                && !allow_detail.contains("token_account=create_on_submit")
            {
                allow_detail.push_str(" token_account=create_on_submit");
            }

            if fee_overhead_exceeds_limit(
                estimated_total_fee_lamports,
                notional_lamports,
                max_fee_overhead_bps,
            ) {
                return Ok(Some(PreTradeDecision::reject(
                    "pretrade_fee_overhead_too_high",
                    format!(
                        "route={} notional_sol={:.6} estimated_total_fee_sol={:.6} estimated_network_fee_sol={:.6} estimated_tip_sol={:.6} estimated_ata_create_rent_sol={:.6} fee_overhead_bps={} max_fee_overhead_bps={}",
                        normalized_route,
                        lamports_to_sol(Lamports::new(notional_lamports)),
                        lamports_to_sol(Lamports::new(estimated_total_fee_lamports)),
                        lamports_to_sol(Lamports::new(estimated_network_fee_lamports)),
                        lamports_to_sol(Lamports::new(estimated_tip_lamports)),
                        lamports_to_sol(Lamports::new(estimated_ata_create_rent_lamports)),
                        fee_overhead_bps,
                        max_fee_overhead_bps
                    ),
                )));
            }

            Ok(None)
        })();

        match evaluation {
            Ok(value) => value,
            Err(error) => Some(PreTradeDecision::reject(
                "pretrade_fee_policy_invalid",
                error.to_string(),
            )),
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

fn normalize_route_u64_policy(values: &BTreeMap<String, u64>) -> HashMap<String, u64> {
    values
        .iter()
        .filter_map(|(route, value)| {
            let normalized = route.trim().to_ascii_lowercase();
            if normalized.is_empty() {
                None
            } else {
                Some((normalized, *value))
            }
        })
        .collect()
}

fn normalize_route_u32_policy(values: &BTreeMap<String, u32>) -> HashMap<String, u32> {
    values
        .iter()
        .filter_map(|(route, value)| {
            let normalized = route.trim().to_ascii_lowercase();
            if normalized.is_empty() {
                None
            } else {
                Some((normalized, *value))
            }
        })
        .collect()
}

fn fee_overhead_exceeds_limit(
    estimated_total_fee_lamports: u64,
    notional_lamports: u64,
    max_fee_overhead_bps: u32,
) -> bool {
    (estimated_total_fee_lamports as u128).saturating_mul(10_000u128)
        > (notional_lamports as u128).saturating_mul(max_fee_overhead_bps as u128)
}

fn fee_overhead_bps_ceil(estimated_total_fee_lamports: u64, notional_lamports: u64) -> u64 {
    if notional_lamports == 0 {
        return u64::MAX;
    }
    let numerator = (estimated_total_fee_lamports as u128)
        .saturating_mul(10_000u128)
        .saturating_add((notional_lamports as u128).saturating_sub(1));
    let value = numerator / (notional_lamports as u128);
    value.min(u64::MAX as u128).try_into().unwrap_or(u64::MAX)
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

            if self.needs_token_account_lookup(intent) {
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
                if let Some(decision) = self.evaluate_fee_overhead_policy(
                    intent,
                    route,
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
            notional_lamports: super::sol_to_lamports_ceil(
                notional_sol,
                "test execution intent notional_sol",
            )
            .expect("test notional should convert to lamports"),
            signal_ts: Utc::now(),
        }
    }

    fn route_tip_lamports(values: &[(&str, u64)]) -> BTreeMap<String, u64> {
        values
            .iter()
            .map(|(route, value)| ((*route).to_string(), *value))
            .collect()
    }

    fn route_cu_limits(values: &[(&str, u32)]) -> BTreeMap<String, u32> {
        values
            .iter()
            .map(|(route, value)| ((*route).to_string(), *value))
            .collect()
    }

    fn route_cu_prices(values: &[(&str, u64)]) -> BTreeMap<String, u64> {
        values
            .iter()
            .map(|(route, value)| ((*route).to_string(), *value))
            .collect()
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
            0,
            false,
            false,
            0,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            false,
            false,
            10_000,
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
    fn rpc_pretrade_required_lamports_for_buy_is_exact_and_conservative() -> Result<()> {
        let checker = RpcPreTradeChecker::new(
            "https://rpc.primary.example",
            "",
            1_000,
            "11111111111111111111111111111111",
            0.0000000001,
            0,
            false,
            false,
            0,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            false,
            false,
            10_000,
        )
        .expect("checker should initialize");

        let required =
            checker.required_lamports_for_side(&make_intent(ExecutionSide::Buy, 0.1000000001))?;

        assert_eq!(required.as_u64(), 100_000_002);
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
            0,
            false,
            false,
            0,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            false,
            false,
            10_000,
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
            0,
            false,
            false,
            0,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            false,
            false,
            10_000,
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
            0,
            true,
            true,
            0,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            false,
            false,
            10_000,
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
            0,
            true,
            true,
            0,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            false,
            false,
            10_000,
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
            0,
            true,
            false,
            0,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            false,
            false,
            10_000,
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
            0,
            true,
            false,
            0,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            false,
            false,
            10_000,
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
            0,
            true,
            true,
            0,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            false,
            false,
            10_000,
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
    fn rpc_pretrade_fee_overhead_policy_rejects_buy_when_missing_ata_makes_overhead_too_high(
    ) -> Result<()> {
        let checker = RpcPreTradeChecker::new(
            "https://rpc.primary.example",
            "",
            1_000,
            "11111111111111111111111111111111",
            0.05,
            1_000,
            false,
            true,
            0,
            &route_tip_lamports(&[("rpc", 10_000)]),
            &route_cu_limits(&[("rpc", 300_000)]),
            &route_cu_prices(&[("rpc", 1_500)]),
            false,
            false,
            10_000,
        )
        .expect("checker should initialize");
        let mut allow_detail = "rpc_pretrade_ok".to_string();

        let decision = checker
            .evaluate_fee_overhead_policy(
                &make_intent(ExecutionSide::Buy, 0.01),
                "rpc",
                false,
                &mut allow_detail,
            )
            .expect("buy should reject when ATA overhead dominates notional");

        assert_eq!(decision.kind, PreTradeDecisionKind::TerminalReject);
        assert_eq!(decision.reason_code, "pretrade_fee_overhead_too_high");
        assert!(
            decision
                .detail
                .contains("estimated_ata_create_rent_sol=0.002039"),
            "unexpected detail: {}",
            decision.detail
        );
        Ok(())
    }

    #[test]
    fn rpc_pretrade_fee_overhead_policy_allows_buy_when_fee_fraction_is_within_limit() -> Result<()>
    {
        let checker = RpcPreTradeChecker::new(
            "https://rpc.primary.example",
            "",
            1_000,
            "11111111111111111111111111111111",
            0.05,
            1_000,
            false,
            true,
            0,
            &route_tip_lamports(&[("rpc", 10_000)]),
            &route_cu_limits(&[("rpc", 300_000)]),
            &route_cu_prices(&[("rpc", 1_500)]),
            false,
            false,
            10_000,
        )
        .expect("checker should initialize");
        let mut allow_detail = "rpc_pretrade_ok".to_string();

        let decision = checker.evaluate_fee_overhead_policy(
            &make_intent(ExecutionSide::Buy, 0.1),
            "rpc",
            true,
            &mut allow_detail,
        );

        assert!(
            decision.is_none(),
            "fee fraction should be within configured limit"
        );
        assert!(
            allow_detail.contains("estimated_fee_overhead_bps="),
            "unexpected allow detail: {}",
            allow_detail
        );
        Ok(())
    }

    #[test]
    fn rpc_pretrade_fee_overhead_policy_uses_dynamic_caps_for_conservative_buy_estimate(
    ) -> Result<()> {
        let checker = RpcPreTradeChecker::new(
            "https://rpc.primary.example",
            "",
            1_000,
            "11111111111111111111111111111111",
            0.05,
            2_000,
            false,
            true,
            10_000,
            &route_tip_lamports(&[("jito", 10_000)]),
            &route_cu_limits(&[("jito", 300_000)]),
            &route_cu_prices(&[("jito", 1_000)]),
            true,
            true,
            20_000,
        )
        .expect("checker should initialize");
        let mut allow_detail = "rpc_pretrade_ok".to_string();

        let decision = checker
            .evaluate_fee_overhead_policy(
                &make_intent(ExecutionSide::Buy, 0.00008),
                "jito",
                true,
                &mut allow_detail,
            )
            .expect("dynamic conservative estimate should reject small buy");

        assert_eq!(decision.kind, PreTradeDecisionKind::TerminalReject);
        assert_eq!(decision.reason_code, "pretrade_fee_overhead_too_high");
        assert!(
            decision.detail.contains("estimated_tip_sol=0.000010")
                || decision.detail.contains("estimated_tip_sol=0.000006"),
            "unexpected detail: {}",
            decision.detail
        );
        Ok(())
    }

    #[test]
    fn rpc_pretrade_fee_overhead_policy_does_not_block_sell() -> Result<()> {
        let checker = RpcPreTradeChecker::new(
            "https://rpc.primary.example",
            "",
            1_000,
            "11111111111111111111111111111111",
            0.05,
            1_000,
            false,
            true,
            0,
            &route_tip_lamports(&[("rpc", 10_000)]),
            &route_cu_limits(&[("rpc", 300_000)]),
            &route_cu_prices(&[("rpc", 1_500)]),
            false,
            false,
            10_000,
        )
        .expect("checker should initialize");
        let mut allow_detail = "rpc_pretrade_ok".to_string();

        let decision = checker.evaluate_fee_overhead_policy(
            &make_intent(ExecutionSide::Sell, 0.0005),
            "rpc",
            false,
            &mut allow_detail,
        );

        assert!(
            decision.is_none(),
            "sell path must not be blocked by buy fee overhead guard"
        );
        Ok(())
    }

    #[test]
    fn rpc_pretrade_fee_overhead_policy_rejects_zero_lamport_notional_without_panicking(
    ) -> Result<()> {
        let checker = RpcPreTradeChecker::new(
            "https://rpc.primary.example",
            "",
            1_000,
            "11111111111111111111111111111111",
            0.05,
            1_000,
            false,
            true,
            0,
            &route_tip_lamports(&[("rpc", 10_000)]),
            &route_cu_limits(&[("rpc", 300_000)]),
            &route_cu_prices(&[("rpc", 1_500)]),
            false,
            false,
            10_000,
        )
        .expect("checker should initialize");
        let mut allow_detail = "rpc_pretrade_ok".to_string();
        let mut intent = make_intent(ExecutionSide::Buy, 0.1);
        intent.notional_lamports = Lamports::ZERO;

        let decision = checker
            .evaluate_fee_overhead_policy(&intent, "rpc", true, &mut allow_detail)
            .expect("zero lamport notional should fail closed without panic");

        assert_eq!(decision.kind, PreTradeDecisionKind::TerminalReject);
        assert_eq!(decision.reason_code, "pretrade_fee_policy_invalid");
        Ok(())
    }

    #[test]
    fn fee_overhead_bps_ceil_saturates_when_notional_is_zero() {
        assert_eq!(fee_overhead_bps_ceil(5_000, 0), u64::MAX);
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
