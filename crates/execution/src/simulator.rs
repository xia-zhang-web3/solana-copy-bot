use anyhow::{anyhow, Result};
use chrono::Utc;
use hmac::{Hmac, Mac};
use reqwest::blocking::Client;
use serde_json::{json, Value};
use sha2::Sha256;
use std::time::Duration as StdDuration;
use uuid::Uuid;

use crate::intent::ExecutionIntent;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone)]
pub struct SimulationResult {
    pub accepted: bool,
    pub detail: String,
}

pub trait IntentSimulator {
    fn simulate(&self, intent: &ExecutionIntent, route: &str) -> Result<SimulationResult>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PaperIntentSimulator;

impl IntentSimulator for PaperIntentSimulator {
    fn simulate(&self, intent: &ExecutionIntent, _route: &str) -> Result<SimulationResult> {
        if !intent.notional_sol.is_finite() || intent.notional_sol <= 0.0 {
            return Err(anyhow!("invalid notional for signal {}", intent.signal_id));
        }

        Ok(SimulationResult {
            accepted: true,
            detail: "paper_simulation_ok".to_string(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct FailClosedIntentSimulator {
    detail: String,
}

impl FailClosedIntentSimulator {
    pub fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }
}

impl IntentSimulator for FailClosedIntentSimulator {
    fn simulate(&self, _intent: &ExecutionIntent, _route: &str) -> Result<SimulationResult> {
        Ok(SimulationResult {
            accepted: false,
            detail: self.detail.clone(),
        })
    }
}

#[derive(Debug, Clone)]
struct AdapterHmacAuth {
    key_id: String,
    secret: String,
    ttl_sec: u64,
}

#[derive(Debug, Clone)]
pub struct AdapterIntentSimulator {
    endpoints: Vec<String>,
    auth_token: Option<String>,
    hmac_auth: Option<AdapterHmacAuth>,
    contract_version: String,
    require_policy_echo: bool,
    client: Client,
}

impl AdapterIntentSimulator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        primary_url: &str,
        fallback_url: &str,
        auth_token: &str,
        hmac_key_id: &str,
        hmac_secret: &str,
        hmac_ttl_sec: u64,
        contract_version: &str,
        require_policy_echo: bool,
        timeout_ms: u64,
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
        let client = Client::builder()
            .timeout(StdDuration::from_millis(timeout_ms.max(500)))
            .build()
            .ok()?;
        let token = auth_token.trim();
        let auth_token = if token.is_empty() {
            None
        } else {
            Some(token.to_string())
        };
        let hmac_key_id = hmac_key_id.trim();
        let hmac_secret = hmac_secret.trim();
        let hmac_auth = if hmac_key_id.is_empty() && hmac_secret.is_empty() {
            None
        } else if hmac_key_id.is_empty()
            || hmac_secret.is_empty()
            || !(5..=300).contains(&hmac_ttl_sec)
        {
            return None;
        } else {
            Some(AdapterHmacAuth {
                key_id: hmac_key_id.to_string(),
                secret: hmac_secret.to_string(),
                ttl_sec: hmac_ttl_sec,
            })
        };
        let contract_version = contract_version.trim();
        if contract_version.is_empty() || contract_version.len() > 64 {
            return None;
        }
        Some(Self {
            endpoints,
            auth_token,
            hmac_auth,
            contract_version: contract_version.to_string(),
            require_policy_echo,
            client,
        })
    }

    fn simulate_via_endpoint(
        &self,
        endpoint: &str,
        payload: &Value,
        expected_route: &str,
    ) -> Result<SimulationResult> {
        let payload_json = serde_json::to_string(payload)
            .map_err(|error| anyhow!("failed serializing simulate payload: {}", error))?;
        let mut request = self
            .client
            .post(endpoint)
            .header("content-type", "application/json")
            .body(payload_json.clone());
        if let Some(token) = self.auth_token.as_deref() {
            request = request.bearer_auth(token);
        }
        if let Some(hmac_auth) = self.hmac_auth.as_ref() {
            let timestamp_sec = Utc::now().timestamp();
            let nonce = Uuid::new_v4().simple().to_string();
            let signature_payload = format!(
                "{}\n{}\n{}\n{}",
                timestamp_sec, hmac_auth.ttl_sec, nonce, payload_json
            );
            let signature = compute_hmac_signature_hex(
                hmac_auth.secret.as_bytes(),
                signature_payload.as_bytes(),
            )?;
            request = request
                .header("x-copybot-key-id", hmac_auth.key_id.as_str())
                .header("x-copybot-timestamp", timestamp_sec.to_string())
                .header("x-copybot-auth-ttl-sec", hmac_auth.ttl_sec.to_string())
                .header("x-copybot-nonce", nonce)
                .header("x-copybot-signature", signature)
                .header("x-copybot-signature-alg", "hmac-sha256-v1");
        }
        let response = request
            .send()
            .map_err(|error| anyhow!("endpoint={} request_error={}", endpoint, error))?;
        let status = response.status();
        if !status.is_success() {
            let status_code = status.as_u16();
            let body_text = response.text().unwrap_or_default();
            if status_code == 429 || status.is_server_error() {
                return Err(anyhow!(
                    "endpoint={} simulate_http_unavailable status={} body={}",
                    endpoint,
                    status_code,
                    body_text
                ));
            }
            return Ok(SimulationResult {
                accepted: false,
                detail: format!(
                    "simulation_http_rejected status={} body={}",
                    status_code, body_text
                ),
            });
        }
        let body: Value = response
            .json()
            .map_err(|error| anyhow!("endpoint={} invalid_json parse_error={}", endpoint, error))?;
        parse_adapter_simulate_response(
            &body,
            expected_route,
            self.contract_version.as_str(),
            self.require_policy_echo,
        )
    }
}

impl IntentSimulator for AdapterIntentSimulator {
    fn simulate(&self, intent: &ExecutionIntent, route: &str) -> Result<SimulationResult> {
        if !intent.notional_sol.is_finite() || intent.notional_sol <= 0.0 {
            return Err(anyhow!("invalid notional for signal {}", intent.signal_id));
        }
        let route = route.trim().to_ascii_lowercase();
        if route.is_empty() {
            return Ok(SimulationResult {
                accepted: false,
                detail: "simulation_route_missing".to_string(),
            });
        }
        let payload = json!({
            "action": "simulate",
            "contract_version": self.contract_version,
            "request_id": format!("sim:{}:{}", intent.signal_id, Uuid::new_v4().simple()),
            "signal_id": intent.signal_id,
            "side": intent.side.as_str(),
            "token": intent.token,
            "notional_sol": intent.notional_sol,
            "signal_ts": intent.signal_ts.to_rfc3339(),
            "route": route,
            "dry_run": true
        });

        let mut last_error: Option<anyhow::Error> = None;
        for endpoint in &self.endpoints {
            match self.simulate_via_endpoint(endpoint, &payload, route.as_str()) {
                Ok(result) => return Ok(result),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("all simulate adapter endpoints failed")))
    }
}

fn parse_adapter_simulate_response(
    body: &Value,
    expected_route: &str,
    expected_contract_version: &str,
    require_policy_echo: bool,
) -> Result<SimulationResult> {
    let status = body
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let ok_flag = body.get("ok").and_then(Value::as_bool);
    let is_reject = status == "reject" || ok_flag == Some(false);
    if is_reject {
        let code = body
            .get("code")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("simulation_rejected");
        let detail = body
            .get("detail")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("adapter simulation rejected order");
        return Ok(SimulationResult {
            accepted: false,
            detail: format!("{}: {}", code, detail),
        });
    }

    let route = body
        .get("route")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase());
    if let Some(route) = route {
        if route != expected_route {
            return Ok(SimulationResult {
                accepted: false,
                detail: format!(
                    "simulation_route_mismatch: response route={} expected={}",
                    route, expected_route
                ),
            });
        }
    } else if require_policy_echo {
        return Ok(SimulationResult {
            accepted: false,
            detail: "simulation_policy_echo_missing: route".to_string(),
        });
    }

    let response_contract_version = body
        .get("contract_version")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if let Some(version) = response_contract_version {
        if version != expected_contract_version {
            return Ok(SimulationResult {
                accepted: false,
                detail: format!(
                    "simulation_contract_version_mismatch: response={} expected={}",
                    version, expected_contract_version
                ),
            });
        }
    } else if require_policy_echo {
        return Ok(SimulationResult {
            accepted: false,
            detail: "simulation_policy_echo_missing: contract_version".to_string(),
        });
    }

    let accepted = body
        .get("accepted")
        .and_then(Value::as_bool)
        .or(ok_flag)
        .unwrap_or(!status.is_empty());
    let detail = body
        .get("detail")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("adapter_simulation_ok");

    Ok(SimulationResult {
        accepted,
        detail: detail.to_string(),
    })
}

fn compute_hmac_signature_hex(secret: &[u8], payload: &[u8]) -> Result<String> {
    let mut mac = HmacSha256::new_from_slice(secret)
        .map_err(|error| anyhow!("invalid HMAC secret: {}", error))?;
    mac.update(payload);
    let digest = mac.finalize().into_bytes();
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{:02x}", byte);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_adapter_simulate_response_accepts_success() -> Result<()> {
        let body = json!({
            "status": "ok",
            "ok": true,
            "route": "rpc",
            "contract_version": "v1",
            "detail": "simulated"
        });
        let parsed = parse_adapter_simulate_response(&body, "rpc", "v1", true)?;
        assert!(parsed.accepted);
        assert_eq!(parsed.detail, "simulated");
        Ok(())
    }

    #[test]
    fn parse_adapter_simulate_response_rejects_route_mismatch() -> Result<()> {
        let body = json!({
            "status": "ok",
            "ok": true,
            "route": "jito",
            "contract_version": "v1",
        });
        let parsed = parse_adapter_simulate_response(&body, "rpc", "v1", true)?;
        assert!(!parsed.accepted);
        assert!(parsed.detail.contains("simulation_route_mismatch"));
        Ok(())
    }

    #[test]
    fn parse_adapter_simulate_response_requires_contract_version_in_strict_mode() -> Result<()> {
        let body = json!({
            "status": "ok",
            "ok": true,
            "route": "rpc",
        });
        let parsed = parse_adapter_simulate_response(&body, "rpc", "v1", true)?;
        assert!(!parsed.accepted);
        assert!(parsed
            .detail
            .contains("simulation_policy_echo_missing: contract_version"));
        Ok(())
    }

    #[test]
    fn parse_adapter_simulate_response_maps_reject_payload() -> Result<()> {
        let body = json!({
            "status": "reject",
            "ok": false,
            "code": "no_route",
            "detail": "route unavailable"
        });
        let parsed = parse_adapter_simulate_response(&body, "rpc", "v1", false)?;
        assert!(!parsed.accepted);
        assert_eq!(parsed.detail, "no_route: route unavailable");
        Ok(())
    }
}
