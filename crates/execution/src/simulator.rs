use anyhow::{anyhow, Result};
use chrono::Utc;
use reqwest::blocking::Client;
use serde_json::{json, Value};
use std::time::Duration as StdDuration;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::auth::compute_hmac_signature_hex;
use crate::intent::ExecutionIntent;

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
        if contract_version.is_empty()
            || contract_version.len() > 64
            || !is_valid_contract_version_token(contract_version)
        {
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
        let body: Value = match response.json() {
            Ok(value) => value,
            Err(error) => {
                return Ok(SimulationResult {
                    accepted: false,
                    detail: format!(
                        "simulation_invalid_json endpoint={} parse_error={}",
                        endpoint, error
                    ),
                });
            }
        };
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
            debug!(
                endpoint = %endpoint,
                route = %route,
                signal_id = %intent.signal_id,
                "adapter simulator attempt"
            );
            match self.simulate_via_endpoint(endpoint, &payload, route.as_str()) {
                Ok(result) => {
                    if !result.accepted {
                        warn!(
                            endpoint = %endpoint,
                            route = %route,
                            signal_id = %intent.signal_id,
                            detail = %result.detail,
                            "adapter simulator terminal reject"
                        );
                    }
                    return Ok(result);
                }
                Err(error) => {
                    warn!(
                        endpoint = %endpoint,
                        route = %route,
                        signal_id = %intent.signal_id,
                        error = %error,
                        "adapter simulator retryable endpoint error; trying fallback if available"
                    );
                    last_error = Some(error);
                }
            }
        }
        let final_error =
            last_error.unwrap_or_else(|| anyhow!("all simulate adapter endpoints failed"));
        warn!(
            route = %route,
            signal_id = %intent.signal_id,
            error = %final_error,
            "adapter simulator failed on all endpoints"
        );
        Err(final_error)
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
    let accepted_flag = body.get("accepted").and_then(Value::as_bool);
    let is_reject = matches!(
        status.as_str(),
        "reject" | "rejected" | "error" | "failed" | "failure"
    ) || ok_flag == Some(false)
        || accepted_flag == Some(false);
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

    let is_known_success_status = matches!(status.as_str(), "ok" | "accepted" | "success");
    let is_known_status = is_known_success_status
        || matches!(
            status.as_str(),
            "reject" | "rejected" | "error" | "failed" | "failure"
        );
    if !status.is_empty() && !is_known_status {
        return Ok(SimulationResult {
            accepted: false,
            detail: format!("simulation_invalid_status: {}", status),
        });
    }

    let accepted = accepted_flag.or(ok_flag).unwrap_or(is_known_success_status);
    let detail_default = if accepted {
        "adapter_simulation_ok"
    } else {
        "simulation_rejected"
    };
    let detail = body
        .get("detail")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(detail_default);

    Ok(SimulationResult {
        accepted,
        detail: detail.to_string(),
    })
}

fn is_valid_contract_version_token(value: &str) -> bool {
    value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent::ExecutionSide;
    use chrono::Utc;
    use serde_json::json;
    use std::collections::HashMap;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration as StdDuration;

    fn make_intent() -> ExecutionIntent {
        ExecutionIntent {
            signal_id: "shadow:test:wallet:buy:token".to_string(),
            leader_wallet: "leader".to_string(),
            side: ExecutionSide::Buy,
            token: "token".to_string(),
            notional_sol: 0.1,
            signal_ts: Utc::now(),
        }
    }

    #[derive(Debug)]
    struct CapturedHttpRequest {
        path: String,
        headers: HashMap<String, String>,
        body: String,
    }

    fn find_header_end(buffer: &[u8]) -> Option<usize> {
        buffer.windows(4).position(|window| window == b"\r\n\r\n")
    }

    fn spawn_one_shot_simulate_adapter(
        status: u16,
        response_body: Value,
    ) -> Option<(String, thread::JoinHandle<CapturedHttpRequest>)> {
        let body = response_body.to_string();
        spawn_one_shot_simulate_adapter_raw(status, "application/json", &body)
    }

    fn spawn_one_shot_simulate_adapter_raw(
        status: u16,
        content_type: &str,
        response_body: &str,
    ) -> Option<(String, thread::JoinHandle<CapturedHttpRequest>)> {
        let listener = match TcpListener::bind("127.0.0.1:0") {
            Ok(listener) => listener,
            Err(error) => {
                eprintln!(
                    "skipping simulator HTTP integration test: failed to bind 127.0.0.1:0: {}",
                    error
                );
                return None;
            }
        };
        if let Err(error) = listener.set_nonblocking(false) {
            eprintln!(
                "skipping simulator HTTP integration test: failed to set blocking mode: {}",
                error
            );
            return None;
        }
        let addr = match listener.local_addr() {
            Ok(addr) => addr,
            Err(error) => {
                eprintln!(
                    "skipping simulator HTTP integration test: failed to read listener addr: {}",
                    error
                );
                return None;
            }
        };
        let response_body = response_body.to_string();
        let content_type = content_type.to_string();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept simulator client");
            stream
                .set_read_timeout(Some(StdDuration::from_secs(5)))
                .expect("set read timeout");
            let mut buffer = Vec::new();
            let mut chunk = [0_u8; 1024];
            let mut header_end = None;
            while header_end.is_none() {
                let read = stream.read(&mut chunk).expect("read request headers");
                if read == 0 {
                    break;
                }
                buffer.extend_from_slice(&chunk[..read]);
                header_end = find_header_end(&buffer).map(|offset| offset + 4);
            }

            let header_end = header_end.expect("request headers must be present");
            let header_text = String::from_utf8_lossy(&buffer[..header_end]).to_string();
            let mut lines = header_text.split("\r\n");
            let request_line = lines.next().unwrap_or_default().to_string();
            let path = request_line
                .split_whitespace()
                .nth(1)
                .unwrap_or_default()
                .to_string();
            let mut headers = HashMap::new();
            let mut content_length = 0usize;
            for line in lines {
                if line.trim().is_empty() {
                    continue;
                }
                if let Some((name, value)) = line.split_once(':') {
                    let key = name.trim().to_ascii_lowercase();
                    let value = value.trim().to_string();
                    if key == "content-length" {
                        content_length = value.parse::<usize>().unwrap_or(0);
                    }
                    headers.insert(key, value);
                }
            }

            while buffer.len() < header_end.saturating_add(content_length) {
                let read = stream.read(&mut chunk).expect("read request body");
                if read == 0 {
                    break;
                }
                buffer.extend_from_slice(&chunk[..read]);
            }
            let body_end = header_end
                .saturating_add(content_length)
                .min(buffer.len())
                .max(header_end);
            let body = String::from_utf8_lossy(&buffer[header_end..body_end]).to_string();

            let reason = if status == 200 { "OK" } else { "ERR" };
            let response = format!(
                "HTTP/1.1 {} {}\r\ncontent-type: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                status,
                reason,
                content_type,
                response_body.len(),
                response_body
            );
            stream
                .write_all(response.as_bytes())
                .expect("write simulator response");
            stream.flush().expect("flush simulator response");

            CapturedHttpRequest {
                path,
                headers,
                body,
            }
        });
        Some((format!("http://{}/simulate", addr), handle))
    }

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

    #[test]
    fn parse_adapter_simulate_response_rejects_unknown_status_without_ok_flags() -> Result<()> {
        let body = json!({
            "status": "pending",
            "route": "rpc",
            "contract_version": "v1"
        });
        let parsed = parse_adapter_simulate_response(&body, "rpc", "v1", true)?;
        assert!(!parsed.accepted);
        assert_eq!(parsed.detail, "simulation_invalid_status: pending");
        Ok(())
    }

    #[test]
    fn parse_adapter_simulate_response_rejects_error_status_without_ok_flags() -> Result<()> {
        let body = json!({
            "status": "error",
            "route": "rpc",
            "contract_version": "v1"
        });
        let parsed = parse_adapter_simulate_response(&body, "rpc", "v1", true)?;
        assert!(!parsed.accepted);
        assert_eq!(
            parsed.detail,
            "simulation_rejected: adapter simulation rejected order"
        );
        Ok(())
    }

    #[test]
    fn adapter_intent_simulator_requires_non_empty_contract_version_token() {
        let missing = AdapterIntentSimulator::new(
            "https://adapter.example/simulate",
            "",
            "",
            "",
            "",
            30,
            "",
            false,
            2_000,
        );
        assert!(missing.is_none());

        let invalid = AdapterIntentSimulator::new(
            "https://adapter.example/simulate",
            "",
            "",
            "",
            "",
            30,
            "v1 beta",
            false,
            2_000,
        );
        assert!(invalid.is_none());
    }

    #[test]
    fn parse_adapter_simulate_response_rejects_unknown_status_even_with_accepted_true() -> Result<()>
    {
        let body = json!({
            "status": "pending",
            "accepted": true,
            "route": "rpc",
            "contract_version": "v1"
        });
        let parsed = parse_adapter_simulate_response(&body, "rpc", "v1", true)?;
        assert!(!parsed.accepted);
        assert_eq!(parsed.detail, "simulation_invalid_status: pending");
        Ok(())
    }

    #[test]
    fn parse_adapter_simulate_response_rejects_unknown_status_even_with_ok_true() -> Result<()> {
        let body = json!({
            "status": "pending",
            "ok": true,
            "route": "rpc",
            "contract_version": "v1"
        });
        let parsed = parse_adapter_simulate_response(&body, "rpc", "v1", true)?;
        assert!(!parsed.accepted);
        assert_eq!(parsed.detail, "simulation_invalid_status: pending");
        Ok(())
    }

    #[test]
    fn adapter_intent_simulator_posts_simulation_payload() {
        let response = json!({
            "status": "ok",
            "ok": true,
            "route": "rpc",
            "contract_version": "v1",
            "detail": "simulated"
        });
        let Some((endpoint, handle)) = spawn_one_shot_simulate_adapter(200, response) else {
            return;
        };
        let simulator =
            AdapterIntentSimulator::new(&endpoint, "", "", "", "", 30, "v1", true, 2_000)
                .expect("simulator should initialize");
        let result = simulator
            .simulate(&make_intent(), "rpc")
            .expect("simulate call should succeed");
        assert!(result.accepted);

        let captured = handle.join().expect("join simulator server thread");
        assert_eq!(captured.path, "/simulate");
        assert_eq!(
            captured
                .headers
                .get("content-type")
                .map(String::as_str)
                .unwrap_or_default(),
            "application/json"
        );
        let payload: Value = serde_json::from_str(&captured.body).expect("parse captured payload");
        assert_eq!(
            payload
                .get("action")
                .and_then(Value::as_str)
                .unwrap_or_default(),
            "simulate"
        );
        assert_eq!(
            payload
                .get("route")
                .and_then(Value::as_str)
                .unwrap_or_default(),
            "rpc"
        );
        assert_eq!(
            payload
                .get("contract_version")
                .and_then(Value::as_str)
                .unwrap_or_default(),
            "v1"
        );
        assert_eq!(
            payload
                .get("dry_run")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            true
        );
    }

    #[test]
    fn adapter_intent_simulator_hmac_signature_matches_raw_http_body() {
        let response = json!({
            "status": "ok",
            "ok": true,
            "route": "rpc",
            "contract_version": "v1",
            "detail": "simulated"
        });
        let Some((endpoint, handle)) = spawn_one_shot_simulate_adapter(200, response) else {
            return;
        };
        let hmac_secret = "super-secret";
        let simulator = AdapterIntentSimulator::new(
            &endpoint,
            "",
            "",
            "key-123",
            hmac_secret,
            30,
            "v1",
            true,
            2_000,
        )
        .expect("simulator should initialize");
        let result = simulator
            .simulate(&make_intent(), "rpc")
            .expect("simulate call should succeed");
        assert!(result.accepted);

        let captured = handle.join().expect("join simulator server thread");
        let headers = &captured.headers;
        let key_id = headers
            .get("x-copybot-key-id")
            .map(String::as_str)
            .unwrap_or_default();
        let timestamp = headers
            .get("x-copybot-timestamp")
            .map(String::as_str)
            .unwrap_or_default();
        let ttl = headers
            .get("x-copybot-auth-ttl-sec")
            .map(String::as_str)
            .unwrap_or_default();
        let nonce = headers
            .get("x-copybot-nonce")
            .map(String::as_str)
            .unwrap_or_default();
        let signature = headers
            .get("x-copybot-signature")
            .map(String::as_str)
            .unwrap_or_default();
        let alg = headers
            .get("x-copybot-signature-alg")
            .map(String::as_str)
            .unwrap_or_default();

        assert_eq!(key_id, "key-123");
        assert_eq!(ttl, "30");
        assert_eq!(alg, "hmac-sha256-v1");
        assert!(!timestamp.is_empty(), "timestamp header must be present");
        assert!(!nonce.is_empty(), "nonce header must be present");
        assert!(!signature.is_empty(), "signature header must be present");

        let signed_payload = format!("{}\n{}\n{}\n{}", timestamp, ttl, nonce, captured.body);
        let expected_signature = crate::auth::compute_hmac_signature_hex(
            hmac_secret.as_bytes(),
            signed_payload.as_bytes(),
        )
        .expect("compute expected signature");
        assert_eq!(signature, expected_signature);
    }

    #[test]
    fn adapter_intent_simulator_uses_fallback_endpoint_after_primary_failure() {
        let response = json!({
            "status": "ok",
            "ok": true,
            "route": "rpc",
            "contract_version": "v1",
            "detail": "simulated"
        });
        let Some((fallback_endpoint, handle)) = spawn_one_shot_simulate_adapter(200, response)
        else {
            return;
        };
        let simulator = AdapterIntentSimulator::new(
            "http://127.0.0.1:1/simulate",
            &fallback_endpoint,
            "",
            "",
            "",
            30,
            "v1",
            true,
            2_000,
        )
        .expect("simulator should initialize");
        let result = simulator
            .simulate(&make_intent(), "rpc")
            .expect("simulate should succeed via fallback endpoint");
        assert!(result.accepted);
        let _ = handle
            .join()
            .expect("join fallback simulator server thread");
    }

    #[test]
    fn adapter_intent_simulator_does_not_fallback_on_invalid_json_terminal_reject() {
        let Some((endpoint, handle)) =
            spawn_one_shot_simulate_adapter_raw(200, "application/json", "{not-json}")
        else {
            return;
        };

        let simulator = AdapterIntentSimulator::new(
            &endpoint,
            "http://127.0.0.1:1/simulate",
            "",
            "",
            "",
            30,
            "v1",
            true,
            2_000,
        )
        .expect("simulator should initialize");

        let result = simulator
            .simulate(&make_intent(), "rpc")
            .expect("invalid JSON must be terminal reject without fallback");
        assert!(!result.accepted);
        assert!(
            result.detail.contains("simulation_invalid_json"),
            "unexpected detail: {}",
            result.detail
        );
        let _ = handle.join().expect("join primary simulator server thread");
    }
}
