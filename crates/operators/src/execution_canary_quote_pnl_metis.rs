use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{
    ExecutionQuoteCanaryProviderSelectionSummary, ExecutionTinyProofReport,
};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

const MAX_TIMEOUT_MS: u64 = 5_000;
const MAX_MATCHED_TOKENS: usize = 20;

#[derive(Debug, Serialize)]
pub struct MetisDiagnosticsReport {
    pub as_of: DateTime<Utc>,
    pub timeout_ms: u64,
    pub token_sample_count: usize,
    pub new_pools: MetisEndpointDiagnostics,
    pub program_id_to_label: MetisEndpointDiagnostics,
}

#[derive(Debug, Serialize)]
pub struct MetisEndpointDiagnostics {
    pub status: String,
    pub http_status: Option<u16>,
    pub latency_ms: Option<u64>,
    pub item_count: Option<usize>,
    pub matched_token_count: usize,
    pub matched_tokens: Vec<String>,
    pub error: Option<String>,
}

pub fn build_metis_diagnostics(
    config: &ExecutionConfig,
    proof: &ExecutionTinyProofReport,
    provider_selection: &ExecutionQuoteCanaryProviderSelectionSummary,
    as_of: DateTime<Utc>,
) -> MetisDiagnosticsReport {
    let timeout_ms = config.quote_canary_timeout_ms.clamp(1, MAX_TIMEOUT_MS);
    let tokens = token_sample(proof, provider_selection);
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_millis(timeout_ms))
        .build();
    let (new_pools, program_id_to_label) = match client {
        Ok(client) => (
            fetch_endpoint(&client, config, "new-pools", "new pools", Some(&tokens)),
            fetch_endpoint(
                &client,
                config,
                "program-id-to-label",
                "program id labels",
                None,
            ),
        ),
        Err(error) => {
            let error = truncate_error(&error.to_string());
            (
                MetisEndpointDiagnostics::error(error.clone()),
                MetisEndpointDiagnostics::error(error),
            )
        }
    };
    MetisDiagnosticsReport {
        as_of,
        timeout_ms,
        token_sample_count: tokens.len(),
        new_pools,
        program_id_to_label,
    }
}

fn fetch_endpoint(
    client: &reqwest::blocking::Client,
    config: &ExecutionConfig,
    endpoint: &str,
    label: &str,
    token_sample: Option<&BTreeSet<String>>,
) -> MetisEndpointDiagnostics {
    let url = match endpoint_url(&config.quote_canary_base_url, endpoint) {
        Some(url) => url,
        None => return MetisEndpointDiagnostics::error(format!("{label} base URL is empty")),
    };
    let started = Instant::now();
    let mut request = client.get(url);
    if !config.quote_canary_api_key.trim().is_empty() {
        request = request.header("x-api-key", config.quote_canary_api_key.trim());
    }
    let response = match request.send() {
        Ok(response) => response,
        Err(error) => return MetisEndpointDiagnostics::error(truncate_error(&error.to_string())),
    };
    let status = response.status();
    let latency_ms = elapsed_ms(started);
    if !status.is_success() {
        let body = response.text().unwrap_or_default();
        return MetisEndpointDiagnostics {
            status: "error".to_string(),
            http_status: Some(status.as_u16()),
            latency_ms: Some(latency_ms),
            item_count: None,
            matched_token_count: 0,
            matched_tokens: Vec::new(),
            error: Some(truncate_error(&body)),
        };
    }
    let value = match response.json::<Value>() {
        Ok(value) => value,
        Err(error) => return MetisEndpointDiagnostics::error(truncate_error(&error.to_string())),
    };
    let matched_tokens = token_sample
        .map(|tokens| matched_tokens(&value, tokens))
        .unwrap_or_default();
    MetisEndpointDiagnostics {
        status: "ok".to_string(),
        http_status: Some(status.as_u16()),
        latency_ms: Some(latency_ms),
        item_count: json_item_count(&value),
        matched_token_count: matched_tokens.len(),
        matched_tokens,
        error: None,
    }
}

impl MetisEndpointDiagnostics {
    fn error(error: String) -> Self {
        Self {
            status: "error".to_string(),
            http_status: None,
            latency_ms: None,
            item_count: None,
            matched_token_count: 0,
            matched_tokens: Vec::new(),
            error: Some(error),
        }
    }
}

fn token_sample(
    proof: &ExecutionTinyProofReport,
    provider_selection: &ExecutionQuoteCanaryProviderSelectionSummary,
) -> BTreeSet<String> {
    let mut tokens = BTreeSet::new();
    for trade in &proof.trades {
        tokens.insert(trade.token.clone());
    }
    for order in &proof.recent_orders {
        if let Some(token) = &order.token {
            tokens.insert(token.clone());
        }
    }
    for position in &proof.open_positions {
        tokens.insert(position.token.clone());
    }
    for event in &provider_selection.latest {
        tokens.insert(event.token.clone());
    }
    tokens.retain(|token| !token.trim().is_empty());
    tokens
}

fn endpoint_url(base_url: &str, endpoint: &str) -> Option<String> {
    let trimmed = base_url.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return None;
    }
    let root = trimmed
        .strip_suffix("/pump-fun/swap-instructions")
        .or_else(|| trimmed.strip_suffix("/pump-fun/quote"))
        .or_else(|| trimmed.strip_suffix("/pump-fun/swap"))
        .or_else(|| trimmed.strip_suffix("/swap-instructions"))
        .or_else(|| trimmed.strip_suffix("/quote"))
        .or_else(|| trimmed.strip_suffix("/swap"))
        .unwrap_or(trimmed);
    Some(format!("{root}/{endpoint}"))
}

fn matched_tokens(value: &Value, tokens: &BTreeSet<String>) -> Vec<String> {
    let mut matches = BTreeSet::new();
    collect_matched_tokens(value, tokens, &mut matches);
    matches.into_iter().take(MAX_MATCHED_TOKENS).collect()
}

fn collect_matched_tokens(
    value: &Value,
    tokens: &BTreeSet<String>,
    matches: &mut BTreeSet<String>,
) {
    match value {
        Value::String(raw) => {
            let trimmed = raw.trim();
            if tokens.contains(trimmed) {
                matches.insert(trimmed.to_string());
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_matched_tokens(item, tokens, matches);
            }
        }
        Value::Object(map) => {
            for item in map.values() {
                collect_matched_tokens(item, tokens, matches);
            }
        }
        _ => {}
    }
}

fn json_item_count(value: &Value) -> Option<usize> {
    if let Some(items) = value.as_array() {
        return Some(items.len());
    }
    let object = value.as_object()?;
    for key in ["data", "pools", "tokens", "result"] {
        if let Some(items) = object.get(key).and_then(Value::as_array) {
            return Some(items.len());
        }
    }
    Some(object.len())
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn truncate_error(raw: &str) -> String {
    const LIMIT: usize = 240;
    let trimmed = raw.trim();
    if trimmed.len() <= LIMIT {
        trimmed.to_string()
    } else {
        format!("{}...", &trimmed[..LIMIT])
    }
}
