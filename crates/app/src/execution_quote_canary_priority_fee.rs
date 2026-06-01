use crate::execution_quote_canary_helpers::{
    priority_fee_lamports, short_error, truncate_for_log, PriorityFeeSample, QUOTE_STATUS_ERROR,
    QUOTE_STATUS_OK, QUOTE_STATUS_SKIPPED,
};
use anyhow::{anyhow, Context, Result};
use copybot_config::ExecutionConfig;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::Mutex;

const PRIORITY_FEE_MIN_REQUEST_INTERVAL: StdDuration = StdDuration::from_millis(1_100);
const PRIORITY_FEE_CACHE_TTL: StdDuration = StdDuration::from_secs(5);

#[derive(Debug, Clone)]
pub(crate) struct PriorityFeeSampler {
    config: ExecutionConfig,
    http: reqwest::Client,
    state: Arc<Mutex<PriorityFeeSamplerState>>,
}

#[derive(Debug, Default)]
struct PriorityFeeSamplerState {
    cached: Option<CachedPriorityFeeSample>,
    last_request_at: Option<Instant>,
}

#[derive(Debug, Clone)]
struct CachedPriorityFeeSample {
    sample: PriorityFeeSample,
    cached_at: Instant,
}

impl PriorityFeeSampler {
    pub(crate) fn new(config: ExecutionConfig, http: reqwest::Client) -> Self {
        Self {
            config,
            http,
            state: Arc::new(Mutex::new(PriorityFeeSamplerState::default())),
        }
    }

    pub(crate) async fn sample_if_enabled(&self) -> Option<PriorityFeeSample> {
        if !self.config.priority_fee_canary_enabled {
            return None;
        }
        Some(self.sample().await)
    }

    async fn sample(&self) -> PriorityFeeSample {
        if let Some(sample) = self.cached_or_throttled_sample().await {
            return sample;
        }
        let sample = match self.fetch_sample_inner().await {
            Ok(sample) => sample,
            Err(error) if is_priority_fee_rate_limit_error(&error) => skipped_sample(),
            Err(error) => PriorityFeeSample {
                status: QUOTE_STATUS_ERROR.to_string(),
                lamports: None,
                json: None,
                error: Some(short_error(&error)),
            },
        };
        if sample.status == QUOTE_STATUS_OK {
            let mut state = self.state.lock().await;
            state.cached = Some(CachedPriorityFeeSample {
                sample: sample.clone(),
                cached_at: Instant::now(),
            });
        }
        sample
    }

    async fn cached_or_throttled_sample(&self) -> Option<PriorityFeeSample> {
        let now = Instant::now();
        let mut state = self.state.lock().await;
        if let Some(cached) = state.cached.as_ref() {
            if now.duration_since(cached.cached_at) <= PRIORITY_FEE_CACHE_TTL {
                return Some(cached.sample.clone());
            }
        }
        if let Some(last_request_at) = state.last_request_at {
            if now.duration_since(last_request_at) < PRIORITY_FEE_MIN_REQUEST_INTERVAL {
                return Some(
                    state
                        .cached
                        .as_ref()
                        .map(|cached| cached.sample.clone())
                        .unwrap_or_else(skipped_sample),
                );
            }
        }
        state.last_request_at = Some(now);
        None
    }

    async fn fetch_sample_inner(&self) -> Result<PriorityFeeSample> {
        let rpc_url = self.config.priority_fee_canary_rpc_url.trim();
        if rpc_url.is_empty() {
            return Err(anyhow!("priority fee canary RPC URL is empty"));
        }
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "qn_estimatePriorityFees",
            "params": {
                "last_n_blocks": self.config.priority_fee_canary_last_n_blocks,
                "account": self.config.priority_fee_canary_account.trim(),
                "api_version": 2
            }
        });
        let response = self
            .http
            .post(rpc_url)
            .json(&body)
            .timeout(StdDuration::from_millis(
                self.config.priority_fee_canary_timeout_ms.max(1),
            ))
            .send()
            .await
            .context("priority fee canary request failed")?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "priority fee canary returned HTTP {status}: {}",
                truncate_for_log(&body, 240)
            ));
        }
        let value: Value = response
            .json()
            .await
            .context("priority fee canary response JSON decode failed")?;
        if let Some(error) = value.get("error") {
            return Err(anyhow!("priority fee canary RPC error: {error}"));
        }
        let result = value.get("result").unwrap_or(&value);
        Ok(PriorityFeeSample {
            status: QUOTE_STATUS_OK.to_string(),
            lamports: priority_fee_lamports(result),
            json: Some(result.to_string()),
            error: None,
        })
    }
}

pub(crate) fn is_priority_fee_rate_limit_error(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}").to_ascii_lowercase();
    message.contains("429")
        || message.contains("too many requests")
        || message.contains("request limit reached")
}

fn skipped_sample() -> PriorityFeeSample {
    PriorityFeeSample {
        status: QUOTE_STATUS_SKIPPED.to_string(),
        lamports: None,
        json: Some("{\"reason\":\"priority_fee_throttled\"}".to_string()),
        error: None,
    }
}
