use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::IngestionConfig;
use copybot_core_types::ExactSwapAmounts;
use reqwest::{Client, Url};
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{self, Interval};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tracing::warn;
#[cfg(test)]
use yellowstone_grpc_proto::prelude::{CommitmentLevel, TransactionStatusMeta, UiTokenAmount};

mod core;
#[allow(dead_code)]
mod helius_fetch;
#[allow(dead_code)]
mod helius_parser;
#[allow(dead_code)]
mod helius_pipeline;
mod queue;
mod rate_limit;
mod reorder;
mod telemetry;
mod yellowstone;
mod yellowstone_pipeline;
mod yellowstone_proto;
mod yellowstone_request;
mod yellowstone_source;

#[cfg(test)]
use self::core::compute_retry_delay;
use self::core::{
    decrement_atomic_usize, effective_per_endpoint_rps_limit, normalize_program_ids_or_fallback,
};
use self::helius_pipeline::{fetch_worker_loop, ws_reader_loop};
use self::queue::{OverflowQueue, QueueOverflowPolicy, QueuePushResult};
use self::rate_limit::{HeliusEndpoint, TokenBucketLimiter};
use self::reorder::{ReorderBuffer, ReorderRelease};
#[cfg(test)]
use self::telemetry::classify_parse_reject_reason;
use self::telemetry::IngestionTelemetry;
#[cfg(test)]
use self::yellowstone_proto::infer_swap_from_proto_balances;

include!("source_parts/01_types_mock.rs");
include!("source_parts/02_helius.rs");
include!("source_parts/03_yellowstone_shell.rs");

#[cfg(test)]
mod tests {
    use super::core::{is_seen_signature, mark_seen_signature, prune_seen_signatures};
    use super::yellowstone::build_yellowstone_subscribe_request;
    use super::*;
    use anyhow::anyhow;
    use serde_json::{json, Value};
    use std::collections::{HashMap, VecDeque};

    fn token_balance(owner: &str, mint: &str, amount: &str) -> Value {
        let raw_amount = amount
            .parse::<u64>()
            .expect("token balance fixture must be an integer ui amount")
            .saturating_mul(1_000_000)
            .to_string();
        json!({
            "owner": owner,
            "mint": mint,
            "uiTokenAmount": {
                "uiAmountString": amount,
                "amount": raw_amount,
                "decimals": 6
            }
        })
    }

    fn token_balance_without_raw(owner: &str, mint: &str, amount: &str) -> Value {
        json!({
            "owner": owner,
            "mint": mint,
            "uiTokenAmount": {
                "uiAmountString": amount
            }
        })
    }

    fn test_runtime_config(telemetry: Arc<IngestionTelemetry>) -> Arc<HeliusRuntimeConfig> {
        Arc::new(HeliusRuntimeConfig {
            ws_url: "wss://example".to_string(),
            http_endpoints: vec![Arc::new(HeliusEndpoint {
                url: "https://example".to_string(),
                limiter: None,
            })],
            http_endpoint_rr: AtomicUsize::new(0),
            global_http_limiter: None,
            reconnect_initial_ms: 500,
            reconnect_max_ms: 8_000,
            tx_fetch_retries: 1,
            tx_fetch_retry_base_ms: 50,
            tx_fetch_retry_max_ms: 500,
            tx_fetch_retry_jitter_ms: 20,
            seen_signatures_limit: 100,
            seen_signatures_ttl: Duration::from_secs(120),
            prefetch_stale_drop: Some(Duration::from_secs(30)),
            interested_program_ids: HashSet::new(),
            raydium_program_ids: HashSet::new(),
            pumpswap_program_ids: HashSet::new(),
            http_client: Client::new(),
            telemetry,
        })
    }

    include!("source_tests/01_helius_and_queue.rs");
    include!("source_tests/02_yellowstone_proto.rs");
}
