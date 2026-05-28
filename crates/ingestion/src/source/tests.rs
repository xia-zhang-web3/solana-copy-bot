use super::core::{
    effective_per_endpoint_rps_limit, is_seen_signature, mark_seen_signature, prune_seen_signatures,
};
use super::telemetry::classify_parse_reject_reason;
use super::yellowstone::build_yellowstone_subscribe_request;
use super::yellowstone_proto::{self, infer_swap_from_proto_balances};
use super::*;
use anyhow::anyhow;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use yellowstone_grpc_proto::prelude::{CommitmentLevel, TransactionStatusMeta, UiTokenAmount};

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

#[path = "../source_tests/01_helius_and_queue.rs"]
mod helius_and_queue;
#[path = "../source_tests/03_rpc_backfill.rs"]
mod rpc_backfill_tests;
#[path = "../source_tests/02_yellowstone_proto.rs"]
mod yellowstone_proto_tests;
