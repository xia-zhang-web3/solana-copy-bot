use super::{redacted_secret_debug_value, redacted_url_debug_value};
use serde::Deserialize;
use std::fmt;

#[derive(Clone, Deserialize)]
#[serde(default)]
pub struct IngestionConfig {
    pub source: String,
    pub helius_ws_url: String,
    pub helius_http_url: String,
    pub helius_http_urls: Vec<String>,
    pub yellowstone_grpc_url: String,
    pub yellowstone_x_token: String,
    pub yellowstone_connect_timeout_ms: u64,
    pub yellowstone_subscribe_timeout_ms: u64,
    pub yellowstone_stream_buffer_capacity: usize,
    pub yellowstone_reconnect_initial_ms: u64,
    pub yellowstone_reconnect_max_ms: u64,
    pub yellowstone_program_ids: Vec<String>,
    pub fetch_concurrency: usize,
    pub ws_queue_capacity: usize,
    pub output_queue_capacity: usize,
    pub prefetch_stale_drop_ms: u64,
    pub seen_signatures_ttl_ms: u64,
    pub queue_overflow_policy: String,
    pub reorder_hold_ms: u64,
    pub reorder_max_buffer: usize,
    pub telemetry_report_seconds: u64,
    pub subscribe_program_ids: Vec<String>,
    pub raydium_program_ids: Vec<String>,
    pub pumpswap_program_ids: Vec<String>,
    pub reconnect_initial_ms: u64,
    pub reconnect_max_ms: u64,
    pub tx_fetch_retries: u32,
    pub tx_fetch_retry_delay_ms: u64,
    pub tx_fetch_retry_max_ms: u64,
    pub tx_fetch_retry_jitter_ms: u64,
    pub tx_request_timeout_ms: u64,
    pub global_rpc_rps_limit: u64,
    pub per_endpoint_rpc_rps_limit: u64,
    pub seen_signatures_limit: usize,
    pub mock_interval_ms: u64,
}

impl Default for IngestionConfig {
    fn default() -> Self {
        Self {
            source: "mock".to_string(),
            helius_ws_url: "wss://mainnet.helius-rpc.com/?api-key=REPLACE_ME".to_string(),
            helius_http_url: "https://mainnet.helius-rpc.com/?api-key=REPLACE_ME".to_string(),
            helius_http_urls: Vec::new(),
            yellowstone_grpc_url: "REPLACE_ME".to_string(),
            yellowstone_x_token: "REPLACE_ME".to_string(),
            yellowstone_connect_timeout_ms: 5_000,
            yellowstone_subscribe_timeout_ms: 15_000,
            yellowstone_stream_buffer_capacity: 2_048,
            yellowstone_reconnect_initial_ms: 500,
            yellowstone_reconnect_max_ms: 8_000,
            yellowstone_program_ids: Vec::new(),
            fetch_concurrency: 8,
            ws_queue_capacity: 2048,
            output_queue_capacity: 1024,
            prefetch_stale_drop_ms: 45_000,
            seen_signatures_ttl_ms: 10 * 60 * 1_000,
            queue_overflow_policy: "block".to_string(),
            reorder_hold_ms: 2000,
            reorder_max_buffer: 512,
            telemetry_report_seconds: 30,
            subscribe_program_ids: Vec::new(),
            raydium_program_ids: vec![
                "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8".to_string(),
                "CPMMoo8L3F4NbTegBCKVN6DKuQh8fYfY4yR4j3uP9s5".to_string(),
            ],
            pumpswap_program_ids: vec!["pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA".to_string()],
            reconnect_initial_ms: 500,
            reconnect_max_ms: 8_000,
            tx_fetch_retries: 3,
            tx_fetch_retry_delay_ms: 150,
            tx_fetch_retry_max_ms: 2_000,
            tx_fetch_retry_jitter_ms: 150,
            tx_request_timeout_ms: 5_000,
            global_rpc_rps_limit: 0,
            per_endpoint_rpc_rps_limit: 0,
            seen_signatures_limit: 5_000,
            mock_interval_ms: 1000,
        }
    }
}

impl fmt::Debug for IngestionConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let helius_http_urls = self
            .helius_http_urls
            .iter()
            .map(|url| redacted_url_debug_value(url))
            .collect::<Vec<_>>();
        f.debug_struct("IngestionConfig")
            .field("source", &self.source)
            .field(
                "helius_ws_url",
                &redacted_url_debug_value(&self.helius_ws_url),
            )
            .field(
                "helius_http_url",
                &redacted_url_debug_value(&self.helius_http_url),
            )
            .field("helius_http_urls", &helius_http_urls)
            .field("yellowstone_grpc_url", &self.yellowstone_grpc_url)
            .field(
                "yellowstone_x_token",
                &redacted_secret_debug_value(&self.yellowstone_x_token),
            )
            .field(
                "yellowstone_connect_timeout_ms",
                &self.yellowstone_connect_timeout_ms,
            )
            .field(
                "yellowstone_subscribe_timeout_ms",
                &self.yellowstone_subscribe_timeout_ms,
            )
            .field(
                "yellowstone_stream_buffer_capacity",
                &self.yellowstone_stream_buffer_capacity,
            )
            .field(
                "yellowstone_reconnect_initial_ms",
                &self.yellowstone_reconnect_initial_ms,
            )
            .field(
                "yellowstone_reconnect_max_ms",
                &self.yellowstone_reconnect_max_ms,
            )
            .field("yellowstone_program_ids", &self.yellowstone_program_ids)
            .field("fetch_concurrency", &self.fetch_concurrency)
            .field("ws_queue_capacity", &self.ws_queue_capacity)
            .field("output_queue_capacity", &self.output_queue_capacity)
            .field("prefetch_stale_drop_ms", &self.prefetch_stale_drop_ms)
            .field("seen_signatures_ttl_ms", &self.seen_signatures_ttl_ms)
            .field("queue_overflow_policy", &self.queue_overflow_policy)
            .field("reorder_hold_ms", &self.reorder_hold_ms)
            .field("reorder_max_buffer", &self.reorder_max_buffer)
            .field("telemetry_report_seconds", &self.telemetry_report_seconds)
            .field("subscribe_program_ids", &self.subscribe_program_ids)
            .field("raydium_program_ids", &self.raydium_program_ids)
            .field("pumpswap_program_ids", &self.pumpswap_program_ids)
            .field("reconnect_initial_ms", &self.reconnect_initial_ms)
            .field("reconnect_max_ms", &self.reconnect_max_ms)
            .field("tx_fetch_retries", &self.tx_fetch_retries)
            .field("tx_fetch_retry_delay_ms", &self.tx_fetch_retry_delay_ms)
            .field("tx_fetch_retry_max_ms", &self.tx_fetch_retry_max_ms)
            .field("tx_fetch_retry_jitter_ms", &self.tx_fetch_retry_jitter_ms)
            .field("tx_request_timeout_ms", &self.tx_request_timeout_ms)
            .field("global_rpc_rps_limit", &self.global_rpc_rps_limit)
            .field(
                "per_endpoint_rpc_rps_limit",
                &self.per_endpoint_rpc_rps_limit,
            )
            .field("seen_signatures_limit", &self.seen_signatures_limit)
            .field("mock_interval_ms", &self.mock_interval_ms)
            .finish()
    }
}
