use anyhow::Result;
use std::env;

use crate::env_parsing::{
    normalize_ingestion_queue_overflow_policy, parse_env_bool, parse_env_number,
    parse_env_string_list,
};
use crate::AppConfig;

pub(super) fn apply_ingestion_and_history_env_overrides(config: &mut AppConfig) -> Result<()> {
    if let Ok(source) = env::var("SOLANA_COPY_BOT_INGESTION_SOURCE") {
        config.ingestion.source = source;
    }
    if let Ok(ws_url) = env::var("SOLANA_COPY_BOT_HELIUS_WS_URL") {
        config.ingestion.helius_ws_url = ws_url;
    }
    if let Ok(http_url) = env::var("SOLANA_COPY_BOT_HELIUS_HTTP_URL") {
        config.ingestion.helius_http_url = http_url;
    }
    if let Ok(grpc_url) = env::var("SOLANA_COPY_BOT_YELLOWSTONE_GRPC_URL") {
        config.ingestion.yellowstone_grpc_url = grpc_url;
    }
    if let Ok(x_token) = env::var("SOLANA_COPY_BOT_YELLOWSTONE_X_TOKEN") {
        config.ingestion.yellowstone_x_token = x_token;
    }
    if let Some(connect_timeout_ms) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_YELLOWSTONE_CONNECT_TIMEOUT_MS", "u64")?
    {
        config.ingestion.yellowstone_connect_timeout_ms = connect_timeout_ms;
    }
    if let Some(subscribe_timeout_ms) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_YELLOWSTONE_SUBSCRIBE_TIMEOUT_MS", "u64")?
    {
        config.ingestion.yellowstone_subscribe_timeout_ms = subscribe_timeout_ms;
    }
    if let Some(stream_buffer_capacity) = parse_env_number::<usize>(
        "SOLANA_COPY_BOT_YELLOWSTONE_STREAM_BUFFER_CAPACITY",
        "usize",
    )? {
        config.ingestion.yellowstone_stream_buffer_capacity = stream_buffer_capacity;
    }
    if let Some(reconnect_initial_ms) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_YELLOWSTONE_RECONNECT_INITIAL_MS", "u64")?
    {
        config.ingestion.yellowstone_reconnect_initial_ms = reconnect_initial_ms;
    }
    if let Some(reconnect_max_ms) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_YELLOWSTONE_RECONNECT_MAX_MS", "u64")?
    {
        config.ingestion.yellowstone_reconnect_max_ms = reconnect_max_ms;
    }
    if let Ok(program_ids_csv) = env::var("SOLANA_COPY_BOT_YELLOWSTONE_PROGRAM_IDS") {
        config.ingestion.yellowstone_program_ids =
            parse_env_string_list(&program_ids_csv, "SOLANA_COPY_BOT_YELLOWSTONE_PROGRAM_IDS")?;
    }
    if let Ok(http_urls_csv) = env::var("SOLANA_COPY_BOT_INGESTION_HELIUS_HTTP_URLS") {
        config.ingestion.helius_http_urls =
            parse_env_string_list(&http_urls_csv, "SOLANA_COPY_BOT_INGESTION_HELIUS_HTTP_URLS")?;
    }
    if let Some(enabled) = parse_env_bool("SOLANA_COPY_BOT_HISTORY_RETENTION_ENABLED")? {
        config.history_retention.enabled = enabled;
    }
    if let Some(sweep_seconds) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_HISTORY_RETENTION_SWEEP_SECONDS", "u64")?
    {
        config.history_retention.sweep_seconds = sweep_seconds;
    }
    if let Some(protected_history_days) = parse_env_number::<u32>(
        "SOLANA_COPY_BOT_HISTORY_RETENTION_PROTECTED_HISTORY_DAYS",
        "u32",
    )? {
        config.history_retention.protected_history_days = protected_history_days;
    }
    if let Some(risk_events_days) =
        parse_env_number::<u32>("SOLANA_COPY_BOT_HISTORY_RETENTION_RISK_EVENTS_DAYS", "u32")?
    {
        config.history_retention.risk_events_days = risk_events_days;
    }
    if let Some(copy_signals_days) =
        parse_env_number::<u32>("SOLANA_COPY_BOT_HISTORY_RETENTION_COPY_SIGNALS_DAYS", "u32")?
    {
        config.history_retention.copy_signals_days = copy_signals_days;
    }
    if let Some(orders_days) =
        parse_env_number::<u32>("SOLANA_COPY_BOT_HISTORY_RETENTION_ORDERS_DAYS", "u32")?
    {
        config.history_retention.orders_days = orders_days;
    }
    if let Some(fills_days) =
        parse_env_number::<u32>("SOLANA_COPY_BOT_HISTORY_RETENTION_FILLS_DAYS", "u32")?
    {
        config.history_retention.fills_days = fills_days;
    }
    if let Some(shadow_closed_trades_days) = parse_env_number::<u32>(
        "SOLANA_COPY_BOT_HISTORY_RETENTION_SHADOW_CLOSED_TRADES_DAYS",
        "u32",
    )? {
        config.history_retention.shadow_closed_trades_days = shadow_closed_trades_days;
    }
    if let Some(fetch_concurrency) =
        parse_env_number::<usize>("SOLANA_COPY_BOT_INGESTION_FETCH_CONCURRENCY", "usize")?
    {
        config.ingestion.fetch_concurrency = fetch_concurrency;
    }
    if let Some(ws_queue_capacity) =
        parse_env_number::<usize>("SOLANA_COPY_BOT_INGESTION_WS_QUEUE_CAPACITY", "usize")?
    {
        config.ingestion.ws_queue_capacity = ws_queue_capacity;
    }
    if let Some(output_queue_capacity) =
        parse_env_number::<usize>("SOLANA_COPY_BOT_INGESTION_OUTPUT_QUEUE_CAPACITY", "usize")?
    {
        config.ingestion.output_queue_capacity = output_queue_capacity;
    }
    if let Some(prefetch_stale_drop_ms) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_INGESTION_PREFETCH_STALE_DROP_MS", "u64")?
    {
        config.ingestion.prefetch_stale_drop_ms = prefetch_stale_drop_ms;
    }
    if let Some(seen_signatures_ttl_ms) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_INGESTION_SEEN_SIGNATURES_TTL_MS", "u64")?
    {
        config.ingestion.seen_signatures_ttl_ms = seen_signatures_ttl_ms;
    }
    if let Ok(policy) = env::var("SOLANA_COPY_BOT_INGESTION_QUEUE_OVERFLOW_POLICY") {
        config.ingestion.queue_overflow_policy =
            normalize_ingestion_queue_overflow_policy(&policy)?;
    }
    if let Some(reorder_hold_ms) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_INGESTION_REORDER_HOLD_MS", "u64")?
    {
        config.ingestion.reorder_hold_ms = reorder_hold_ms;
    }
    if let Some(reorder_max_buffer) =
        parse_env_number::<usize>("SOLANA_COPY_BOT_INGESTION_REORDER_MAX_BUFFER", "usize")?
    {
        config.ingestion.reorder_max_buffer = reorder_max_buffer;
    }
    if let Some(telemetry_report_seconds) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_INGESTION_TELEMETRY_REPORT_SECONDS", "u64")?
    {
        config.ingestion.telemetry_report_seconds = telemetry_report_seconds;
    }
    if let Some(tx_fetch_retries) =
        parse_env_number::<u32>("SOLANA_COPY_BOT_INGESTION_TX_FETCH_RETRIES", "u32")?
    {
        config.ingestion.tx_fetch_retries = tx_fetch_retries;
    }
    if let Some(tx_fetch_retry_delay_ms) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_INGESTION_TX_FETCH_RETRY_DELAY_MS", "u64")?
    {
        config.ingestion.tx_fetch_retry_delay_ms = tx_fetch_retry_delay_ms;
    }
    if let Some(tx_fetch_retry_max_ms) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_INGESTION_TX_FETCH_RETRY_MAX_MS", "u64")?
    {
        config.ingestion.tx_fetch_retry_max_ms = tx_fetch_retry_max_ms;
    }
    if let Some(tx_fetch_retry_jitter_ms) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_INGESTION_TX_FETCH_RETRY_JITTER_MS", "u64")?
    {
        config.ingestion.tx_fetch_retry_jitter_ms = tx_fetch_retry_jitter_ms;
    }
    if let Some(tx_request_timeout_ms) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_INGESTION_TX_REQUEST_TIMEOUT_MS", "u64")?
    {
        config.ingestion.tx_request_timeout_ms = tx_request_timeout_ms;
    }
    if let Some(global_rpc_rps_limit) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_INGESTION_GLOBAL_RPC_RPS_LIMIT", "u64")?
    {
        config.ingestion.global_rpc_rps_limit = global_rpc_rps_limit;
    }
    if let Some(per_endpoint_rpc_rps_limit) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_INGESTION_PER_ENDPOINT_RPC_RPS_LIMIT",
        "u64",
    )? {
        config.ingestion.per_endpoint_rpc_rps_limit = per_endpoint_rpc_rps_limit;
    }
    Ok(())
}
