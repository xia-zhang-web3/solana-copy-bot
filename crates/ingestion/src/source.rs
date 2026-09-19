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

mod core;
pub(crate) mod scoped_capture;
pub(crate) mod durable;
#[allow(dead_code)]
mod helius_fetch;
#[allow(dead_code)]
mod helius_parser;
#[allow(dead_code)]
mod helius_pipeline;
mod native_attribution;
mod pumpswap_instruction;
mod queue;
mod rate_limit;
mod reorder;
mod rpc_backfill;
mod target_balance_rows;
mod telemetry;
mod yellowstone;
#[allow(dead_code)]
mod yellowstone_association;
#[allow(dead_code)]
mod yellowstone_block_association;
mod yellowstone_facts;
mod yellowstone_message_time;
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
use self::telemetry::IngestionTelemetry;

#[path = "source_parts/02_helius_new.rs"]
mod helius_new;
#[path = "source_parts/02_helius_runtime.rs"]
mod helius_runtime;
#[path = "source_parts/01_types_mock.rs"]
mod types_mock;
#[path = "source_parts/03_yellowstone_shell.rs"]
mod yellowstone_shell;

use self::types_mock::{
    redacted_url_for_log, FetchedObservation, HeliusPipeline, HeliusRuntimeConfig, HeliusWsSource,
    HeliusWsStream, LogsNotification, NotificationQueue, OutputRecvOutcome, RawObservationQueue,
    SeenSignatureEntry, SOL_MINT, TELEMETRY_SAMPLE_CAPACITY, WS_IDLE_TIMEOUT_SECS,
};
pub use self::types_mock::{IngestionRuntimeSnapshot, IngestionSource, RawSwapObservation};
pub(crate) use self::yellowstone_shell::{
    YellowstoneGrpcSource, YellowstoneParsedUpdate, YellowstoneRuntimeConfig,
};
use self::yellowstone_shell::{YellowstonePipeline, YellowstoneRecvOutcome};
pub(crate) use rpc_backfill::fetch_recent_raw_swaps_for_wallets;

#[cfg(test)]
#[path = "source/tests.rs"]
mod tests;
