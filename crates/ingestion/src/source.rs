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
use self::telemetry::IngestionTelemetry;

include!("source_parts/01_types_mock.rs");
#[path = "source_parts/02_helius_new.rs"]
mod helius_new;
#[path = "source_parts/02_helius_runtime.rs"]
mod helius_runtime;
include!("source_parts/03_yellowstone_shell.rs");

#[cfg(test)]
#[path = "source/tests.rs"]
mod tests;
