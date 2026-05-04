#![cfg_attr(not(test), allow(dead_code))]

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

use super::core::{
    decrement_atomic_usize, increment_atomic_usize, is_seen_signature, mark_seen_signature,
    prune_seen_signatures, sleep_with_backoff,
};
use super::helius_fetch::fetch_swap_with_retries;
use super::{
    redacted_url_for_log, FetchedObservation, HeliusRuntimeConfig, HeliusWsSource, HeliusWsStream,
    NotificationQueue, QueueOverflowPolicy, QueuePushResult, SeenSignatureEntry,
    WS_IDLE_TIMEOUT_SECS,
};

include!("helius_pipeline_ws_reader.rs");
include!("helius_pipeline_fetch_worker.rs");
