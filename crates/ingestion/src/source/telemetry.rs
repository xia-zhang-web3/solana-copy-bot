#![cfg_attr(not(test), allow(dead_code))]

use chrono::Utc;
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use tracing::info;

use super::core::{percentile, push_sample};
use super::{IngestionRuntimeSnapshot, TELEMETRY_SAMPLE_CAPACITY};

#[path = "telemetry_parse_reject.rs"]
mod parse_reject;
#[path = "telemetry_report.rs"]
mod report;
#[path = "telemetry_samples.rs"]
mod samples;
#[path = "telemetry_snapshot.rs"]
mod snapshot;
#[path = "telemetry_state.rs"]
mod state;

pub(super) use self::parse_reject::classify_parse_reject_reason;
pub(super) use self::state::IngestionTelemetry;
