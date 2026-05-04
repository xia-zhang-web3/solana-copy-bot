#![cfg_attr(not(test), allow(dead_code))]

use chrono::Utc;
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use tracing::info;

use super::core::{percentile, push_sample};
use super::{IngestionRuntimeSnapshot, TELEMETRY_SAMPLE_CAPACITY};

include!("telemetry_state.rs");
include!("telemetry_samples.rs");
include!("telemetry_report.rs");
include!("telemetry_snapshot.rs");
include!("telemetry_parse_reject.rs");
