use super::*;
use copybot_core_types::SwapEvent;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration as StdDuration, Instant};
use tempfile::tempdir;

#[path = "../market_data_tests/00_market_data_tests.rs"]
mod base;
#[path = "../market_data_tests/01_market_data_tests.rs"]
mod observed_window;
#[path = "../market_data_tests/02_market_data_tests.rs"]
mod recent_raw;
#[path = "../market_data_tests/04_market_data_tests.rs"]
mod target_filters;
#[path = "../market_data_tests/03_market_data_tests.rs"]
mod wallet_activity;

use base::*;
