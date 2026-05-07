use super::*;
use anyhow::Context;
use chrono::Duration;
use copybot_core_types::SwapEvent;
use copybot_core_types::{ExactSwapAmounts, TokenQuantity};
use copybot_storage::SqliteStore;
use std::path::Path;
use tempfile::tempdir;

#[path = "tests_parts/02_exact_and_gates.rs"]
mod exact_and_gates;
#[path = "tests_parts/03_outcome_ext.rs"]
mod outcome_ext;
#[path = "tests_parts/01_process.rs"]
mod process;

use outcome_ext::*;
use process::*;
