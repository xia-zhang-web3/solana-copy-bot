use super::*;
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::ShadowConfig;
use copybot_core_types::{
    ExactSwapAmounts, Lamports, SwapEvent, TokenQuantity, COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE,
};
use copybot_storage::SqliteStore;
use std::path::Path;
use tempfile::tempdir;

use crate::types::SOL_MINT;

#[path = "tests_parts/02_exact_and_gates.rs"]
mod exact_and_gates;
#[path = "tests_parts/03_outcome_ext.rs"]
mod outcome_ext;
#[path = "tests_parts/01_process.rs"]
mod process;

use outcome_ext::*;
use process::*;
