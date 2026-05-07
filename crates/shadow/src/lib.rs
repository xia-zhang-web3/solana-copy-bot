use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_config::ShadowConfig;
use copybot_core_types::{
    Lamports, SwapEvent, TokenQuantity, COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE,
    COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS,
};
use copybot_storage::{CopySignalRow, SqliteStore};
use std::collections::{HashMap, HashSet};
use tracing::info;

mod candidate;
use self::candidate::to_shadow_candidate;
mod quality_gates;
mod signals;
mod snapshots;
use self::signals::log_gate_drop;

include!("lib_parts/01_types.rs");
include!("lib_parts/02_service.rs");

#[cfg(test)]
mod tests;
