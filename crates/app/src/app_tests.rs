use super::*;
use crate::app_loop::runtime_follow_reload_interval_seconds;
use crate::app_loop_relevant_swap::handle_relevant_observed_swap;
use copybot_core_types::WalletMetricRow;
use copybot_storage_core::{
    DiscoveryPersistedRebuildPhase, DiscoveryPersistedRebuildStateRow,
    DiscoveryPublicationFreshnessGate, DiscoveryPublicationStateUpdate, DiscoveryRuntimeCursor,
    DiscoveryRuntimeMode, DiscoveryTrustedSelectionStateUpdate, TrustedSelectionState,
    TrustedSnapshotSourceKind,
};
use rusqlite::{params, Connection};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

include!("app_tests/00.rs");
include!("app_tests/01.rs");
include!("app_tests/02.rs");
include!("app_tests/03.rs");
include!("app_tests/04.rs");
include!("app_tests/05.rs");
include!("app_tests/07.rs");
include!("app_tests/08.rs");
include!("app_tests/09.rs");
include!("app_tests/10.rs");
include!("app_tests/11.rs");
include!("app_tests/12.rs");
include!("app_tests/13.rs");
include!("app_tests/14.rs");
include!("app_tests/15.rs");
include!("app_tests/16.rs");
include!("app_tests/17.rs");
include!("app_tests/18.rs");
include!("app_tests/19.rs");
include!("app_tests/20.rs");
include!("app_tests/21.rs");
include!("app_tests/22.rs");
include!("app_tests/23.rs");
include!("app_tests/24.rs");
include!("app_tests/25.rs");
include!("app_tests/26.rs");
include!("app_tests/27.rs");
include!("app_tests/28.rs");
include!("app_tests/29.rs");
include!("app_tests/30.rs");
include!("app_tests/31.rs");
include!("app_tests/32.rs");
include!("app_tests/33.rs");
include!("app_tests/34.rs");
include!("app_tests/35.rs");
#[path = "app_tests/43.rs"]
mod execution_quote_canary_hot_observed;
#[path = "app_tests/50.rs"]
mod execution_quote_canary_provider_fallback;
#[path = "app_tests/37.rs"]
mod execution_state_machine;
#[path = "app_tests/41.rs"]
mod execution_state_machine_build_metadata;
#[path = "app_tests/44.rs"]
mod execution_state_machine_entry_gate;
#[path = "app_tests/49.rs"]
mod execution_state_machine_provider_selector;
#[path = "app_tests/40.rs"]
mod execution_state_machine_safety;
#[path = "app_tests/38.rs"]
mod execution_state_machine_sell;
#[path = "app_tests/45.rs"]
mod execution_state_machine_swap_blueprint;
#[path = "app_tests/46.rs"]
mod execution_state_machine_swap_blueprint_runner;
#[path = "app_tests/47.rs"]
mod execution_state_machine_swap_instructions_http;
#[path = "app_tests/48.rs"]
mod execution_state_machine_swap_transaction_http;
#[path = "app_tests/39.rs"]
mod execution_state_machine_timeout;
#[path = "app_tests/42.rs"]
mod priority_fee_canary_transient;
#[path = "app_tests/36_restart_recovery.rs"]
mod restart_recovery;
