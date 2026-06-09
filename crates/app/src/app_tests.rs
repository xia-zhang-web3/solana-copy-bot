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
#[path = "app_tests/69.rs"]
mod execution_build_plan_refresh_contract;
#[path = "app_tests/70.rs"]
mod execution_candidate_sell_retry_contract;
#[path = "app_tests/71.rs"]
mod execution_confirmed_transaction_fill_contract;
#[path = "app_tests/73.rs"]
mod execution_orphan_position_recovery_contract;
#[path = "app_tests/72.rs"]
mod execution_orphan_sell_confirmation_contract;
#[path = "app_tests/74.rs"]
mod execution_partial_sell_accounting_contract;
#[path = "app_tests/76.rs"]
mod execution_pump_fun_owned_sell_contract;
#[path = "app_tests/75.rs"]
mod execution_pump_fun_swap_transaction_contract;
#[path = "app_tests/43.rs"]
mod execution_quote_canary_hot_observed;
#[path = "app_tests/50.rs"]
mod execution_quote_canary_provider_fallback;
#[path = "app_tests/77.rs"]
mod execution_sell_quote_failure_contract;
#[path = "app_tests/78.rs"]
mod execution_sell_token_in_flight_contract;
#[path = "app_tests/37.rs"]
mod execution_state_machine;
#[path = "app_tests/41.rs"]
mod execution_state_machine_build_metadata;
#[path = "app_tests/61.rs"]
mod execution_state_machine_confirmation_boundary_contract;
#[path = "app_tests/57.rs"]
mod execution_state_machine_confirmation_tracker_contract;
#[path = "app_tests/56.rs"]
mod execution_state_machine_confirmed_fill_accounting;
#[path = "app_tests/44.rs"]
mod execution_state_machine_entry_gate;
#[path = "app_tests/53.rs"]
mod execution_state_machine_file_signer_contract;
#[path = "app_tests/67.rs"]
mod execution_state_machine_owned_sell_quote;
#[path = "app_tests/49.rs"]
mod execution_state_machine_provider_selector;
#[path = "app_tests/58.rs"]
mod execution_state_machine_rpc_confirmation_contract;
#[path = "app_tests/59.rs"]
mod execution_state_machine_rpc_submit_contract;
#[path = "app_tests/40.rs"]
mod execution_state_machine_safety;
#[path = "app_tests/38.rs"]
mod execution_state_machine_sell;
#[path = "app_tests/52.rs"]
mod execution_state_machine_signer_contract;
#[path = "app_tests/51.rs"]
mod execution_state_machine_submit_contract;
#[path = "app_tests/54.rs"]
mod execution_state_machine_submit_transport_contract;
#[path = "app_tests/55.rs"]
mod execution_state_machine_submit_transport_outcome_contract;
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
#[path = "app_tests/62.rs"]
mod execution_state_machine_tiny_submit_confirm_path;
#[path = "app_tests/60.rs"]
mod execution_state_machine_tiny_submit_gate_contract;
#[path = "app_tests/63.rs"]
mod execution_state_machine_tiny_submit_route;
#[path = "app_tests/64.rs"]
mod execution_state_machine_tiny_submit_sell_route;
#[path = "app_tests/66.rs"]
mod execution_state_machine_tiny_submit_terminal_write_off;
#[path = "app_tests/65.rs"]
mod execution_state_machine_tiny_submit_timeout_route;
#[path = "app_tests/80.rs"]
mod execution_swap_instructions_soft_failure_contract;
#[path = "app_tests/42.rs"]
mod priority_fee_canary_transient;
#[path = "app_tests/36_restart_recovery.rs"]
mod restart_recovery;
#[path = "app_tests/68.rs"]
mod shadow_risk_rug_rate_sample_floor;
#[path = "app_tests/79.rs"]
mod tiny_submit_confirmed_fill_backfill;
