use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::{
    load_from_env_or_default, ExecutionConfig, IngestionConfig, RiskConfig, ShadowConfig,
};
#[cfg(test)]
use copybot_core_types::TokenQuantity;
use copybot_core_types::{Lamports, SignedLamports, SwapEvent};
use copybot_ingestion::{IngestionRuntimeSnapshot, IngestionService};
use copybot_shadow::{FollowSnapshot, ShadowService};
use copybot_storage_core::{
    is_fatal_sqlite_anyhow_error, run_observed_startup_step, sqlite_contention_snapshot,
    SqliteContentionSnapshot, SqliteStore, StartupStepOutcome,
};
#[cfg(test)]
use copybot_storage_core::{
    StartupStepProgressReporter, StartupStepRuntimePolicy, StartupStepTimeoutBehavior,
};
#[cfg(test)]
use serde::Deserialize;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::env;
#[cfg(test)]
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
#[cfg(test)]
use std::time::Duration as StdDuration;
use std::time::Instant as StdInstant;
use tokio::task::JoinHandle;
use tokio::time::{self, Duration, MissedTickBehavior};
use tracing::{debug, info, warn};

mod alerts;
mod app_loop_ingestion;
mod app_loop_irrelevant_backlog;
mod app_loop_irrelevant_swap;
mod app_loop_maintenance;
mod app_loop_relevant_swap;
mod app_loop_shadow;
mod app_loop_shutdown;
mod config_contract;
mod discovery_runtime;
mod entry_quote_shadow_diagnostic;
mod execution_build_plan_age;
mod execution_build_plan_metadata;
mod execution_build_plan_refresh;
mod execution_canary;
mod execution_canary_entry_gate;
mod execution_canary_route;
mod execution_canary_safety;
mod execution_canary_signing_contract;
#[allow(dead_code)]
mod execution_canary_state_machine;
mod execution_canary_submit_contract;
mod execution_canary_summary;
mod execution_pump_fun_migration_fallback;
mod execution_pump_fun_quote_http;
mod execution_pump_fun_swap_instructions_http;
mod execution_pump_fun_swap_transaction_http;
mod execution_pumpswap_accounts;
mod execution_pumpswap_direct_builder;
mod execution_pumpswap_direct_instructions;
mod execution_pumpswap_error;
mod execution_pumpswap_route_context;
mod execution_quote_canary;
mod execution_quote_canary_helpers;
mod execution_quote_canary_priority_fee;
mod execution_quote_canary_rpc;
mod execution_quote_http;
mod execution_quote_provider_selection;
mod execution_route_plan;
mod execution_serialized_transaction_slot;
mod execution_signing_envelope;
mod execution_simulation_proof;
mod execution_solana_tx;
#[allow(dead_code)]
mod execution_submit_adapter;
mod execution_swap_blueprint;
mod execution_swap_http_request;
mod execution_swap_http_retry;
mod execution_swap_instructions_http;
mod execution_swap_transaction_http;
mod execution_tiny_entry_route;
mod execution_transaction_rpc_simulation;
mod exit_policy_shadow_quote;
mod history_retention;
mod irrelevant_backpressure;
mod irrelevant_persistence;
mod json_sanitize;
mod market_exit_shadow_quote;
mod observed_swap_writer;
mod quote_price_sanity;
mod runtime_follow_surface;
mod shadow_restart_recovery;
mod shadow_risk;
mod shadow_runtime_helpers;
mod shadow_scheduler;
mod shadow_state_events;
mod stale_close;
mod stale_close_quote;
mod startup;
mod swap_classification;
mod task_spawns;
mod telemetry;
mod zero_universe;

use crate::alerts::AlertDispatcher;
use crate::app_loop_ingestion::handle_ingestion_swap_poll;
use crate::app_loop_irrelevant_backlog::retry_pending_irrelevant_swaps;
use crate::app_loop_maintenance::{
    handle_app_heartbeat_tick, handle_observed_swap_retention_join, handle_risk_refresh_tick,
};
use crate::app_loop_shadow::{
    handle_shadow_interval_tick, handle_shadow_snapshot_join, handle_shadow_worker_join,
    prepare_shadow_scheduler_before_select,
};
use crate::app_loop_shutdown::shutdown_app_loop_tasks;
use crate::config_contract::{
    validate_execution_runtime_contract, validate_live_ingestion_source_contract,
};
use crate::discovery_runtime::{DiscoveryService, RuntimePublicationTruthResolution};
use crate::entry_quote_shadow_diagnostic::EntryQuoteShadowDiagnostic;
use crate::execution_canary::ExecutionCanaryRunner;
use crate::exit_policy_shadow_quote::ExitPolicyShadowQuoteDiagnostic;
use crate::history_retention::HistoryRetentionRunner;
use crate::irrelevant_backpressure::*;
use crate::irrelevant_persistence::*;
use crate::json_sanitize::sanitize_json_value;
use crate::market_exit_shadow_quote::MarketExitShadowQuoteDiagnostic;
#[cfg(test)]
use crate::observed_swap_writer::OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY;
use crate::observed_swap_writer::{
    run_observed_swap_retention_maintenance_once, ObservedSwapRecentRawJournalConfig,
    ObservedSwapRetentionConfig, ObservedSwapRetentionMaintenanceSummary,
    ObservedSwapRetentionRuntimeHealthHandle, ObservedSwapWriter, ObservedSwapWriterSnapshot,
    OBSERVED_SWAP_RECENT_RAW_JOURNAL_OVERFLOW_CAPACITY_MULTIPLIER,
    OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_COALESCE_MAX_BATCHES,
    OBSERVED_SWAP_RETENTION_PARTIAL_RETRY_INTERVAL, OBSERVED_SWAP_RETENTION_STARTUP_GRACE_INTERVAL,
    OBSERVED_SWAP_RETENTION_SWEEP_INTERVAL, OBSERVED_SWAP_WRITER_CHANNEL_CLOSED_CONTEXT,
    OBSERVED_SWAP_WRITER_REPLY_CLOSED_CONTEXT, OBSERVED_SWAP_WRITER_TERMINAL_FAILURE_CONTEXT,
};
use crate::shadow_restart_recovery::recover_shadow_restart_gap;
use crate::shadow_runtime_helpers::{handle_shadow_task_output, spawn_shadow_worker_task};
use crate::shadow_scheduler::{
    ShadowScheduler, ShadowSwapSide, ShadowTaskInput, ShadowTaskKey, ShadowTaskOutput,
};
use crate::shadow_state_events::*;
use crate::stale_close::close_stale_shadow_lots;
use crate::stale_close_quote::StaleCloseQuotePricer;
#[cfg(test)]
use crate::startup::STARTUP_LARGE_WAL_CHECKPOINT_TIMEOUT;
use crate::startup::{
    build_startup_progress_reporter, defer_implicit_startup_sqlite_wal_autocheckpoint,
    emit_inline_startup_progress, perform_startup_wal_checkpoint,
    restore_implicit_startup_sqlite_wal_autocheckpoint, run_inline_startup_step,
    skip_inline_startup_step, sqlite_startup_policy, startup_step_policy,
    StartupWalCheckpointOutcome, STARTUP_SQLITE_AUX_STEP_TIMEOUT,
    STARTUP_WAL_CHECKPOINT_DEFER_REASON,
};
use crate::swap_classification::{classify_swap_side, shadow_task_key_for_swap};
use crate::task_spawns::spawn_shadow_snapshot_task;
use crate::telemetry::format_error_chain;
use crate::zero_universe::*;

mod app_consumer_telemetry;
mod app_loop;
mod app_main;
mod cli_and_ingestion_override;
mod constants;
mod lamports;
mod operator_emergency;
mod quality_contract;
mod risk_contract;
mod risk_types;
mod runtime_bootstrap;
mod runtime_helpers;
mod sqlite_maintenance;
use crate::app_consumer_telemetry::AppConsumerLoopTelemetry;
#[cfg(test)]
use crate::app_consumer_telemetry::AppConsumerLoopTelemetrySnapshot;
use crate::app_loop::run_app_loop;
#[cfg(test)]
use crate::cli_and_ingestion_override::parse_ingestion_source_override;
use crate::cli_and_ingestion_override::{
    apply_ingestion_source_override, load_ingestion_source_override, parse_config_arg,
};
use crate::constants::*;
use crate::lamports::{
    lamports_to_sol, signed_lamports_to_sol, sol_to_lamports_floor,
    sol_to_signed_lamports_conservative,
};
#[cfg(test)]
use crate::operator_emergency::parse_operator_emergency_stop_reason;
use crate::operator_emergency::OperatorEmergencyStop;
use crate::quality_contract::{
    enforce_quality_gate_http_url, select_role_helius_http_url,
    validate_shadow_quality_gate_contract,
};
use crate::risk_contract::validate_execution_risk_contract;
use crate::risk_types::{
    BuyRiskBlockReason, BuyRiskDecision, InfraBlockKey, InfraBlockSignal, ShadowRiskGuard,
};
#[cfg(test)]
use crate::runtime_bootstrap::parse_app_log_env_filter;
use crate::runtime_bootstrap::{
    init_tracing, resolve_migrations_dir, validate_live_execution_policy_contract,
};
#[cfg(test)]
use crate::runtime_follow_surface::follow_event_retention_duration;
use crate::runtime_follow_surface::{
    runtime_follow_reload_from_publication_truth, startup_follow_snapshot_from_publication_truth,
    startup_runtime_publication_truth,
};
use crate::runtime_helpers::*;
use crate::sqlite_maintenance::*;

#[cfg(test)]
pub(crate) mod app_tests;

#[tokio::main]
async fn main() -> Result<()> {
    app_main::run().await
}
