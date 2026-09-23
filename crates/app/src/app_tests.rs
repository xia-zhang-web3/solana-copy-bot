mod b61_buy_fixture;
mod b61_fixture;
mod b61_receipt_fixture;
mod b61_rpc_fixture;
mod b61_tests;
mod b96_consumer_tests;
mod capture_scope_baseline_tests;
mod capture_scope_pipeline_tests;
mod root_hot_arrival_fairness;
mod source_sell_handoff_fixture;
mod source_sell_handoff_recovery_tests;
mod source_sell_handoff_runtime_tests;
use super::*;
mod source_sell_ack_state_tests;
#[path = "app_tests/source_sell_buy_fixture.rs"]
mod source_sell_buy_fixture;
#[path = "app_tests/source_sell_delivery_tests.rs"]
mod source_sell_delivery_tests;
#[path = "app_tests/source_sell_event_capture.rs"]
mod source_sell_event_capture;
mod source_sell_eviction_fixture;
mod source_sell_eviction_review_tests;
#[path = "app_tests/source_sell_ingress_fixture.rs"]
mod source_sell_ingress_fixture;
#[path = "app_tests/source_sell_ingress_tests.rs"]
mod source_sell_ingress_tests;
mod source_sell_producer_failure_tests;
mod source_sell_producer_fixture;
mod source_sell_producer_queue_tests;
mod source_sell_producer_runtime_tests;
mod source_sell_recent_eviction_review_tests;
mod source_sell_retention_maintenance_tests;
mod source_sell_retention_tests;
mod source_sell_review_tests;
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

mod association_consumer_tests;
#[path = "app_tests/native_buy_reviewer_consumer_candidate_tests.rs"]
mod native_buy_reviewer_consumer_candidate_tests;
#[path = "app_tests/native_buy_reviewer_quote_candidate_tests.rs"]
mod native_buy_reviewer_quote_candidate_tests;
mod association_fixture;
mod association_frozen_tests;
mod association_transport_tests;
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
#[path = "app_tests/93.rs"]
mod entry_quote_shadow_diagnostic_contract;
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
#[path = "app_tests/89.rs"]
mod execution_pump_fun_direct_builder_contract;
#[path = "app_tests/76.rs"]
mod execution_pump_fun_owned_sell_contract;
#[path = "app_tests/75.rs"]
mod execution_pump_fun_swap_transaction_contract;
#[path = "app_tests/91.rs"]
mod execution_pumpswap_direct_live_probe;
#[path = "app_tests/92.rs"]
mod execution_pumpswap_entry_route_contract;
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
#[path = "app_tests/81.rs"]
mod execution_swap_builder_alternate_fallback_contract;
#[path = "app_tests/80.rs"]
mod execution_swap_instructions_soft_failure_contract;
#[path = "app_tests/82.rs"]
mod execution_tiny_buy_simulation_retry_contract;
#[path = "app_tests/90.rs"]
mod execution_tiny_candidate_cleanup_contract;
#[path = "app_tests/87.rs"]
mod execution_tiny_provider_selection_contract;
#[path = "app_tests/88.rs"]
mod execution_tiny_stale_quote_materialize_contract;
#[path = "app_tests/85.rs"]
mod execution_tiny_terminal_dust_no_route;
#[path = "app_tests/84.rs"]
mod execution_tiny_terminal_stale_signal_guard;
#[path = "app_tests/86.rs"]
mod execution_tiny_terminal_token_not_tradable;
#[path = "app_tests/83.rs"]
mod execution_tiny_tick_ordering_contract;
#[path = "app_tests/94.rs"]
mod market_exit_shadow_quote_contract;
#[path = "app_tests/42.rs"]
mod priority_fee_canary_transient;
#[path = "app_tests/receipt_reconciliation_fixture.rs"]
mod receipt_reconciliation_fixture;
#[path = "app_tests/receipt_reconciliation_tests.rs"]
mod receipt_reconciliation_tests;
#[path = "app_tests/36_restart_recovery.rs"]
mod restart_recovery;
#[path = "app_tests/68.rs"]
mod shadow_risk_rug_rate_sample_floor;
#[path = "app_tests/79.rs"]
mod tiny_submit_confirmed_fill_backfill;

#[path = "app_tests/receipt_reconciliation_risk_tests.rs"]
mod receipt_reconciliation_risk_tests;

#[path = "app_tests/receipt_legacy_fixture.rs"]
mod receipt_legacy_fixture;

#[path = "app_tests/receipt_lifecycle_fixture.rs"]
mod receipt_lifecycle_fixture;
#[path = "app_tests/receipt_lifecycle_tests.rs"]
mod receipt_lifecycle_tests;
#[path = "app_tests/receipt_orphan_recovery_tests.rs"]
mod receipt_orphan_recovery_tests;

#[path = "app_tests/priority_fee_boundary_tests.rs"]
mod priority_fee_boundary_tests;

#[path = "app_tests/priority_fee_fixture.rs"]
mod priority_fee_fixture;
#[path = "app_tests/priority_fee_route_fixture.rs"]
mod priority_fee_route_fixture;
#[path = "app_tests/priority_fee_route_tests.rs"]
mod priority_fee_route_tests;

mod open_risk_sell_contract_tests;
#[path = "app_tests/open_risk_sell_fixture.rs"]
mod open_risk_sell_fixture;
#[path = "app_tests/open_risk_sell_pipeline_tests.rs"]
mod open_risk_sell_pipeline_tests;
mod open_risk_sell_rpc_fixture;
mod open_risk_sell_task_fixture;

#[path = "app_tests/failed_expense_runtime_tests.rs"]
mod failed_expense_runtime_tests;
#[path = "app_tests/owned_sell_intake_fixture.rs"]
mod owned_sell_intake_fixture;
#[path = "app_tests/owned_sell_intake_guard_tests.rs"]
mod owned_sell_intake_guard_tests;
#[path = "app_tests/owned_sell_intake_tests.rs"]
mod owned_sell_intake_tests;
#[path = "app_tests/owned_sell_queue_fixture.rs"]
mod owned_sell_queue_fixture;
#[path = "app_tests/owned_sell_queue_tests.rs"]
mod owned_sell_queue_tests;
#[path = "app_tests/owned_sell_tick_event_tests.rs"]
mod owned_sell_tick_event_tests;
#[path = "app_tests/receipt_accounting_isolation_tests.rs"]
mod receipt_accounting_isolation_tests;
#[path = "app_tests/receipt_cash_facts_atomicity_tests.rs"]
mod receipt_cash_facts_atomicity_tests;
#[path = "app_tests/receipt_cash_facts_coverage_tests.rs"]
mod receipt_cash_facts_coverage_tests;
#[path = "app_tests/receipt_cash_facts_fixture.rs"]
mod receipt_cash_facts_fixture;
#[path = "app_tests/receipt_cash_facts_tests.rs"]
mod receipt_cash_facts_tests;
#[path = "app_tests/receipt_signed_settlement_tests.rs"]
mod receipt_signed_settlement_tests;

#[path = "app_tests/failed_expense_coverage_tests.rs"]
mod failed_expense_coverage_tests;

#[path = "app_tests/failed_expense_queue_tests.rs"]
mod failed_expense_queue_tests;

#[path = "app_tests/failed_expense_arrivals_tests.rs"]
mod failed_expense_arrivals_tests;

#[path = "app_tests/failed_expense_reservation_tests.rs"]
mod failed_expense_reservation_tests;

#[path = "app_tests/receipt_native_observations_tests.rs"]
mod receipt_native_observations_tests;

#[path = "app_tests/receipt_native_lifecycle_tests.rs"]
mod receipt_native_lifecycle_tests;

#[path = "app_tests/receipt_native_recovery_tests.rs"]
mod receipt_native_recovery_tests;

#[path = "app_tests/receipt_native_coverage_tests.rs"]
mod receipt_native_coverage_tests;

#[path = "app_tests/receipt_system_funding_review_tests.rs"]
mod receipt_system_funding_review_tests;

#[path = "app_tests/receipt_system_funding_tests.rs"]
mod receipt_system_funding_tests;

#[path = "app_tests/receipt_lifecycle_payment_review_tests.rs"]
mod receipt_lifecycle_payment_review_tests;

#[path = "app_tests/receipt_lifecycle_selection_tests.rs"]
mod receipt_lifecycle_selection_tests;

mod fresh_buy_size_counterexample_tests;
mod fresh_buy_size_domain_tests;
mod fresh_buy_size_fixture;
mod fresh_buy_size_provider_tests;
mod fresh_buy_size_runtime_fixture;
mod fresh_buy_size_runtime_tests;

mod buy_retry_queue_fixture;
mod buy_retry_queue_http_fixture;
mod buy_retry_queue_tests;
mod buy_retry_safety_fixture;
mod buy_retry_safety_state_tests;
mod buy_retry_safety_tests;

mod buy_retry_dynamic_cap_tests;
mod buy_retry_persistent_receipt_tests;
mod buy_retry_recovery_progress_tests;

mod entry_cost_cash_rpc_fixture;
mod entry_cost_cash_tests;
mod entry_cost_guard_tests;
mod entry_cost_policy_tests;
mod entry_cost_queue_tests;
mod entry_cost_runtime_fixture;

mod entry_risk_boundary_tests;
pub(crate) mod entry_risk_clock_fixture;
mod entry_risk_retry_tests;
mod entry_risk_same_tick_tests;
mod entry_risk_transition_tests;

mod entry_cash_guard_tests;
mod entry_cash_queue_tests;
mod entry_cash_summary_tests;

mod rpc_simulation_contract_tests;
mod rpc_simulation_http_fixture;
mod rpc_simulation_queue_tests;
mod rpc_simulation_route_tests;
mod rpc_simulation_runtime_tests;

mod native_funding_amount_tests;
mod native_funding_binding_tests;
mod native_funding_builder_tests;
mod native_funding_fixture;
mod native_funding_runtime_builder_tests;
mod native_funding_state_tests;
mod native_funding_validation_tests;
mod native_rpc_binding_tests;
mod native_rpc_fixture;
mod native_rpc_io_tests;
mod native_rpc_protocol_tests;
mod native_rpc_reuse_fixture;
mod native_rpc_runtime_builder_tests;
mod native_rpc_transport_tests;
mod native_setup_ata_tests;
mod native_setup_binding_tests;
mod native_setup_builder_tests;
mod native_setup_fixture;
mod native_setup_provenance_tests;
mod native_setup_rpc_fixture;
mod native_setup_rpc_tests;
mod native_setup_wsol_tests;
mod wire_extraction_oracle_tests;

mod final_floor_binding_tests;
mod final_floor_fixture;
mod final_floor_layout_tests;
mod native_ata_amount_tests;
mod native_ata_fixture;
mod native_ata_order_tests;
mod native_ata_provenance_tests;
mod native_ata_rpc_tests;
mod native_ata_transport_tests;
mod native_floor_api_tests;
mod native_floor_rejection_tests;
mod receipt_rpc_error_tests;
mod receipt_rpc_fixture;
mod receipt_rpc_request;
mod receipt_rpc_server;
mod receipt_rpc_task_fixture;
mod receipt_rpc_timeout_tests;

mod native_floor_conversion_cases;
mod native_floor_policy_tests;
mod native_floor_queue_tests;
mod native_floor_runtime_tests;
mod native_floor_signing_mutation_tests;
mod native_floor_signing_tests;
mod native_floor_submit_tests;

mod initial_sol_amount_tests;
mod initial_sol_future_size_tests;
mod initial_sol_queue_tests;
mod initial_sol_rpc_fixture;
mod initial_sol_runtime_tests;
mod initial_sol_state_tests;
mod initial_sol_submit_tests;
mod initial_sol_transport_tests;
mod native_floor_review_retry_tests;
mod native_floor_review_tests;

#[path = "app_tests/source_write_off_fixture.rs"]
mod source_write_off_fixture;
#[path = "app_tests/source_write_off_http.rs"]
mod source_write_off_http;
#[path = "app_tests/source_write_off_runtime_tests.rs"]
mod source_write_off_runtime_tests;
mod submit_refusal_fixture;
mod submit_refusal_original_probe_tests;
mod submit_refusal_queue_tests;
mod submit_refusal_runtime_tests;

#[path = "app_tests/source_sell_sweep_fixture.rs"]
mod source_sell_sweep_fixture;
#[path = "app_tests/source_sell_sweep_traversal_tests.rs"]
mod source_sell_sweep_traversal_tests;
#[path = "app_tests/source_write_off_sweep_review_tests.rs"]
mod source_write_off_sweep_review_tests;

#[path = "app_tests/b53_dispatch.rs"]
mod b53_dispatch;
#[path = "app_tests/b53_fixture.rs"]
mod b53_fixture;
#[path = "app_tests/b53_http.rs"]
mod b53_http;
#[path = "app_tests/b53_pending.rs"]
mod b53_pending;
#[path = "app_tests/b53_queue.rs"]
mod b53_queue;
#[path = "app_tests/b53_storage_trace.rs"]
mod b53_storage_trace;
#[path = "app_tests/b53_terminal.rs"]
mod b53_terminal;
#[path = "app_tests/source_guard_await_tests.rs"]
mod source_guard_await_tests;
#[path = "app_tests/source_guard_baseline_tests.rs"]
mod source_guard_baseline_tests;
#[path = "app_tests/source_guard_continuation_tests.rs"]
mod source_guard_continuation_tests;
#[path = "app_tests/source_guard_fixture.rs"]
mod source_guard_fixture;
#[path = "app_tests/source_guard_queue_tests.rs"]
mod source_guard_queue_tests;
#[path = "app_tests/source_guard_rpc_fixture.rs"]
mod source_guard_rpc_fixture;
#[path = "app_tests/source_guard_signing_tests.rs"]
mod source_guard_signing_tests;

#[path = "app_tests/b53_boundary.rs"]
mod b53_boundary;

#[path = "app_tests/b53_buy_gates.rs"]
mod b53_buy_gates;

#[path = "app_tests/b53_response_cases.rs"]
mod b53_response_cases;

#[path = "app_tests/b53_root_event_review_tests.rs"]
mod b53_root_event_review_tests;

#[path = "app_tests/b53_blocker_event_tests.rs"]
mod b53_blocker_event_tests;

#[path = "app_tests/b58_fixture.rs"]
mod b58_fixture;
#[path = "app_tests/b58_http.rs"]
mod b58_http;
#[path = "app_tests/b58_probes.rs"]
mod b58_probes;

#[path = "app_tests/b60_http_tests.rs"]
mod b60_http_tests;

#[path = "app_tests/b60_bundle_tests.rs"]
mod b60_bundle_tests;

#[path = "app_tests/b60_replacement_tests.rs"]
mod b60_replacement_tests;

#[path = "app_tests/b64_boundary_tests.rs"]
mod b64_boundary_tests;
#[path = "app_tests/b64_case_fixture.rs"]
mod b64_case_fixture;
#[path = "app_tests/b64_http_fixture.rs"]
pub(crate) mod b64_http_fixture;
#[path = "app_tests/b64_overlap_tests.rs"]
mod b64_overlap_tests;
#[path = "app_tests/b64_provider_tests.rs"]
mod b64_provider_tests;
#[path = "app_tests/b64_source_tests.rs"]
mod b64_source_tests;

#[path = "app_tests/b70_fixture.rs"]
mod b70_fixture;
#[path = "app_tests/b70_hooks.rs"]
pub(crate) mod b70_hooks;
#[path = "app_tests/b70_tests.rs"]
mod b70_tests;

#[path = "app_tests/b70_job_fixture.rs"]
pub(crate) mod b70_job_fixture;

#[path = "app_tests/b70_completion_tests.rs"]
pub(crate) mod b70_completion_tests;

#[path = "app_tests/b70_capacity_tests.rs"]
pub(crate) mod b70_capacity_tests;

#[path = "app_tests/b70_priority_tests.rs"]
mod b70_priority_tests;

#[path = "app_tests/b70_event_capture.rs"]
mod b70_event_capture;

#[path = "app_tests/b70_shutdown_tests.rs"]
mod b70_shutdown_tests;

#[path = "app_tests/b70_root_loop_tests.rs"]
mod b70_root_loop_tests;

#[path = "app_tests/b70_root_scheduler_tests.rs"]
mod b70_root_scheduler_tests;

#[path = "app_tests/b70_r1_loop_tests.rs"]
mod b70_r1_loop_tests;
#[path = "app_tests/b70_r1_owner_tests.rs"]
mod b70_r1_owner_tests;

#[path = "app_tests/b70_r2_fixture.rs"]
mod b70_r2_fixture;
#[path = "app_tests/b70_r2_owner_tests.rs"]
mod b70_r2_owner_tests;

#[path = "app_tests/b70_r2_provenance_tests.rs"]
mod b70_r2_provenance_tests;

#[path = "app_tests/b70_r3_fixture.rs"]
mod b70_r3_fixture;
#[path = "app_tests/b70_r3_tests.rs"]
mod b70_r3_tests;

#[path = "app_tests/association_loop_tests.rs"]
mod association_loop_tests;

mod association_sell_fixture;
mod association_sell_tests;

mod association_parent_causal_tests;
mod association_parent_fixture;
mod association_parent_tests;
mod association_parent_upgrade_tests;

mod b93_attempt_fixture;
mod b93_boundary_tests;
mod b93_buy_fixture;
mod b93_continuation_tests;
mod b93_fixture;
mod b93_fresh_tests;
mod b93_http_fixture;
mod b93_negative_tests;
mod b93_prior_submitted_tests;
mod b93_quote_refresh_tests;
mod b93_reconcile_tests;
mod b93_retry_tests;
mod b93_submitted_fixture;

mod b93_r1_progress_tests;
mod b93_r1_retry_fixture;
mod b93_r1_transition_tests;

mod b97_consumer_tests;
mod b97_continuation_tests;
mod b97_fault_tests;
mod b97_fixture;
mod b97_sqlite_hooks;

mod association_observation_fixture;

mod strict_quote_fixture;
mod strict_quote_late_tests;
mod strict_quote_loop_tests;
mod strict_quote_runtime_tests;

mod strict_quote_completion_wait_tests;

mod b102_wake_hooks;
mod b102_wake_tests;

#[path = "app_tests/b105_availability_tests.rs"]
mod b105_availability_tests;

#[path = "app_tests/b105_boundary_tests.rs"]
mod b105_boundary_tests;

mod current_owned_sell_queue_tests;
mod owned_sell_boundary_tests;
mod owned_sell_fixture;

mod generic_buy_binding_tests;
mod generic_buy_boundary_tests;
mod generic_buy_decode;
mod generic_buy_fixture;
mod generic_buy_loopback;
mod generic_buy_oracle_tests;
mod generic_buy_rejection_tests;
mod generic_buy_test_support;

mod generic_sell_decode;
mod generic_sell_fixture;
mod generic_sell_loopback;
mod generic_sell_oracle_tests;

mod generic_sell_binding_tests;
mod generic_sell_boundary_tests;
mod generic_sell_presign_tests;
mod generic_sell_rejection_tests;
mod generic_sell_synthetic_fixture;
mod generic_sell_test_support;

mod b123_causal_fixture;
mod b123_causal_protocol_tests;
mod b123_jupiter_fixture_tests;
mod b123_jupiter_gate_tests;
mod b123_jupiter_state_tests;
mod token2022_ata_boundary_tests;
mod token2022_ata_collector_tests;
mod token2022_ata_inputs_tests;
mod token2022_ata_layout_tests;
mod token2022_ata_order_tests;
mod token2022_ata_rpc_tests;
mod token2022_ata_submit_tests;
mod token2022_ata_transport_tests;

mod b126_causal_tests;
mod b126_config_fixture;

mod b126_runtime_fixture;
mod b126_runtime_tests;

mod b126_r1_fixture;
mod b126_r1_tests;

mod b127_baseline_tests;
mod b127_causal_fixture;
mod b127_floor_tests;
mod b127_lifecycle_tests;
mod b127_runtime_tests;

mod entry_risk_future_size_tests;

mod b93_local_fixture;
mod temporary_output_fixture;

mod tiny_transport_fixture;

mod queue_owned_buy_fixture;

mod b131_hold_tests;

mod b135_budget_tests;
mod b135_fixture;
pub(crate) mod b135_hooks;
mod b135_partial_tests;
mod b135_refusal_tests;
mod b135_server;
mod b135_state_tests;
mod b135_transition_tests;
mod fractional;

#[path = "app_tests/tiny_parent_fixture.rs"]
mod tiny_parent_fixture;

#[path = "app_tests/tiny_buy_route_fixture.rs"]
mod tiny_buy_route_fixture;

#[path = "app_tests/tiny_submit_fixture.rs"]
mod tiny_submit_fixture;

mod b136_baseline_tests;
mod b136_fixture;
mod b136_server;

mod b136_budget_tests;
mod b136_config;
mod b136_endpoint_tests;
mod b136_prior_sell;
mod b136_recovery_tests;
mod b136_refusal_tests;
mod b136_rpc;

pub(crate) mod b136_r1_hooks;
mod b136_r1_lifecycle_tests;
mod b136_r1_tick_tests;
