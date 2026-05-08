use super::*;
use chrono::Duration;
use copybot_core_types::SwapEvent;
use std::sync::atomic::AtomicBool;
use tempfile::tempdir;

#[path = "lib_tests/00_storage_tests.rs"]
mod base;
#[path = "lib_tests/05_storage_tests.rs"]
mod discovery_scoring_a;
#[path = "lib_tests/06_storage_tests.rs"]
mod discovery_scoring_b;
#[path = "lib_tests/08_storage_tests.rs"]
mod migration_guards;
#[path = "lib_tests/07_storage_tests.rs"]
mod observed_buy_mints;
#[path = "lib_tests/09_storage_tests.rs"]
mod observed_sol_legs;
#[path = "lib_tests/11_storage_tests.rs"]
mod retention_and_heartbeat;
#[path = "lib_tests/01_storage_tests.rs"]
mod shadow_exact;
#[path = "lib_tests/02_storage_tests.rs"]
mod shadow_risk;
#[path = "lib_tests/14_storage_tests.rs"]
mod snapshot_policy;
#[path = "lib_tests/10_storage_tests.rs"]
mod snapshots_and_observed;
#[path = "lib_tests/13_storage_tests.rs"]
mod startup_migrations;
#[path = "lib_tests/12_storage_tests.rs"]
mod startup_policy;
#[path = "lib_tests/03_storage_tests.rs"]
mod trusted_selection_a;
#[path = "lib_tests/04_storage_tests.rs"]
mod trusted_selection_b;

#[path = "lib_tests/18_storage_tests.rs"]
mod base_shadow_notional;
#[path = "lib_tests/23_storage_tests.rs"]
mod discovery_scoring_tail;
#[path = "lib_tests/20_storage_tests.rs"]
mod history_retention_events;
#[path = "lib_tests/17_storage_tests.rs"]
mod observed_buy_mints_tail;
#[path = "lib_tests/19_storage_tests.rs"]
mod observed_cursor_pages;
#[path = "lib_tests/15_storage_tests.rs"]
mod retention_heartbeat_tail;
#[path = "lib_tests/22_storage_tests.rs"]
mod shadow_risk_metrics_tail;
#[path = "lib_tests/16_storage_tests.rs"]
mod trusted_selection_activity;
#[path = "lib_tests/21_storage_tests.rs"]
mod trusted_selection_tail;

use base::*;
